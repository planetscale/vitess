package schema

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	createTable           = `
		CREATE TABLE %s (
		id BIGINT(20) not NULL,
		msg varchar(64),
		PRIMARY KEY (id)
		) ENGINE=InnoDB;`
	alterTable = `
		ALTER TABLE %s
		ADD COLUMN new_id bigint(20) NOT NULL AUTO_INCREMENT FIRST,
		DROP PRIMARY KEY,
		ADD PRIMARY KEY (new_id),
		ADD INDEX idx_column(%s)`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = &cluster.LocalProcessCluster{Cell: cell, Hostname: hostname}
		schemaChangeDirectory = path.Join("/tmp", fmt.Sprintf("schema_change_dir_%d", clusterInstance.GetAndReserveTabletUID()))
		defer os.RemoveAll(schemaChangeDirectory)
		defer clusterInstance.Teardown()

		if _, err := os.Stat(schemaChangeDirectory); os.IsNotExist(err) {
			_ = os.Mkdir(schemaChangeDirectory, 0700)
		}

		clusterInstance.VtctldExtraArgs = []string{
			"-schema_change_dir", schemaChangeDirectory,
			"-schema_change_controller", "local",
			"-schema_change_check_interval", "1"}

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}

		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 2, true); err != nil {
			return 1, err
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"1"}, 1, false); err != nil {
			return 1, err
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"2"}, 1, false); err != nil {
			return 1, err
		}
		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}

func TestSchemaChange(t *testing.T) {
	// Create 4 tables
	totalTables := 4
	var sqlQuery = ""
	for i := 0; i < totalTables; i++ {
		sqlQuery = fmt.Sprintf(createTable, fmt.Sprintf("vt_select_test_%02d", i))
		err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sqlQuery)
		assert.Nil(t, err)

	}

	// Check if 4 tables are created
	checkTables(t, 4)
	checkTables(t, 4)

	// Also match the vschema for those tablets
	matchSchema(t)

	// Alter schema and then match the schema
	sqlQuery = fmt.Sprintf(alterTable, fmt.Sprintf("vt_select_test_%02d", 3), "msg")
	err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sqlQuery)
	assert.Nil(t, err)

	matchSchema(t)

	// If we put file into schema change dir, then it should be applied to all shards
	_ = os.Mkdir(path.Join(schemaChangeDirectory, keyspaceName), 0700)
	_ = os.Mkdir(path.Join(schemaChangeDirectory, keyspaceName, "input"), 0700)
	sqlFile := path.Join(schemaChangeDirectory, keyspaceName, "input/create_test_table_x.sql")
	err = ioutil.WriteFile(sqlFile, []byte("create table test_table_x (id int)"), 0644)
	assert.Nil(t, err)
	timeout := time.Now().Add(10 * time.Second)
	matchFoundAfterAutoSchemaApply := false
	for time.Now().Before(timeout) {
		if _, err := os.Stat(sqlFile); os.IsNotExist(err) {
			matchFoundAfterAutoSchemaApply = true
			checkTables(t, 5)
			matchSchema(t)
		}
	}
	if !matchFoundAfterAutoSchemaApply {
		assert.Fail(t, "Auto schema is not consumed")
	}
	defer os.RemoveAll(path.Join(schemaChangeDirectory, keyspaceName))
}

func checkTables(t *testing.T, count int) {
	// Check if 4 tables are created
	checkTablesForCount(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0], count)
	checkTablesForCount(t, clusterInstance.Keyspaces[0].Shards[1].Vttablets[0], count)
}

func checkTablesForCount(t *testing.T, tablet cluster.Vttablet, count int) {
	queryResult, err := tablet.VttabletProcess.QueryTablet("show tables;", keyspaceName, true)
	assert.Nil(t, err)
	assert.Equal(t, len(queryResult.Rows), count)
}

func matchSchema(t *testing.T) {
	firstShardSchema, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetSchema", clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.TabletPath)
	assert.Nil(t, err)

	secondShardSchema, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetSchema", clusterInstance.Keyspaces[0].Shards[1].Vttablets[0].VttabletProcess.TabletPath)
	assert.Nil(t, err)

	assert.Equal(t, firstShardSchema, secondShardSchema)
}
