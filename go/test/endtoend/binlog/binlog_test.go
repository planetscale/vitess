/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binlog

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	localCluster *cluster.LocalProcessCluster
	cell         = "zone1"
	hostname     = "localhost"
	keyspaceName = "ks"
	sqlSchema    = `
					create table test_table(
					id bigint(20) unsigned auto_increment,
					msg varchar(64),
					primary key (id),
					index by_msg (msg)
					) Engine=InnoDB
`
	commonTabletArg = []string{
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-enable_replication_reporter",
		"-serving_state_grace_period", "1s",
		"-binlog_player_protocol", "grpc",
	}
	vSchema = `
		{
		  "sharded": false,
		  "vindexes": {
			"hash_index": {
			  "type": "hash"
			}
		  },
		  "tables": {
			"test_table": {
			   "column_vindexes": [
				{
				  "column": "id",
				  "name": "hash_index"
				}
			  ] 
			}
		  }
		}
`
	srcMaster  *cluster.Vttablet
	srcReplica *cluster.Vttablet
	srcRdonly  *cluster.Vttablet

	destMaster  *cluster.Vttablet
	destReplica *cluster.Vttablet
	destRdonly  *cluster.Vttablet
)

func TestCharset(t *testing.T) {
	position, _ := cluster.GetMasterPosition(t, *destReplica, hostname)

	insertSQL := `insert into test_table(id,msg) values(1, 'Šṛ́rỏé')`
	_, err := queryTablet(t, *srcMaster, insertSQL, "latin1")
	assert.Nil(t, err)

	waitForReplicaEvent(t, position, insertSQL, *destReplica)
	data, err := queryTablet(t, *destMaster, "select id, msg from test_table where id = 1", "")
	assert.Nil(t, err)
	assert.NotNil(t, data.Rows)
	assert.Equal(t, len(data.Rows), 1)
}

func waitForReplicaEvent(t *testing.T, position string, sql string, vttablet cluster.Vttablet) {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		println("fetching with position " + position)
		output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletUpdateStream", "-position", position, "-count", "1", vttablet.Alias)
		assert.Nil(t, err)
		var binlogTxn binlogdata.BinlogTransaction

		err = json.Unmarshal([]byte(output), &binlogTxn)
		assert.Nil(t, err)
		for _, statement := range binlogTxn.Statements {
			if string(statement.Sql) == sql {
				return
			} else {
				println(fmt.Sprintf("expected [ %s ], got [ %s ]", sql, string(statement.Sql)))
				time.Sleep(300 * time.Millisecond)
			}
		}
		position = binlogTxn.EventToken.Position
	}
}

func queryTablet(t *testing.T, vttablet cluster.Vttablet, query string, charset string) (*sqltypes.Result, error) {
	dbParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(vttablet.VttabletProcess.Directory, "mysql.sock"),

		DbName: fmt.Sprintf("vt_%s", keyspaceName),
	}
	if charset != "" {
		dbParams.Charset = charset
	}
	ctx := context.Background()
	dbConn, err := mysql.Connect(ctx, &dbParams)
	assert.Nil(t, err)
	defer dbConn.Close()
	return dbConn.ExecuteFetch(query, 1000, true)
}

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		localCluster = cluster.NewCluster(cell, hostname)
		defer localCluster.Teardown()
		os.Setenv("EXTRA_MY_CNF", path.Join(os.Getenv("VTROOT"), "config", "mycnf", "default-fast.cnf"))
		localCluster.Keyspaces = append(localCluster.Keyspaces, cluster.Keyspace{
			Name: keyspaceName,
		})

		// Start topo server
		if err := localCluster.StartTopo(); err != nil {
			return 1, err
		}

		srcMaster = localCluster.GetVttabletInstanceWithType(0, "master")
		srcReplica = localCluster.GetVttabletInstanceWithType(0, "replica")
		srcRdonly = localCluster.GetVttabletInstanceWithType(0, "rdonly")

		destMaster = localCluster.GetVttabletInstanceWithType(0, "master")
		destReplica = localCluster.GetVttabletInstanceWithType(0, "replica")
		destRdonly = localCluster.GetVttabletInstanceWithType(0, "rdonly")

		var mysqlProcs []*exec.Cmd
		for _, tablet := range []*cluster.Vttablet{srcMaster, srcReplica, srcRdonly, destMaster, destReplica, destRdonly} {
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
			tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				cell,
				"",
				keyspaceName,
				localCluster.VtctldProcess.Port,
				tablet.Type,
				localCluster.TopoPort,
				hostname,
				localCluster.TmpDirectory,
				commonTabletArg,
				true,
			)
			tablet.VttabletProcess.SupportsBackup = true
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				return 1, err
			}
			mysqlProcs = append(mysqlProcs, proc)
		}
		for _, proc := range mysqlProcs {
			if err := proc.Wait(); err != nil {
				return 1, err
			}
		}

		if err := localCluster.VtctlProcess.CreateKeyspace(keyspaceName); err != nil {
			return 1, err
		}

		shard1 := cluster.Shard{
			Name:      "0",
			Vttablets: []cluster.Vttablet{*srcMaster, *srcReplica, *srcRdonly},
		}
		for idx := range shard1.Vttablets {
			shard1.Vttablets[idx].VttabletProcess.Shard = shard1.Name
		}
		localCluster.Keyspaces[0].Shards = append(localCluster.Keyspaces[0].Shards, shard1)

		shard2 := cluster.Shard{
			Name:      "-",
			Vttablets: []cluster.Vttablet{*destMaster, *destReplica, *destRdonly},
		}
		for idx := range shard2.Vttablets {
			shard2.Vttablets[idx].VttabletProcess.Shard = shard2.Name
		}
		localCluster.Keyspaces[0].Shards = append(localCluster.Keyspaces[0].Shards, shard2)

		for _, tablet := range shard1.Vttablets {
			if err := localCluster.VtctlclientProcess.InitTablet(&tablet, cell, keyspaceName, hostname, shard1.Name); err != nil {
				return 1, err
			}
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}
		if err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard1.Name, cell, srcMaster.TabletUID); err != nil {
			return 1, err
		}

		if err := localCluster.VtctlclientProcess.ApplySchema(keyspaceName, sqlSchema); err != nil {
			return 1, err
		}
		if err := localCluster.VtctlclientProcess.ApplyVSchema(keyspaceName, vSchema); err != nil {
			return 1, err
		}

		if err := localCluster.VtctlclientProcess.ExecuteCommand("RunHealthCheck", srcReplica.Alias); err != nil {
			return 1, err
		}

		if err := localCluster.VtctlclientProcess.ExecuteCommand("RunHealthCheck", srcRdonly.Alias); err != nil {
			return 1, err
		}

		for _, tablet := range shard2.Vttablets {
			if err := localCluster.VtctlclientProcess.InitTablet(&tablet, cell, keyspaceName, hostname, shard2.Name); err != nil {
				return 1, err
			}
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}

		if err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard2.Name, cell, destMaster.TabletUID); err != nil {
			return 1, err
		}
		_ = localCluster.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
		if err := localCluster.VtctlclientProcess.ExecuteCommand("CopySchemaShard", srcReplica.Alias, fmt.Sprintf("%s/%s", keyspaceName, shard2.Name)); err != nil {
			return 1, err
		}
		localCluster.VtworkerProcess = *cluster.VtworkerProcessInstance(localCluster.GetAndReservePort(),
			localCluster.GetAndReservePort(),
			localCluster.TopoPort,
			localCluster.Hostname,
			localCluster.TmpDirectory)
		localCluster.VtworkerProcess.Cell = cell
		if err := localCluster.VtworkerProcess.ExecuteVtworkerCommand(localCluster.VtworkerProcess.Port,
			localCluster.VtworkerProcess.GrpcPort, "--use_v3_resharding_mode=true",
			"SplitClone",
			"--chunk_count", "10",
			"--min_rows_per_chunk", "1",
			"--exclude_tables", "unrelated",
			"--min_healthy_rdonly_tablets", "1",
			fmt.Sprintf("%s/%s", keyspaceName, shard1.Name)); err != nil {
			return 1, err
		}
		if err := destMaster.VttabletProcess.WaitForBinLogPlayerCount(1); err != nil {
			return 1, err
		}
		if err := destReplica.VttabletProcess.WaitForBinlogServerState("Enabled"); err != nil {
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
