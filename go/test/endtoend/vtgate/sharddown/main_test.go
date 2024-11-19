/*
Copyright 2024 The Vitess Authors.

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

package sharddown

import (
	"context"
	_ "embed"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	cell            = "zone1"
	hostname        = "localhost"
	vtParams        mysql.ConnParams
	unsKs           = "commerce"
	unsSchema       = `
CREATE TABLE t1_seq (
    id INT, 
    next_id BIGINT, 
    cache BIGINT, 
    PRIMARY KEY(id)
) comment 'vitess_sequence';

INSERT INTO t1_seq (id, next_id, cache) values(0, 1, 1000);
`

	unsVSchema = `
{
  "sharded": false,
  "tables": {}
}
`
	sKs = "customer"
	//go:embed sharded_schema.sql
	sSchema string

	//go:embed sharded_vschema.json
	sVSchema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		uKeyspace := &cluster.Keyspace{
			Name:      unsKs,
			SchemaSQL: unsSchema,
			VSchema:   unsVSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*uKeyspace, 0, false); err != nil {
			return 1
		}

		sKeyspace := &cluster.Keyspace{
			Name:      sKs,
			SchemaSQL: sSchema,
			VSchema:   sVSchema,
		}
		if err := clusterInstance.StartKeyspace(*sKeyspace, []string{"-40", "40-80", "80-c0", "c0-"}, 0, false); err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--enable_buffer")
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		vtParams = clusterInstance.GetVTParams(sKs)

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestShardDownInTx(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	// initial data
	utils.Exec(t, conn, `insert into t1(c2, c3, c4) values (1,10,'100'),(2,20,'200'),(3,30,'300'),(4,40,'400'),(5,50,'500')`)

	// retrieve -40 shard
	tabletProcess := clusterInstance.Keyspaces[1].Shards[0].Vttablets[0].VttabletProcess

	// start transaction
	utils.Exec(t, conn, `begin`)
	utils.Exec(t, conn, `select c1 from t1 where c2 > 1`)

	// tear down -40 shard
	err := tabletProcess.TearDown()
	require.NoError(t, err)

	_, err = utils.ExecAllowError(t, conn, `select c1 from t1 where c2 <= 1 limit 5`)
	assert.ErrorContains(t, err, "target: customer.-40.primary")
	assert.ErrorContains(t, err, "either down or nonexistent")

	_, err = utils.ExecAllowError(t, conn, `rollback`)
	assert.ErrorContains(t, err, "either down or nonexistent")

	// bring back -40 shard
	tabletProcess.ServingStatus = "SERVING"
	err = tabletProcess.Setup()
	require.NoError(t, err)
}

func TestShardDownWithoutTx(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	// initial data
	utils.Exec(t, conn, `insert into t1(c2, c3, c4) values (1,10,'100'),(2,20,'200'),(3,30,'300'),(4,40,'400'),(5,50,'500')`)

	// retrieve -40 shard
	tabletProcess := clusterInstance.Keyspaces[1].Shards[0].Vttablets[0].VttabletProcess

	// test query
	utils.Exec(t, conn, `select c1 from t1 where c2 > 1 limit 2`)

	// tear down -40 shard
	err := tabletProcess.TearDown()
	require.NoError(t, err)

	_, err = utils.ExecAllowError(t, conn, `select c1 from t1 where c2 <= 1 limit 5`)
	assert.ErrorContains(t, err, "target: customer.-40.primary")
	assert.ErrorContains(t, err, "primary is not serving")

	// bring back -40 shard
	tabletProcess.ServingStatus = "SERVING"
	err = tabletProcess.Setup()
	require.NoError(t, err)
}

func TestMySQLDownWithoutTx(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	// initial data
	utils.Exec(t, conn, `insert into t1(c2, c3, c4) values (1,10,'100'),(2,20,'200'),(3,30,'300'),(4,40,'400'),(5,50,'500')`)

	// retrieve -40 shard
	mysqlProcess := clusterInstance.Keyspaces[1].Shards[0].Vttablets[0].MysqlctlProcess

	// test query
	utils.Exec(t, conn, `select c1 from t1 where c2 > 1 limit 2`)

	// tear down -40 shard
	err := mysqlProcess.Stop()
	require.NoError(t, err)

	_, err = utils.ExecAllowError(t, conn, `select c1 from t1 where c2 <= 1 limit 5`)
	assert.ErrorContains(t, err, "target: customer.-40.primary")
	assert.ErrorContains(t, err, "primary is not serving")

	// bring back -40 shard
	err = mysqlProcess.StartProvideInit(false)
	require.NoError(t, err)
}

func start(t *testing.T) (*mysql.Conn, func()) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, conn, "delete from t1")
	}

	deleteAll()

	return conn, func() {
		deleteAll()
		conn.Close()
		cluster.PanicHandler(t)
	}
}
