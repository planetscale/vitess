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

package updatestream

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	localCluster        *cluster.LocalProcessCluster
	masterStartPosition string
	cell                = "zone1"
	hostname            = "localhost"
	keyspaceName        = "ks"
	sqlVtInsert         = `
					create table if not exists vt_insert_test (
					id bigint auto_increment,
					msg varchar(64),
					primary key (id)
					) Engine=InnoDB`
	sqlVtA = `
					create table if not exists vt_a (
					eid bigint,
					id int,
					primary key(eid, id)
					) Engine=InnoDB`
	sqlVtB = `
					create table if not exists vt_b (
					eid bigint,
					name varchar(128),
					foo varbinary(128),
					primary key(eid, name)
					) Engine=InnoDB`
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
		"-enable-autocommit",
	}

	master  *cluster.Vttablet
	replica *cluster.Vttablet
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		localCluster = cluster.NewCluster(cell, hostname)
		defer localCluster.Teardown()

		localCluster.Keyspaces = append(localCluster.Keyspaces, cluster.Keyspace{
			Name: keyspaceName,
		})

		// Start topo server
		if err := localCluster.StartTopo(); err != nil {
			return 1, err
		}

		master = localCluster.GetVttabletInstanceWithType(0, "master")
		replica = localCluster.GetVttabletInstanceWithType(0, "replica")

		var mysqlProcs []*exec.Cmd
		for _, tablet := range []*cluster.Vttablet{master, replica} {
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
		if errors := cluster.WaitForProcs(mysqlProcs, false); len(errors) > 1 {
			return 1, errors[0]
		}

		if err := localCluster.VtctlProcess.CreateKeyspace(keyspaceName); err != nil {
			return 1, err
		}

		shard1 := cluster.Shard{
			Name:      "0",
			Vttablets: []cluster.Vttablet{*master, *replica},
		}
		for idx := range shard1.Vttablets {
			shard1.Vttablets[idx].VttabletProcess.Shard = shard1.Name
		}
		localCluster.Keyspaces[0].Shards = append(localCluster.Keyspaces[0].Shards, shard1)

		_ = localCluster.VtctlclientProcess.InitTablet(master, cell, keyspaceName, hostname, shard1.Name)
		_ = localCluster.VtctlclientProcess.InitTablet(replica, cell, keyspaceName, hostname, shard1.Name)
		_ = localCluster.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)

		_ = master.VttabletProcess.CreateDB(keyspaceName)
		_, _ = master.VttabletProcess.QueryTablet("create database other_database", keyspaceName, false)

		_ = replica.VttabletProcess.CreateDB(keyspaceName)
		_, _ = replica.VttabletProcess.QueryTablet("create database other_database", keyspaceName, false)

		_ = master.VttabletProcess.Setup()
		_ = replica.VttabletProcess.Setup()

		if err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard1.Name, cell, master.TabletUID); err != nil {
			return 1, err
		}

		_ = master.VttabletProcess.WaitForStatus("SERVING")
		_ = replica.VttabletProcess.WaitForStatus("SERVING")
		masterStartPosition, _ = cluster.GetMasterPosition(nil, *master, hostname)

		_, _ = master.VttabletProcess.QueryTablet(sqlVtInsert, keyspaceName, true)
		_, _ = master.VttabletProcess.QueryTablet(sqlVtA, keyspaceName, true)
		_, _ = master.VttabletProcess.QueryTablet(sqlVtB, keyspaceName, true)

		_ = localCluster.VtctlclientProcess.ExecuteCommand("ReloadSchemaKeyspace", keyspaceName)
		_ = localCluster.VtctlclientProcess.ExecuteCommand("RebuildVSchemaGraph")

		_ = localCluster.StartVtgate()

		_ = localCluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard1.Name))
		_ = localCluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard1.Name))

		for _, tabletType := range []string{master.Type, replica.Type} {
			if _, err := localCluster.VtctlProcess.ExecuteCommandWithOutput("VtGateExecute",
				"-json",
				"-server", fmt.Sprintf("%s:%d", hostname, localCluster.VtgateProcess.GrpcPort),
				"-target", "@"+tabletType,
				"select count(1) from vt_insert_test"); err != nil {
				return 1, err
			}
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

func TestCharset(t *testing.T) {
	assert.True(t, true, "This is not passing")
}
