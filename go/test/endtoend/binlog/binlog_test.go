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
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
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
	vSchema = `
		{
		  "sharded": true,
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

func TestSomething(t *testing.T) {
	assert.True(t, true, "it is passing")
}

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

		srcMaster = localCluster.GetVttabletInstanceWithType(0, "master")
		srcReplica = localCluster.GetVttabletInstanceWithType(0, "replica")
		srcRdonly = localCluster.GetVttabletInstanceWithType(0, "rdonly")

		destMaster = localCluster.GetVttabletInstanceWithType(0, "master")
		destReplica = localCluster.GetVttabletInstanceWithType(0, "replica")
		destRdonly = localCluster.GetVttabletInstanceWithType(0, "rdonly")

		var mysqlProcs []*exec.Cmd
		println("tablets done")
		for _, tablet := range []*cluster.Vttablet{srcMaster, srcReplica, srcRdonly, destMaster, destReplica, destRdonly} {
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
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
		localCluster.Keyspaces[0].Shards = append(localCluster.Keyspaces[0].Shards, shard1)

		shard2 := cluster.Shard{
			Name:      "1",
			Vttablets: []cluster.Vttablet{*destMaster, *destReplica, *destRdonly},
		}
		localCluster.Keyspaces[0].Shards = append(localCluster.Keyspaces[0].Shards, shard2)
		for _, tablet := range shard1.Vttablets {
			if err := localCluster.VtctlclientProcess.InitTablet(&tablet, cell, keyspaceName, hostname, shard1.Name); err != nil {
				return 1, err
			}
			if err := localCluster.StartVttablet(&tablet, "NOT_SERVING", false, cell, keyspaceName, hostname, shard1.Name); err != nil {
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
			if err := localCluster.StartVttablet(&tablet, "NOT_SERVING", false, cell, keyspaceName, hostname, shard2.Name); err != nil {
				return 1, err
			}
		}

		if err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard2.Name, cell, destMaster.TabletUID); err != nil {
			return 1, err
		}
		if err := localCluster.VtctlclientProcess.ExecuteCommand("CopySchemaShard", srcReplica.Alias, fmt.Sprintf("%s/%s", keyspaceName, shard2.Name)); err != nil {
			return 1, err
		}

		if err := localCluster.VtworkerProcess.ExecuteCommand("--cell", cell,
			"--use_v3_resharding_mode=true",
			"SplitClone",
			"--chunk_count", "10",
			"--min_rows_per_chunk", "1",
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
