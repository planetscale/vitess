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

package sequence

import (
	"encoding/json"
	"flag"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/proto/topodata"
)

var (
	clusterForKSTest      *cluster.LocalProcessCluster
	keyspaceShardedName   = "test_ks_sharded"
	keyspaceUnshardedName = "test_ks_unsharded"
	cell                  = "zone1"
	hostname              = "localhost"
	sqlSchema             = `create table vt_insert_test (
								id bigint auto_increment,
								msg varchar(64),
								keyspace_id bigint(20) unsigned NOT NULL,
								primary key (id)
								) Engine=InnoDB`
	shardKIdMap = map[string][]uint64{
		"-80": {527875958493693904, 626750931627689502,
			345387386794260318, 332484755310826578,
			1842642426274125671, 1326307661227634652,
			1761124146422844620, 1661669973250483744,
			3361397649937244239, 2444880764308344533},
		"80-": {9767889778372766922, 9742070682920810358,
			10296850775085416642, 9537430901666854108,
			10440455099304929791, 11454183276974683945,
			11185910247776122031, 10460396697869122981,
			13379616110062597001, 12826553979133932576},
	}
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterForKSTest = &cluster.LocalProcessCluster{Cell: cell, Hostname: hostname}
		defer clusterForKSTest.Teardown()

		// Start topo server
		if err := clusterForKSTest.StartTopo(); err != nil {
			return 1
		}

		// Start sharded keyspace
		keyspaceSharded := &cluster.Keyspace{
			Name:      keyspaceShardedName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterForKSTest.StartKeyspace(*keyspaceSharded, []string{"-80", "80-"}, 1, false); err != nil {
			return 1
		}
		if err := clusterForKSTest.VtctlclientProcess.ExecuteCommand("SetKeyspaceShardingInfo", "-force", keyspaceShardedName, "keyspace_id", "uint64"); err != nil {
			return 1
		}
		if err := clusterForKSTest.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceShardedName); err != nil {
			return 1
		}

		// Start unsharded keyspace
		keyspaceUnsharded := &cluster.Keyspace{
			Name:      keyspaceUnshardedName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterForKSTest.StartKeyspace(*keyspaceUnsharded, []string{keyspaceUnshardedName}, 1, false); err != nil {
			return 1
		}
		if err := clusterForKSTest.VtctlclientProcess.ExecuteCommand("SetKeyspaceShardingInfo", "-force", keyspaceUnshardedName, "keyspace_id", "uint64"); err != nil {
			return 1
		}
		if err := clusterForKSTest.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceUnshardedName); err != nil {
			return 1
		}

		// Start vtgate
		if err := clusterForKSTest.StartVtgate(); err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestGetSrvKeyspaceNames(t *testing.T) {
	output, err := clusterForKSTest.VtctlclientProcess.ExecuteCommandWithOutput("GetSrvKeyspaceNames", cell)
	assert.Nil(t, err)
	assert.Contains(t, strings.Split(output, "\n"), keyspaceUnshardedName)
	assert.Contains(t, strings.Split(output, "\n"), keyspaceShardedName)
}

func TestShardNames(t *testing.T) {
	output, err := clusterForKSTest.VtctlclientProcess.ExecuteCommandWithOutput("GetSrvKeyspace", cell, keyspaceShardedName)
	assert.Nil(t, err)
	var srvKeyspace topodata.SrvKeyspace

	err = json.Unmarshal([]byte(output), &srvKeyspace)
	assert.Nil(t, err)
}

func TestGetKeyspace(t *testing.T) {
	output, err := clusterForKSTest.VtctlclientProcess.ExecuteCommandWithOutput("GetKeyspace", keyspaceUnshardedName)
	assert.Nil(t, err)

	var keyspace topodata.Keyspace

	err = json.Unmarshal([]byte(output), &keyspace)
	assert.Nil(t, err)

	assert.Equal(t, keyspace.ShardingColumnName, "keyspace_id")
	assert.Equal(t, keyspace.ShardingColumnType, topodata.KeyspaceIdType(1))
}
