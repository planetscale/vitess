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

package recovery

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	recoveryKS1  = "recovery_ks1"
	recoveryKS2  = "recovery_ks1"
	vtInsertTest = `create table vt_insert_test (
					  id bigint auto_increment,
					  msg varchar(64),
					  primary key (id)
					  ) Engine=InnoDB`
	vSchema = `{
    "tables": {
        "vt_insert_test": {}
    }
}`
)

//TestRecovery does following
//- create a shard with master and replica1 only
//- run InitShardMaster
//- insert some data
//- take a backup
//- insert more data on the master
//- take another backup
//- create a recovery keyspace after first backup
//- bring up tablet_replica2 in the new keyspace
//- check that new tablet does not have data created after backup1
//- create second recovery keyspace after second backup
//- bring up tablet_replica3 in second keyspace
//- check that new tablet has data created after backup1 but not data created after backup2
//- check that vtgate queries work correctly

func TestRecovery(t *testing.T) {
	verifyInitialReplication(t)

	err := localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.Nil(t, err)

	backups := listBackups(t)
	assert.Equal(t, len(backups), 1)
	assert.Contains(t, backups[0], replica1.Alias)

	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	assert.Nil(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 2)

	err = localCluster.VtctlclientProcess.ApplyVSchema(keyspaceName, vSchema)
	assert.Nil(t, err)

	output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetVSchema", keyspaceName)
	assert.Nil(t, err)
	assert.Contains(t, output, "vt_insert_test")

	restoreTablet(t, replica2, recoveryKS1)

	output, err = localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetSrvVSchema", cell)
	assert.Nil(t, err)
	println("---GetSrvVSchema----")
	println(output)

	output, err = localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetSrvKeyspace", cell, keyspaceName)
	assert.Nil(t, err)
	println("---GetSrvKeyspace----")
	println(output)

	output, err = localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetVSchema", recoveryKS1)
	assert.Nil(t, err)
	println("---GetVSchema recovery_ks1----")
	println(output)

	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 1)

	cluster.VerifyLocalMetadata(t, replica2, recoveryKS1, shardName, cell)

	// update the original row in master
	_, err = master.VttabletProcess.QueryTablet("update vt_insert_test set msg = 'msgx1' where id = 1", keyspaceName, true)
	assert.Nil(t, err)

	//verify that master has new value
	qr, err := master.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
	assert.Nil(t, err)
	assert.Equal(t, "msgx1", fmt.Sprintf("%s", qr.Rows[0][0].ToBytes()))

	//verify that restored replica has old value
	qr, err = replica2.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
	assert.Nil(t, err)
	assert.Equal(t, "msg1", fmt.Sprintf("%s", qr.Rows[0][0].ToBytes()))

	//err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	//assert.Nil(t, err)
	//
	//_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test3')", keyspaceName, true)
	//assert.Nil(t, err)
	//cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 3)
	//
	//restoreTablet(t, replica3, recoveryKS2)
	//
	//output, err = localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetVSchema", recoveryKS2)
	//assert.Nil(t, err)
	//assert.Contains(t, output, "vt_insert_test")
	//
	//cluster.VerifyRowsInTablet(t, replica3, keyspaceName, 2)
	//
	//// update the original row in master
	//_, err = master.VttabletProcess.QueryTablet("update vt_insert_test set msg = 'msgx2' where id = 1", keyspaceName, true)
	//assert.Nil(t, err)
	//
	////verify that master has new value
	//qr, err = master.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
	//assert.Nil(t, err)
	//assert.Equal(t, "msgx2", fmt.Sprintf("%s", qr.Rows[0][0].ToBytes()))
	//
	////verify that restored replica has old value
	//qr, err = replica3.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
	//assert.Nil(t, err)
	//assert.Equal(t, "msg1", fmt.Sprintf("%s", qr.Rows[0][0].ToBytes()))

	err = localCluster.StartVtgate()
	assert.Nil(t, err)
	err = localCluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shardName), 1)
	assert.Nil(t, err)
	err = localCluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), 1)
	assert.Nil(t, err)
	err = localCluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", recoveryKS1, shardName), 1)
	assert.Nil(t, err)
	err = localCluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", recoveryKS2, shardName), 1)
	assert.Nil(t, err)

	//check that vtgate doesn't route queries to new tablet
	verifyQueriesUsingVtgate(t, "select count(*) from vt_insert_test", "", 3)
	verifyQueriesUsingVtgate(t, "select msg from vt_insert_test where id = 1", "msgx2", 0)
	verifyQueriesUsingVtgate(t, fmt.Sprintf("select count(*) from %s.vt_insert_test", recoveryKS1), "", 1)
	verifyQueriesUsingVtgate(t, fmt.Sprintf("select msg from %s.vt_insert_test where id = 1", recoveryKS1), "msg1", 0)
	verifyQueriesUsingVtgate(t, fmt.Sprintf("select count(*) from %s.vt_insert_test", recoveryKS2), "", 2)
	verifyQueriesUsingVtgate(t, fmt.Sprintf("select msg from %s.vt_insert_test where id = 1", recoveryKS2), "msgx1", 0)
}

func verifyQueriesUsingVtgate(t *testing.T, query string, strValue string, intValue int) {
	result, err := localCluster.VtctlProcess.ExecuteCommandWithOutput("VtGateExecute", "-json",
		"-server", fmt.Sprintf("%s:%d", localCluster.Hostname, localCluster.VtgateProcess.GrpcPort),
		"-target", "@replica",
		query)
	assert.Nil(t, err)
	var sqlResult sqltypes.Result

	err = json.Unmarshal([]byte(result), &sqlResult)
	assert.Nil(t, err)
	if intValue > 0 {
		assert.Equal(t, intValue, fmt.Sprintf("%d", sqlResult.Rows[0][0].ToBytes()))
	} else {
		assert.Equal(t, strValue, fmt.Sprintf("%s", sqlResult.Rows[0][0].ToBytes()))
	}

}

// This will create schema in master, insert some data to master and verify the same data in replica
func verifyInitialReplication(t *testing.T) {
	_, err := master.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	assert.Nil(t, err)
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	assert.Nil(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)
}

func restoreTablet(t *testing.T, tablet *cluster.Vttablet, restoreKSName string) {
	err := cluster.ResetTabletDirectory(*tablet)
	assert.Nil(t, err)
	tm := time.Now().UTC()
	tm.Format(time.RFC3339)
	output, err := localCluster.VtctlProcess.ExecuteCommandWithOutput("CreateKeyspace",
		"-keyspace_type=SNAPSHOT", "-base_keyspace="+keyspaceName,
		"-snapshot_time", tm.Format(time.RFC3339), restoreKSName)
	assert.Nil(t, err)
	println("---- create keysapce output ----")
	println(output)

	replicaTabletArgs := commonTabletArg
	replicaTabletArgs = append(replicaTabletArgs, "-disable_active_reparents",
		"-enable_replication_reporter=false",
		"-init_tablet_type", "replica",
		"-init_keyspace", restoreKSName,
		"-init_shard", "0")
	tablet.VttabletProcess.SupportsBackup = true
	tablet.VttabletProcess.ExtraArgs = replicaTabletArgs

	tablet.VttabletProcess.ServingStatus = ""
	println("starting tablet - " + tablet.Alias)
	err = tablet.VttabletProcess.Setup()
	assert.Nil(t, err)
	println("done starting tablet - " + tablet.Alias + " wating till serving status")

	err = tablet.VttabletProcess.WaitForTabletTypeForTimeout("SERVING", 20*time.Second)
	assert.Nil(t, err)
	println("done waiting for status")
}
func listBackups(t *testing.T) []string {
	output, err := localCluster.ListBackups(shardKsName)
	assert.Nil(t, err)
	return output
}
