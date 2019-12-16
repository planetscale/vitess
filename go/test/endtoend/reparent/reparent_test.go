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

package reparent

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestMasterToSpareStateChangeImpossible(t *testing.T) {
	// Making tablet as master
	tablet62344.Type = "master"

	// Init Tablet
	err := clusterInstance.VtctlclientProcess.InitTablet(tablet62344, tablet62344.Cell, keyspaceName, hostname, shardName)
	assert.Nil(t, err)

	// Start the tablet
	err = tablet62344.VttabletProcess.Setup()
	assert.Nil(t, err)

	// Create Database
	err = tablet62344.VttabletProcess.CreateDB(keyspaceName)
	assert.Nil(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", tablet62344.Alias, "spare")
	assert.NotNil(t, err)

	//killTablets(t, []cluster.Vttablet{*tablet62344})
	err = tablet62344.VttabletProcess.TearDown()
	assert.Nil(t, err)

	// Reset status and type
	tablet62344.VttabletProcess.ServingStatus = "NOT_SERVING"
	tablet62344.Type = "replica"
}

func TestReparentDownMaster(t *testing.T) {
	ctx := context.Background()

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// Create Database
		err := tablet62344.VttabletProcess.CreateDB(keyspaceName)
		assert.Nil(t, err)

		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		assert.Nil(t, err)

		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		assert.Nil(t, err)
	}

	// Init Shard Master
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shardName), tablet62344.Alias)
	assert.Nil(t, err)

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		err := tablet.VttabletProcess.WaitForTabletType("SERVING")
		assert.Nil(t, err)
	}

	// Validate topology
	validateTopology(t, false)

	// create Tables
	runSQL(t, sqlSchema, tablet62344, ctx)

	// Make the current master agent and database unavailable.
	err = tablet62344.VttabletProcess.TearDown()
	assert.Nil(t, err)
	err = tablet62344.MysqlctlProcess.Stop()
	assert.Nil(t, err)

	// Perform a planned reparent operation, will try to contact
	// the current master and fail somewhat quickly
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-wait-time", "5s",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet62044.Alias)
	assert.NotNil(t, err)

	// Run forced reparent operation, this should now proceed unimpeded.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"EmergencyReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet62044.Alias)
	assert.Nil(t, err)

	validateTopology(t, false)

	checkMasterTablet(t, tablet62044)

	// insert data into the new master, check the connected slaves work
	index := 2
	insertSQL := fmt.Sprintf(insertSQL, index, index)
	runSQL(t, insertSQL, tablet62044, ctx)
	err = checkInsertedValues(t, tablet41983, index, ctx)
	assert.Nil(t, err)
	err = checkInsertedValues(t, tablet31981, index, ctx)
	assert.Nil(t, err)

	// bring back the old master as a slave, check that it catches up
	tablet62344.MysqlctlProcess.InitMysql = false
	err = tablet62344.MysqlctlProcess.Start()
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.InitTablet(tablet62344, tablet62344.Cell, keyspaceName, hostname, shardName)
	assert.Nil(t, err)
	// As there is already a master the new slave will come directly in SERVING state
	tablet62344.VttabletProcess.ServingStatus = "SERVING"
	// Start the tablet
	err = tablet62344.VttabletProcess.Setup()
	assert.Nil(t, err)

	err = checkInsertedValues(t, tablet62344, index, ctx)
	assert.Nil(t, err)

	// Kill tablets
	killTablets(t)
}

func TestReparentCrossCell(t *testing.T) {
	// Create a few slaves for testing reparenting. Won't be healthy as replication is not running.
	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// create database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		assert.Nil(t, err)
		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		assert.Nil(t, err)
		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		assert.Nil(t, err)
	}

	// Force the slaves to reparent assuming that all the datasets are identical.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shardName), tablet62344.Alias)
	assert.Nil(t, err)

	// Validate topology
	validateTopology(t, true)

	checkMasterTablet(t, tablet62344)

	// Perform a graceful reparent operation to another cell.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet31981.Alias)
	assert.Nil(t, err)

	validateTopology(t, false)

	checkMasterTablet(t, tablet31981)

	// Kill tablets
	killTablets(t)

}

//func TestReparentGracefulRangeBased(t *testing.T){
//	//err := clusterInstance.VtctlclientProcess.ExecuteCommand("CreateKeyspace",
//	//	"--sharding_column_name", "keyspace_id",
//	//	"--sharding_column_type", "uint64",
//	//	keyspaceName)
//	//assert.Nil(t, err)
//	reparentGraceful(t, "0000000000000000-ffffffffffffffff", false)
//}

func ReparentGraceful(t *testing.T) {
	reparentGraceful(t, shardName, false)
}

//func TestReparentGracefulRecovery(t *testing.T){
//	reparentGraceful(t, shardName, true)
//}

func reparentGraceful(t *testing.T, shardID string, confusedMaster bool) {
	ctx := context.Background()

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// create database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		assert.Nil(t, err)
		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardID)
		assert.Nil(t, err)
		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		assert.Nil(t, err)
	}

	// Force the slaves to reparent assuming that all the datasets are identical.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shardID), tablet62344.Alias)
	assert.Nil(t, err)

	// Validate topology
	validateTopology(t, true)

	// create Tables
	runSQL(t, sqlSchema, tablet62344, ctx)

	checkMasterTablet(t, tablet62344)

	validateTopology(t, false)

	// Run this to make sure it succeeds.
	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"ShardReplicationPositions", fmt.Sprintf("%s/%s", keyspaceName, shardID))
	strArray := strings.Split(output, "\n")
	if strArray[len(strArray)-1] == "" {
		strArray = strArray[:len(strArray)-1] // Truncate slice, remove empty line
	}
	assert.Equal(t, 4, len(strArray))         // one master, three slaves
	assert.Contains(t, strArray[0], "master") // master first

	// Perform a graceful reparent operation
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", fmt.Sprintf("%s/%s", keyspaceName, shardID),
		"-new_master", tablet62044.Alias)
	assert.Nil(t, err)

	// Validate topology
	validateTopology(t, false)

	// Simulate a master that forgets it's master and becomes replica.
	// PlannedReparentShard should be able to recover by reparenting to the same master again,
	// as long as all tablets are available to check that it's safe.
	if confusedMaster {
		err = clusterInstance.VtctlclientProcess.InitTablet(tablet62044, tablet62044.Cell, keyspaceName, hostname, shardID)
		assert.Nil(t, err)

		err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshState", tablet62044.Alias)
		assert.Nil(t, err)
	}

	// Perform a graceful reparent to the same master.
	// It should be idempotent, and should fix any inconsistencies if necessary
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", fmt.Sprintf("%s/%s", keyspaceName, shardID),
		"-new_master", tablet62044.Alias)
	assert.Nil(t, err)

	// Validate topology
	validateTopology(t, false)

	checkMasterTablet(t, tablet62044)

	// insert data into the new master, check the connected slaves work
	insertSQL := fmt.Sprintf(insertSQL, 1, 1)
	runSQL(t, insertSQL, tablet62044, ctx)
	err = checkInsertedValues(t, tablet41983, 1, ctx)
	assert.Nil(t, err)
	err = checkInsertedValues(t, tablet62344, 1, ctx)
	assert.Nil(t, err)

	// Kill tablets
	killTablets(t)

}

func TestReparentSlaveOffline(t *testing.T) {

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// create database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		assert.Nil(t, err)
		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		assert.Nil(t, err)
		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		assert.Nil(t, err)
	}

	// Force the slaves to reparent assuming that all the datasets are identical.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", keyspaceShard, tablet62344.Alias)
	assert.Nil(t, err)

	// Validate topology
	validateTopology(t, true)

	checkMasterTablet(t, tablet62344)

	// Kill one tablet so we seem offline
	err = tablet31981.VttabletProcess.TearDown()
	assert.Nil(t, err)

	// Perform a graceful reparent operation.
	out, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet62044.Alias)
	assert.NotNil(t, err)
	assert.Contains(t, out, "tablet zone2-0000031981 SetMaster failed")

	checkMasterTablet(t, tablet62044)

	killTablets(t)
}

func TestReparentAvoid(t *testing.T) {

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet31981} {
		// create database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		assert.Nil(t, err)
		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		assert.Nil(t, err)
		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		assert.Nil(t, err)
	}

	// Force the slaves to reparent assuming that all the datasets are identical.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", keyspaceShard, tablet62344.Alias)
	assert.Nil(t, err)

	// Validate topology
	validateTopology(t, true)

	checkMasterTablet(t, tablet62344)

	// Perform a reparent operation with avoid_master pointing to non-master. It
	// should succeed without doing anything.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-avoid_master", tablet62044.Alias)
	assert.Nil(t, err)

	validateTopology(t, false)

	checkMasterTablet(t, tablet62344)

	// Perform a reparent operation with avoid_master pointing to master.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-avoid_master", tablet62344.Alias)
	assert.Nil(t, err)

	validateTopology(t, false)

	// 62044 is in the same cell and 31981 is in a different cell, so we must land on 62044
	checkMasterTablet(t, tablet62044)

	// If we kill the tablet in the same cell as master then reparent
	// -avoid_master will fail.
	err = tablet62344.VttabletProcess.TearDown()
	assert.Nil(t, err)

	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-avoid_master", tablet62044.Alias)
	assert.NotNil(t, err)
	assert.Contains(t, output, "cannot find a tablet to reparent to")

	validateTopology(t, false)

	checkMasterTablet(t, tablet62044)

	killTablets(t)
}

// Makes sure the tablet type is master, and its health check agrees.
func checkMasterTablet(t *testing.T, tablet *cluster.Vttablet) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tablet.Alias)
	assert.Nil(t, err)
	var tabletInfo topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tabletInfo)
	assert.Nil(t, err)
	assert.Equal(t, topodatapb.TabletType_MASTER, tabletInfo.GetType())

	//if port {
	//	assert.Equal(t, port, tabletInfo.GetPortMap()["vt"])
	//}

	// make sure the health stream is updated
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", tablet.Alias)
	assert.Nil(t, err)
	var streamHealthResponse querypb.StreamHealthResponse

	err = json2.Unmarshal([]byte(result), &streamHealthResponse)
	assert.Nil(t, err)

	assert.True(t, streamHealthResponse.GetServing())
	tabletType := streamHealthResponse.GetTarget().GetTabletType()
	assert.Equal(t, topodatapb.TabletType_MASTER, tabletType)

}

func checkInsertedValues(t *testing.T, tablet *cluster.Vttablet, index int, ctx context.Context) error {
	// wait until it gets the data
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		selectSQL := fmt.Sprintf("select msg from vt_insert_test where id=%d", index)
		qr := runSQL(t, selectSQL, tablet, ctx)
		if len(qr.Rows) == 1 {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("Data is not yet replicated!")
}

func killTablets(t *testing.T) {
	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		fmt.Println("Teardown tablet: ", tablet.Alias)
		err := tablet.VttabletProcess.TearDown()
		assert.Nil(t, err)

		// Reset status and type
		tablet.VttabletProcess.ServingStatus = "NOT_SERVING"
		tablet.Type = "replica"

	}
}

func validateTopology(t *testing.T, pingTablets bool) {
	if pingTablets {
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("Validate", "-ping-tablets=true")
		assert.Nil(t, err)
	} else {
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
		assert.Nil(t, err)
	}
}
