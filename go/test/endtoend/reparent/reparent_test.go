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
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestMasterToSpareStateChangeImpossible(t *testing.T) {
	defer cluster.PanicHandler(t)

	args := []string{"InitTablet", "-hostname", hostname,
		"-port", fmt.Sprintf("%d", tablet62344.HTTPPort), "-allow_update", "-parent",
		"-keyspace", keyspaceName,
		"-shard", shardName,
		"-mysql_port", fmt.Sprintf("%d", tablet62344.MySQLPort),
		"-grpc_port", fmt.Sprintf("%d", tablet62344.GrpcPort)}
	args = append(args, fmt.Sprintf("%s-%010d", tablet62344.Cell, tablet62344.TabletUID), "master")
	err := clusterInstance.VtctlclientProcess.ExecuteCommand(args...)
	require.NoError(t, err)

	// Start the tablet
	err = tablet62344.VttabletProcess.Setup()
	require.NoError(t, err)

	// Create Database
	err = tablet62344.VttabletProcess.CreateDB(keyspaceName)
	require.NoError(t, err)

	// We cannot change a master to spare
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeReplicaType", tablet62344.Alias, "spare")
	require.Error(t, err)

	//kill Tablet
	err = tablet62344.VttabletProcess.TearDown()
	require.NoError(t, err)
}

func TestReparentDownMaster(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// Create Database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.NoError(t, err)

		// Reset status, don't wait for the tablet status. We will check it later
		tablet.VttabletProcess.ServingStatus = ""

		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.NoError(t, err)
	}

	// Init Shard Master
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		fmt.Sprintf("%s/%s", keyspaceName, shardName), tablet62344.Alias)
	require.NoError(t, err)

	validateTopology(t, true)

	// create Tables
	runSQL(ctx, t, sqlSchema, tablet62344)

	// Make the current master agent and database unavailable.
	err = tablet62344.VttabletProcess.TearDown()
	require.NoError(t, err)
	err = tablet62344.MysqlctlProcess.Stop()
	require.NoError(t, err)

	// Perform a planned reparent operation, will try to contact
	// the current master and fail somewhat quickly
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"-action_timeout", "1s",
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet62044.Alias)
	require.Error(t, err)

	// Run forced reparent operation, this should now proceed unimpeded.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"EmergencyReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet62044.Alias,
		"-wait_replicas_timeout", "31s")
	require.NoError(t, err)

	validateTopology(t, false)

	checkMasterTablet(t, tablet62044)

	// insert data into the new master, check the connected replica work
	insertSQL := fmt.Sprintf(insertSQL, 2, 2)
	runSQL(ctx, t, insertSQL, tablet62044)
	err = checkInsertedValues(ctx, t, tablet41983, 2)
	require.NoError(t, err)
	err = checkInsertedValues(ctx, t, tablet31981, 2)
	require.NoError(t, err)

	// bring back the old master as a replica, check that it catches up
	tablet62344.MysqlctlProcess.InitMysql = false
	err = tablet62344.MysqlctlProcess.Start()
	require.NoError(t, err)

	// As there is already a master the new replica will come directly in SERVING state
	tablet62344.VttabletProcess.ServingStatus = "SERVING"
	// Start the tablet
	err = tablet62344.VttabletProcess.Setup()
	require.NoError(t, err)

	err = checkInsertedValues(ctx, t, tablet62344, 2)
	require.NoError(t, err)

	// Kill tablets
	killTablets(t)
}

// Makes sure the tablet type is master, and its health check agrees.
func checkMasterTablet(t *testing.T, tablet *cluster.Vttablet) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tablet.Alias)
	require.NoError(t, err)
	var tabletInfo topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tabletInfo)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_MASTER, tabletInfo.GetType())

	// make sure the health stream is updated
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", tablet.Alias)
	require.NoError(t, err)
	var streamHealthResponse querypb.StreamHealthResponse

	err = json2.Unmarshal([]byte(result), &streamHealthResponse)
	require.NoError(t, err)

	assert.True(t, streamHealthResponse.GetServing())
	tabletType := streamHealthResponse.GetTarget().GetTabletType()
	assert.Equal(t, topodatapb.TabletType_MASTER, tabletType)

}

func checkInsertedValues(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, index int) error {
	// wait until it gets the data
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		selectSQL := fmt.Sprintf("select msg from vt_insert_test where id=%d", index)
		qr := runSQL(ctx, t, selectSQL, tablet)
		if len(qr.Rows) == 1 {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("data is not yet replicated")
}

func validateTopology(t *testing.T, pingTablets bool) {
	if pingTablets {
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("Validate", "-ping-tablets=true")
		require.NoError(t, err)
	} else {
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
		require.NoError(t, err)
	}
}

func killTablets(t *testing.T) {
	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		log.Infof("Calling TearDown on tablet %v", tablet.Alias)
		err := tablet.VttabletProcess.TearDown()
		require.NoError(t, err)

		// Reset status and type
		tablet.VttabletProcess.ServingStatus = ""
		tablet.Type = "replica"
	}
}
