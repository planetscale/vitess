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
package tabletmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestRepeatedInitShardMaster(t *testing.T) {
	// Test that using InitShardMaster can go back and forth between 2 hosts.

	// Check tablet health
	checkHealth(t, masterTablet.HTTPPort, false)
	checkHealth(t, replicaTablet.HTTPPort, false)

	// Make replica tablet as master
	err := clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, replicaTablet.TabletUID)
	assert.Nil(t, err, "error should be Nil")

	// Run health check on both, make sure they are both healthy.
	// Also make sure the types are correct.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", masterTablet.Alias)
	assert.Nil(t, err, "error should be Nil")
	checkHealth(t, masterTablet.HTTPPort, false)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", replicaTablet.Alias)
	assert.Nil(t, err, "error should be Nil")
	checkHealth(t, replicaTablet.HTTPPort, false)

	checkTabletType(t, masterTablet.Alias, "REPLICA")
	checkTabletType(t, replicaTablet.Alias, "MASTER")

	// Come back to the original guy.
	err = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, masterTablet.TabletUID)
	assert.Nil(t, err, "error should be Nil")

	// Run health check on both, make sure they are both healthy.
	// Also make sure the types are correct.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", masterTablet.Alias)
	assert.Nil(t, err, "error should be Nil")
	checkHealth(t, masterTablet.HTTPPort, false)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", replicaTablet.Alias)
	assert.Nil(t, err, "error should be Nil")
	checkHealth(t, replicaTablet.HTTPPort, false)

	checkTabletType(t, masterTablet.Alias, "MASTER")
	checkTabletType(t, replicaTablet.Alias, "REPLICA")
}

func TestMasterRestartSetsTERTimestamp(t *testing.T) {
	// Test that TER timestamp is set when we restart the MASTER vttablet.
	// TER = TabletExternallyReparented.
	// See StreamHealthResponse.tablet_externally_reparented_timestamp for details.

	ctx := context.Background()
	rTablet := clusterInstance.GetVttabletInstance(replicaUID)

	// Init Tablets
	err := clusterInstance.VtctlclientProcess.InitTablet(rTablet, cell, keyspaceName, hostname, shardName)
	assert.Nil(t, err, "error should be Nil")

	// Start Mysql Processes
	err = cluster.StartMySQL(ctx, rTablet, username, clusterInstance.TmpDirectory)
	assert.Nil(t, err, "error should be Nil")

	// Start Vttablet
	err = clusterInstance.StartVttablet(rTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	assert.Nil(t, err, "error should be Nil")

	// Make this tablet as master
	err = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, rTablet.TabletUID)
	assert.Nil(t, err, "error should be Nil")
	waitForTabletStatus(*rTablet, "SERVING")

	// Capture the current TER.
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", rTablet.Alias)

	var streamHealthRes1 querypb.StreamHealthResponse
	err = json.Unmarshal([]byte(result), &streamHealthRes1)
	assert.Nil(t, err, "error should be Nil")

	actualType := streamHealthRes1.GetTarget().GetTabletType()
	tabletType := topodatapb.TabletType_value["MASTER"]
	got := fmt.Sprintf("%d", actualType)
	want := fmt.Sprintf("%d", tabletType)
	assert.Equal(t, want, got)

	assert.NotNil(t, streamHealthRes1.GetTabletExternallyReparentedTimestamp())
	assert.True(t, streamHealthRes1.GetTabletExternallyReparentedTimestamp() > 0, "TER on MASTER must be set after InitShardMaster")

	// Restart the MASTER vttablet and test again

	// kill the newly created tablet
	rTablet.VttabletProcess.TearDown(false)

	// Start Vttablet
	err = clusterInstance.StartVttablet(rTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	assert.Nil(t, err, "error should be Nil")

	// Make this tablet as master
	err = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, rTablet.TabletUID)
	assert.Nil(t, err, "error should be Nil")
	waitForTabletStatus(*rTablet, "SERVING")

	// Make sure that the TER increased i.e. it was set to the current time.
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", rTablet.Alias)

	var streamHealthRes2 querypb.StreamHealthResponse
	err = json.Unmarshal([]byte(result), &streamHealthRes2)
	assert.Nil(t, err, "error should be Nil")

	actualType = streamHealthRes2.GetTarget().GetTabletType()
	tabletType = topodatapb.TabletType_value["MASTER"]
	got = fmt.Sprintf("%d", actualType)
	want = fmt.Sprintf("%d", tabletType)
	assert.Equal(t, want, got)

	assert.NotNil(t, streamHealthRes2.GetTabletExternallyReparentedTimestamp())
	assert.True(t, streamHealthRes2.GetTabletExternallyReparentedTimestamp() > streamHealthRes1.GetTabletExternallyReparentedTimestamp(),
		"When the MASTER vttablet was restarted, the TER timestamp must be set to the current time.")

	// Reset master
	err = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, masterTablet.TabletUID)
	assert.Nil(t, err, "error should be Nil")
	waitForTabletStatus(masterTablet, "SERVING")

	// Tear down custom processes
	killTablets(t, rTablet)
}
