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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TabletCommands tests the basic tablet commands
func TestTabletCommands(t *testing.T) {
	ctx := context.Background()

	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	if err != nil {
		t.Fatal(err)
	}
	defer masterConn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	if err != nil {
		t.Fatal(err)
	}
	defer replicaConn.Close()

	// Sanity Check
	exec(t, masterConn, "delete from t1")
	exec(t, masterConn, "insert into t1(id, value) values(1,'a'), (2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	// test exclude_field_names to vttablet works as expected
	sql := "select id, value from t1"
	args := []string{
		"VtTabletExecute",
		"-options", "included_fields:TYPE_ONLY",
		"-json",
		masterTabletAlias,
		sql,
	}
	qr, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
	assertExcludeFields(t, qr)

	// make sure direct dba queries work
	sql = "select * from t1"
	qr, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDba", "-json", masterTabletAlias, sql)
	assertExecuteFetch(t, qr)

	// check Ping / RefreshState / RefreshStateByShard
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Ping", masterTabletAlias)
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshState", masterTabletAlias)
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshStateByShard", keyspaceShard)
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshStateByShard", "--cells="+cell, keyspaceShard)
	assert.Nil(t, err, "error should be Nil")

	// Check basic actions.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetReadOnly", masterTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	result := exec(t, masterConn, "show variables like 'read_only'")
	got := fmt.Sprintf("%v", result.Rows)
	want := "[[VARCHAR(\"read_only\") VARCHAR(\"ON\")]]"
	assert.Equal(t, want, got)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetReadWrite", masterTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	result = exec(t, masterConn, "show variables like 'read_only'")
	got = fmt.Sprintf("%v", result.Rows)
	want = "[[VARCHAR(\"read_only\") VARCHAR(\"OFF\")]]"
	assert.Equal(t, want, got)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate", "-ping-tablets=true")
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateKeyspace", keyspaceName)
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateKeyspace", "-ping-tablets=true", keyspaceName)
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateShard", "-ping-tablets=false", keyspaceShard)
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateShard", "-ping-tablets=true", keyspaceShard)
	assert.Nil(t, err, "error should be Nil")

}

func assertExcludeFields(t *testing.T, qr string) {
	resultMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(qr), &resultMap)
	if err != nil {
		t.Fatal(err)
	}

	rowsAffected := resultMap["rows_affected"]
	want := float64(2)
	assert.Equal(t, want, rowsAffected)

	fields := resultMap["fields"]
	assert.NotContainsf(t, fields, "name", "name should not be in field list")
}

func assertExecuteFetch(t *testing.T, qr string) {
	resultMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(qr), &resultMap)
	if err != nil {
		t.Fatal(err)
	}

	rows := reflect.ValueOf(resultMap["rows"])
	got := rows.Len()
	want := int(2)
	assert.Equal(t, want, got)

	fields := reflect.ValueOf(resultMap["fields"])
	got = fields.Len()
	want = int(2)
	assert.Equal(t, want, got)
}

// TabletReshuffle test if a vttablet can be pointed at an existing mysql
func TestTabletReshuffle(t *testing.T) {
	ctx := context.Background()

	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	if err != nil {
		t.Fatal(err)
	}
	defer masterConn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	if err != nil {
		t.Fatal(err)
	}
	defer replicaConn.Close()

	// Sanity Check
	exec(t, masterConn, "delete from t1")
	exec(t, masterConn, "insert into t1(id, value) values(1,'a'), (2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	// Kill Tablet
	// mysqlProc := replicaTablet.MysqlctlProcess
	replicaTablet.VttabletProcess.Kill()

	// mycnf_server_id prevents vttablet from reading the mycnf
	replicaTablet.VttabletProcess.ExtraArgs = []string{
		"-lock_tables_timeout", "5s",
		"-mycnf_server_id", fmt.Sprintf("%d", replicaTablet.TabletUID),
		"-db_socket", fmt.Sprintf("%s/mysql.sock", masterTablet.VttabletProcess.Directory),
	}
	// SupportBackup=False prevents vttablet from trying to restore
	replicaTablet.VttabletProcess.SupportBackup = false
	replicaTablet.VttabletProcess.ServingStatus = "SERVING"
	// Start Tablet
	if err = replicaTablet.VttabletProcess.Setup(); err != nil {
		t.Fatal(err)
	}

	replicaConn2, err := mysql.Connect(ctx, &replicaTabletParams)
	if err != nil {
		t.Fatal(err)
	}
	defer replicaConn2.Close()
	checkDataOnReplica(t, replicaConn2, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	replicaTabletAlias := fmt.Sprintf("%s-%d", cell, replicaTablet.TabletUID)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Backup", replicaTabletAlias)
	assert.Error(t, err, "cannot perform backup without my.cnf")

	//mysqlProc.Stop()
	replicaTablet.VttabletProcess.Kill()
}

// ActionAndTimeout test
func TestActionAndTimeout(t *testing.T) {

	err := clusterInstance.VtctlclientProcess.ExecuteCommand("Sleep", masterTabletAlias, "5s")
	time.Sleep(1 * time.Second)

	// try a frontend RefreshState that should timeout as the tablet is busy running the other one
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshState", masterTabletAlias, "-wait-time", "2s")
	assert.Error(t, err, "timeout as tablet is in Sleep")
}

func TestHook(t *testing.T) {
	// test a regular program works
	runHookAndAssert(t, []string{
		"ExecuteHook", masterTabletAlias, "test.sh", "--flag1", "--param1=hello"}, "0", false, "")

	// test stderr output
	runHookAndAssert(t, []string{
		"ExecuteHook", masterTabletAlias, "test.sh", "--to-stderr"}, "0", false, "ERR: --to-stderr\n")

	// test commands that fail
	runHookAndAssert(t, []string{
		"ExecuteHook", masterTabletAlias, "test.sh", "--exit-error"}, "1", false, "ERROR: exit status 1\n")

	// test hook that is not present
	runHookAndAssert(t, []string{
		"ExecuteHook", masterTabletAlias, "not_here.sh", "--exit-error"}, "-1", false, "missing hook")

	// test hook with invalid name

	runHookAndAssert(t, []string{
		"ExecuteHook", masterTabletAlias, "/bin/ls"}, "-1", true, "hook name cannot have")
}

func runHookAndAssert(t *testing.T, params []string, expectedStatus string, expectedError bool, expectedStderr string) {

	hr, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(params...)
	if expectedError {
		assert.Error(t, err, "Expected error")
	} else {
		if err != nil {
			t.Fatal(err)
		}

		resultMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(hr), &resultMap)
		if err != nil {
			t.Fatal(err)
		}

		exitStatus := reflect.ValueOf(resultMap["ExitStatus"]).Float()
		status := fmt.Sprintf("%.0f", exitStatus)
		assert.Equal(t, expectedStatus, status)

		stderr := reflect.ValueOf(resultMap["Stderr"]).String()
		assert.Contains(t, stderr, expectedStderr)
	}

}

func TestShardReplicationFix(t *testing.T) {
	// make sure the replica is in the replication graph, 2 nodes: 1 master, 1 replica
	qr, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell, keyspaceShard)
	assert.Nil(t, err, "error should be Nil")
	assertNodeCount(t, qr, int(3))

	// Manually add a bogus entry to the replication graph, and check it is removed by ShardReplicationFix
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ShardReplicationAdd", keyspaceShard, fmt.Sprintf("%s-9000", cell))
	assert.Nil(t, err, "error should be Nil")

	qr, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell, keyspaceShard)
	assert.Nil(t, err, "error should be Nil")
	assertNodeCount(t, qr, int(4))

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ShardReplicationFix", cell, keyspaceShard)
	assert.Nil(t, err, "error should be Nil")
	qr, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell, keyspaceShard)
	assert.Nil(t, err, "error should be Nil")
	assertNodeCount(t, qr, int(3))
}

func assertNodeCount(t *testing.T, qr string, want int) {
	resultMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(qr), &resultMap)
	if err != nil {
		t.Fatal(err)
	}

	nodes := reflect.ValueOf(resultMap["nodes"])
	got := nodes.Len()
	assert.Equal(t, want, got)
}

func TestHealthCheck(t *testing.T) {
	ctx := context.Background()

	if replicaTablet.VttabletProcess.GetTabletStatus() == "" {
		fmt.Println("*** creating new replica")

		replicaTablet.VttabletProcess.ExtraArgs = []string{
			"-db_socket", fmt.Sprintf("%s/mysql.sock", replicaTablet.VttabletProcess.Directory),
		}
		// SupportBackup=False prevents vttablet from trying to restore
		replicaTablet.VttabletProcess.SupportBackup = false
		replicaTablet.VttabletProcess.ServingStatus = "SERVING"
		// Start Tablet
		if err := replicaTablet.VttabletProcess.Setup(); err != nil {
			t.Fatal(err)
		}
	} else {
		fmt.Println("*** replica is present")
	}

	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	if err != nil {
		t.Fatal(err)
	}
	defer masterConn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	if err != nil {
		t.Fatal(err)
	}
	defer replicaConn.Close()

	// Sanity Check
	exec(t, masterConn, "delete from t1")
	exec(t, masterConn, "insert into t1(id, value) values(1,'a'), (2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	replicaTabletAlias := fmt.Sprintf("%s-%d", cell, replicaTablet.TabletUID)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", replicaTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	checkHealth(t, replicaTablet.HTTPPort)

	// Make sure the master is still master
	qr, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", masterTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	checkTabletType(t, qr, "MASTER")
	exec(t, masterConn, "stop slave")

	// stop replication, make sure we don't go unhealthy.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopSlave", replicaTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", replicaTabletAlias)
	assert.Nil(t, err, "error should be Nil")

	// make sure the health stream is updated
	qr, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", replicaTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	assert.Containsf(t, qr, "serving", "Tablet should be in serving state")
	assert.NotContainsf(t, qr, "seconds_behind_master", "Tablet should not be behind master")

	// then restart replication, make sure we stay healthy
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopSlave", replicaTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", replicaTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	checkHealth(t, replicaTablet.HTTPPort)

	// now test VtTabletStreamHealth returns the right thing
	qr, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "2", replicaTabletAlias)
	scanner := bufio.NewScanner(strings.NewReader(qr))
	for scanner.Scan() {
		fmt.Println(scanner.Text()) // Println will add back the final '\n'
		assert.Containsf(t, qr, "realtime_stats", "Tablet should have realtime_stats")
		assert.Containsf(t, qr, "serving", "Tablet should be in serving state")
		assert.Containsf(t, qr, fmt.Sprintf("%d", replicaTablet.TabletUID), "Tablet should contain uid")
		assert.True(t, !strings.Contains(qr, "seconds_behind_master") ||
			strings.Contains(qr, "\"seconds_behind_master\":4"),
			"seconds_behind_master", "Tablet should not be behind master")
	}

	//TODO: Ajeet, fix below test
	// Test that VtTabletStreamHealth reports a QPS >0.0.
	n := 0
	for n < 10 {
		n++
		exec(t, replicaConn, "select 1 from dual")
		exec(t, replicaConn, "select * from t1")
	}
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		qr, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", replicaTabletAlias)
		fmt.Println(qr)
		time.Sleep(100 * time.Millisecond)
		//if qps>0.0
		//break
	}

	// Manual cleanup of processes
	mysqlProc := replicaTablet.MysqlctlProcess
	mysqlProc.Stop()
	replicaTablet.VttabletProcess.TearDown()
}

func checkHealth(t *testing.T, port int) {
	url := fmt.Sprintf("http://localhost:%d/healthz", port)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 200, resp.StatusCode)
}

func checkTabletType(t *testing.T, qr string, typeWant string) {
	resultMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(qr), &resultMap)
	if err != nil {
		t.Fatal(err)
	}
	actualType := reflect.ValueOf(resultMap["type"]).Float()
	got := fmt.Sprintf("%.0f", actualType)

	tabletType := topodatapb.TabletType_value[typeWant]
	want := fmt.Sprintf("%d", tabletType)

	assert.Equal(t, want, got)
}

func TestHealthCheckDrainedStateDoesNotShutdownQueryService(t *testing.T) {
	// This test is similar to test_health_check, but has the following differences:
	// - the second tablet is an 'rdonly' and not a 'replica'
	// - the second tablet will be set to 'drained' and we expect that
	// - the query service won't be shutdown

	//Wait if tablet is not in service state
	waitForTabletStatus(rdonlyTablet, "SERVING")

	// Check tablet health
	checkHealth(t, rdonlyTablet.HTTPPort)
	assert.Equal(t, "SERVING", rdonlyTablet.VttabletProcess.GetTabletStatus())

	// Change from rdonly to drained and stop replication. (These
	// actions are similar to the SplitClone vtworker command
	// implementation.)  The tablet will stay healthy, and the
	// query service is still running.
	rdonlyTabletAlias := fmt.Sprintf("%s-%d", cell, rdonlyTablet.TabletUID)
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", rdonlyTabletAlias, "drained")
	assert.Nil(t, err, "error should be Nil")
	// Trying to drain the same tablet again, should error
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", rdonlyTabletAlias, "drained")
	assert.Error(t, err, "already drained")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopSlave", rdonlyTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	// Trigger healthcheck explicitly to avoid waiting for the next interval.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rdonlyTabletAlias)
	assert.Nil(t, err, "error should be Nil")

	qr, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", rdonlyTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	checkTabletType(t, qr, "DRAINED")

	// Query service is still running.
	waitForTabletStatus(rdonlyTablet, "SERVING")

	// Restart replication. Tablet will become healthy again.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", rdonlyTabletAlias, "rdonly")
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartSlave", rdonlyTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rdonlyTabletAlias)
	assert.Nil(t, err, "error should be Nil")
	checkHealth(t, rdonlyTablet.HTTPPort)
}

func waitForTabletStatus(tablet cluster.Vttablet, status string) {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		if tablet.VttabletProcess.WaitForStatus(status) {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
}
