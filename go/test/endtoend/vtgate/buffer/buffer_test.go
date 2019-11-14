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

package buffer

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	vtParams              mysql.ConnParams
	keyspaceUnshardedName = "ks1"
	cell                  = "zone1"
	hostname              = "localhost"
	sqlSchema             = `create table buffer(
									id BIGINT NOT NULL,
									msg VARCHAR(64) NOT NULL,
									PRIMARY KEY (id)
								) Engine=InnoDB`
	wg = &sync.WaitGroup{}
)

const (
	criticalReadRowID = 1
	updateRowID       = 2
)

//threadParams is set of params passed into read and write threads
type threadParams struct {
	name                       string
	writable                   bool
	quit                       bool
	rpcs                       int        // Number of queries successfully executed.
	errors                     int        // Number of failed queries.
	waitForNotification        chan bool  // Channel used to notify the main thread that this thread executed
	notifyLock                 sync.Mutex // notifyLock guards the two fields below.
	notifyAfterNSuccessfulRpcs int        // If 0, notifications are disabled
	rpcsSoFar                  int        // Number of RPCs at the time a notification was requested
	i                          int        //
	commitErrors               int
	executeFunction            func(c *threadParams, conn *mysql.Conn) error
}

func (c *threadParams) threadRun() {
	c.waitForNotification = make(chan bool)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		log.Error(err.Error())
	}
	defer conn.Close()
	for !c.quit {
		err = c.executeFunction(c, conn)
		if err != nil {
			c.errors++
			log.Error(err.Error())
		}
		c.rpcs++
		// If notifications are requested, check if we already executed the
		// required number of successful RPCs.
		// Use >= instead of == because we can miss the exact point due to
		// slow thread scheduling.
		c.notifyLock.Lock()
		if c.notifyAfterNSuccessfulRpcs != 0 && c.rpcs >= (c.notifyAfterNSuccessfulRpcs+c.rpcsSoFar) {
			c.waitForNotification <- true
			c.notifyAfterNSuccessfulRpcs = 0
		}
		c.notifyLock.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	wg.Done()
}

func (c *threadParams) setNotifyAfterNSuccessfulRpcs(n int) {
	c.notifyLock.Lock()
	c.notifyAfterNSuccessfulRpcs = n
	c.rpcsSoFar = c.rpcs
	c.notifyLock.Unlock()
}

func (c *threadParams) stop() {
	c.quit = true
	println("quit : " + fmt.Sprintf("%t", c.quit))
}

func readExecute(c *threadParams, conn *mysql.Conn) error {
	_, err := conn.ExecuteFetch(fmt.Sprintf("SELECT * FROM buffer WHERE id = %d", criticalReadRowID), 1000, true)
	return err
}

func updateExecute(c *threadParams, conn *mysql.Conn) error {
	attempts := c.i
	c.i++
	commitStarted := false
	conn.ExecuteFetch("begin", 1000, true)
	// Do not use a bind variable for "msg" to make sure that the value shows
	// up in the logs.
	_, err := conn.ExecuteFetch(fmt.Sprintf("UPDATE buffer SET msg='update %d' WHERE id = %d", attempts, updateRowID), 1000, true)
	// Sleep between [0, 1] seconds to prolong the time the transaction is in
	// flight. This is more realistic because applications are going to keep
	// their transactions open for longer as well.
	time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)

	commitStarted = true
	fmt.Printf("update %d affected", attempts)
	conn.ExecuteFetch("commit", 1000, true)
	if err != nil {
		_, errRollback := conn.ExecuteFetch("rollback", 1000, true)
		if errRollback != nil {
			fmt.Print("Error in rollback", errRollback.Error())
		}

		if !commitStarted {
			fmt.Printf("UPDATE %d failed before COMMIT. This should not happen.Re-raising exception.", attempts)
			err = errors.New("UPDATE failed before COMMIT")
			return err
		}
		c.commitErrors++
		if c.commitErrors > 1 {
			return err
		}
		fmt.Printf("UPDATE %d failed during COMMIT. This is okay once because we do not support buffering it. err: %s", attempts, err.Error())
	}
	return err
}

func (c *threadParams) getCommitErrors() int {
	return c.commitErrors
}

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = &cluster.LocalProcessCluster{Cell: cell, Hostname: hostname}
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceUnshardedName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1
		}

		clusterInstance.VtGateExtraArgs = []string{
			"-enable_buffer",
			// Long timeout in case failover is slow.
			"-buffer_window", "10m",
			"-buffer_max_failover_duration", "10m",
			"-buffer_min_time_between_failovers", "20m"}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}

func TestBuffer(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Insert two rows for the later threads (critical read, update).
	exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", criticalReadRowID, "'critical read'"))
	exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", updateRowID, "'update'"))

	//Start both threads.
	readThreadInstance := &threadParams{writable: false, quit: false, rpcs: 0, errors: 0, notifyAfterNSuccessfulRpcs: 0, rpcsSoFar: 0, executeFunction: readExecute}
	wg.Add(1)
	go readThreadInstance.threadRun()
	updateThreadInstance := &threadParams{writable: false, quit: false, rpcs: 0, errors: 0, notifyAfterNSuccessfulRpcs: 0, rpcsSoFar: 0, executeFunction: updateExecute, i: 1, commitErrors: 0}
	wg.Add(1)
	go updateThreadInstance.threadRun()

	readThreadInstance.setNotifyAfterNSuccessfulRpcs(2)
	updateThreadInstance.setNotifyAfterNSuccessfulRpcs(2)

	println("before read thread 2")
	println("before update thread 2")
	<-readThreadInstance.waitForNotification
	println("after read thread 2")
	<-updateThreadInstance.waitForNotification
	println("after update thread 2")

	// Execute the failover.
	readThreadInstance.setNotifyAfterNSuccessfulRpcs(10)
	updateThreadInstance.setNotifyAfterNSuccessfulRpcs(10)

	//reparent call
	clusterInstance.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "-keyspace_shard",
		fmt.Sprintf("%s/%s", keyspaceUnshardedName, "0"),
		"-new_master", clusterInstance.Keyspaces[0].Shards[0].Vttablets[1].Alias)

	println("before read thread 10")
	println("before update thread 10")
	<-readThreadInstance.waitForNotification
	println("after read thread 10")
	<-updateThreadInstance.waitForNotification
	println("after update thread 10")

	readThreadInstance.stop()
	updateThreadInstance.stop()

	assert.Equal(t, 0, readThreadInstance.errors)
	assert.Equal(t, 0, updateThreadInstance.errors)

	println(clusterInstance.VtgateProcess.Port)
	//time.Sleep(40 * time.Second)

	//At least one thread should have been buffered.
	//This may fail if a failover is too fast. Add retries then.
	resp, err := http.Get(clusterInstance.VtgateProcess.VerifyURL)
	if err != nil {
		t.Fatal(err)
	}
	label := fmt.Sprintf("%s.%s", keyspaceUnshardedName, "0")
	inFlightMax := 0
	masterPromotedCount := 0
	durationMs := 0
	bufferingStops := 0
	if resp.StatusCode == 200 {
		resultMap := make(map[string]interface{})
		respByte, _ := ioutil.ReadAll(resp.Body)
		err := json.Unmarshal(respByte, &resultMap)
		if err != nil {
			panic(err)
		}
		inFlightMax = getVarFromVtgate(t, label, "BufferLastRequestsInFlightMax", resultMap)
		masterPromotedCount = getVarFromVtgate(t, label, "HealthcheckMasterPromoted", resultMap)
		durationMs = getVarFromVtgate(t, label, "BufferFailoverDurationSumMs", resultMap)
		bufferingStops = getVarFromVtgate(t, "NewMasterSeen", "BufferStops", resultMap)
	}
	if inFlightMax == 0 {
		// Missed buffering is okay when we observed the failover during the
		// COMMIT (which cannot trigger the buffering).
		assert.Greater(t, updateThreadInstance.commitErrors, 0, "No buffering took place and the update thread saw no error during COMMIT. But one of it must happen.")
	} else {
		assert.Greater(t, updateThreadInstance.commitErrors, 0)
	}

	// There was a failover and the HealthCheck module must have seen it.
	if masterPromotedCount > 0 {
		assert.Greater(t, masterPromotedCount, 0)
	}

	//
	if durationMs > 0 {
		// Buffering was actually started.
		t.Logf("Failover was buffered for %d milliseconds.", durationMs)

		// Number of buffering stops must be equal to the number of seen failovers.
		assert.Equal(t, masterPromotedCount, bufferingStops)
	}
	println("in_flight_max", inFlightMax)
	println("masterPromotedCount", masterPromotedCount)
	println("duration Ms", durationMs)
	println("buffering stops", bufferingStops)
	wg.Wait()

}

func getVarFromVtgate(t *testing.T, label string, param string, resultMap map[string]interface{}) int {
	paramVal := 0
	var err error
	object := reflect.ValueOf(resultMap["\""+param+"\""])
	if object.Kind() == reflect.Map {
		for _, key := range object.MapKeys() {
			if strings.Contains(key.String(), label) {
				v := object.MapIndex(key)
				switch v.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					paramVal = int(v.Int())
				case reflect.String:
					paramVal, err = strconv.Atoi(v.String())
					if err != nil {
						t.Fatal(err.Error())
					}
				}
			}
		}
	}
	return paramVal
}
