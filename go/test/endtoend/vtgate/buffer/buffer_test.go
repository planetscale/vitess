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
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
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
}

func (c *threadParams) setNotifyAfterNSuccessfulRpcs(n int) {
	c.notifyLock.Lock()
	c.notifyAfterNSuccessfulRpcs = n
	c.rpcsSoFar = c.rpcs
	c.notifyLock.Unlock()
}

func (c *threadParams) stop() {
	c.quit = true
}

func readExecute(c *threadParams, conn *mysql.Conn) error {
	qr, err := conn.ExecuteFetch(fmt.Sprintf("SELECT * FROM buffer WHERE id = %d", criticalReadRowID), 1000, true)
	fmt.Println(qr)
	return err
}

func updateExecute(c *threadParams, conn *mysql.Conn) error {
	attempts := c.i
	c.i++
	commitStarted := false
	fmt.Printf("\nRPCS : %d,RPCS_SO_FAR : %d,NOTIFY : %d", c.rpcs, c.rpcsSoFar, c.notifyAfterNSuccessfulRpcs)
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

func exec(t *testing.T, conn *mysql.Conn, query string) (*sqltypes.Result, error) {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	return qr, err
}

func TestBuffer(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Insert two rows for the later threads (critical read, update).
	_, err1 := exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", criticalReadRowID, "'critical read'"))
	if err1 != nil {
		t.Fatal("ERROR IN QUERY")
	}
	_, err2 := exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", updateRowID, "'update'"))
	if err2 != nil {
		t.Fatal("ERROR IN QUERY")
	}

	//Start both threads.
	var wg = &sync.WaitGroup{}
	readThreadInstance := &threadParams{writable: false, quit: false, rpcs: 0, errors: 0, notifyAfterNSuccessfulRpcs: 0, rpcsSoFar: 0, executeFunction: readExecute}
	wg.Add(1)
	go readThreadInstance.threadRun()
	fmt.Println("IN MAIN FUNCTION AFTER READ THREAD")
	updateThreadInstance := &threadParams{writable: false, quit: false, rpcs: 0, errors: 0, notifyAfterNSuccessfulRpcs: 0, rpcsSoFar: 0, executeFunction: updateExecute, i: 1, commitErrors: 0}
	wg.Add(1)
	go updateThreadInstance.threadRun()
	fmt.Println("IN MAIN FUNCTION AFTER UPDATE THREAD")

	readThreadInstance.setNotifyAfterNSuccessfulRpcs(2)
	updateThreadInstance.setNotifyAfterNSuccessfulRpcs(2)

	<-readThreadInstance.waitForNotification
	<-updateThreadInstance.waitForNotification

	// Execute the failover.
	readThreadInstance.setNotifyAfterNSuccessfulRpcs(10)
	readThreadInstance.setNotifyAfterNSuccessfulRpcs(10)

	//reparent call
	clusterInstance.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "-keyspace_shard",
		fmt.Sprintf("%s/%s", keyspaceUnshardedName, "0"),
		"-new_master", clusterInstance.Keyspaces[0].Shards[0].Vttablets[1].Alias)

	<-readThreadInstance.waitForNotification
	<-updateThreadInstance.waitForNotification

	readThreadInstance.stop()
	updateThreadInstance.stop()

	wg.Wait()

	assert.Equal(t, 0, readThreadInstance.errors)
	assert.Equal(t, 0, updateThreadInstance.errors)

}
