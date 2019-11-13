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
	quit                       bool
	rpcs                       int        // Number of queries successfully executed.
	errors                     int        // Number of failed queries.
	waitForNotification        chan bool  // Channel used to notify the main thread that this thread executed
	notifyLock                 sync.Mutex // notifyLock guards the two fields below.
	notifyAfterNSuccessfulRpcs int        // If 0, notifications are disabled
	rpcsSoFar                  int        // Number of RPCs at the time a notification was requested
	i                          int        //
	commitErrors               int
	executeFunction            func(t *testing.T, conn *mysql.Conn) error
}

func (c *threadParams) threadRun(t *testing.T, conn *mysql.Conn) {
	for !c.quit {
		err := c.executeFunction(t, conn)
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
	c.notifyLock.Lock()
}

func (c *threadParams) stop() {
	c.quit = true
}

func (c *threadParams) readExecute(t *testing.T, conn *mysql.Conn) error {
	qr, error := exec(t, conn, fmt.Sprintf("SELECT * FROM buffer WHERE id = %d", criticalReadRowID))
	t.Log("num of rows returned", len(qr.Rows))
	return error
}

func (c *threadParams) updateExecute(t *testing.T, conn *mysql.Conn) error {
	attempts := c.i
	c.i++
	commitStarted := false
	exec(t, conn, "begin")
	// Do not use a bind variable for "msg" to make sure that the value shows
	// up in the logs.
	qr, err := exec(t, conn, fmt.Sprintf("UPDATE buffer SET msg=\\'update %d\\' WHERE id = %d", attempts, updateRowID))
	// Sleep between [0, 1] seconds to prolong the time the transaction is in
	// flight. This is more realistic because applications are going to keep
	// their transactions open for longer as well.
	time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)

	commitStarted = true
	t.Logf("update %d affected %d rows", attempts, len(qr.Rows))
	exec(t, conn, "commit")
	if err != nil {
		_, errRollback := exec(t, conn, "rollback")
		if errRollback != nil {
			t.Log("Error in rollback", errRollback.Error())
		}
	}
	if !commitStarted {
		t.Logf("UPDATE %d failed before COMMIT. This should not happen.Re-raising exception.", attempts)
		err = errors.New("UPDATE failed before COMMIT")
		return err
	}
	c.commitErrors++
	if c.commitErrors >= 1 {
		return err
	}
	return err
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
	exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", criticalReadRowID, "critical read"))
	exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", updateRowID, "update"))

}
