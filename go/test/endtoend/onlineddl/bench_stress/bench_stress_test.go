/*
Copyright 2024 The Vitess Authors.

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

// This test is designed to test the flow of a single online DDL migration, with tablet throttler
// enabled. IT tests the following:
// - A primary + replica setup
// - Creating and populating a table
// - Enabling tablet (lag) throttler
// - Running a workload that generates DMLs, and which checks the throttler
// - Running an online DDL migration:
//   - Using `online --postpone-completion` to use vreplication
//   - vreplication configured (by default) to read from replica
//   - vreplication by nature also checks the throttler
//   - meanwhile, the workload generates DMLs, give migration some run time
//   - proactively throttle and then unthrottle the migration
//   - complete the migration
//
// - Validate sufficient DML has been applied
// - Validate the migration completed, and validate new schema is instated
//
// The test is designed with upgrade/downgrade in mind. In particular, we wish to test
// different vitess versions for `primary` and `replica` tablets. Thus, we validate:
// - Cross tablet and cross version throttler communication
// - Cross version vreplication

package flow

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/test/endtoend/throttler"
	"vitess.io/vitess/go/vt/log"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/vttablet"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

type WriteMetrics struct {
	mu                                                      sync.Mutex
	opCounter                                               atomic.Int64
	insertsAttempts, insertsFailures, insertsNoops, inserts int64
	updatesAttempts, updatesFailures, updatesNoops, updates int64
	deletesAttempts, deletesFailures, deletesNoops, deletes int64
}

func (w *WriteMetrics) Clear() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.inserts = 0
	w.updates = 0
	w.deletes = 0

	w.insertsAttempts = 0
	w.insertsFailures = 0
	w.insertsNoops = 0

	w.updatesAttempts = 0
	w.updatesFailures = 0
	w.updatesNoops = 0

	w.deletesAttempts = 0
	w.deletesFailures = 0
	w.deletesNoops = 0
}

func (w *WriteMetrics) String() string {
	return fmt.Sprintf(`WriteMetrics: inserts-deletes=%d, updates-deletes=%d,
insertsAttempts=%d, insertsFailures=%d, insertsNoops=%d, inserts=%d,
updatesAttempts=%d, updatesFailures=%d, updatesNoops=%d, updates=%d,
deletesAttempts=%d, deletesFailures=%d, deletesNoops=%d, deletes=%d,
`,
		w.inserts-w.deletes, w.updates-w.deletes,
		w.insertsAttempts, w.insertsFailures, w.insertsNoops, w.inserts,
		w.updatesAttempts, w.updatesFailures, w.updatesNoops, w.updates,
		w.deletesAttempts, w.deletesFailures, w.deletesNoops, w.deletes,
	)
}

var (
	clusterInstance  *cluster.LocalProcessCluster
	shards           []cluster.Shard
	vtParams         mysql.ConnParams
	primaryTablet    *cluster.Vttablet
	replicaTablet    *cluster.Vttablet
	tablets          []*cluster.Vttablet
	throttleWorkload atomic.Bool
	totalAppliedDML  atomic.Int64

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	tableName             = `stress_test`
	createStatements      = `
	  DROP TABLE IF EXISTS stress_test;
		CREATE TABLE stress_test (
			id bigint not null,
			rand_val varchar(32) null default '',
			hint_col varchar(64) not null default '',
			op_counter bigint not null,
			created_timestamp timestamp not null default current_timestamp,
			updates int unsigned not null default 0,
			PRIMARY KEY (id),
			key created_idx(created_timestamp),
			key updates_idx(updates)
		) ENGINE=InnoDB
	`
	alterHintStatement = `
		ALTER TABLE stress_test modify hint_col varchar(64) not null default '%s'
	`
	insertRowStatement = `
		INSERT IGNORE INTO stress_test (id, rand_val, op_counter) VALUES (%d, left(md5(rand()), 8), %d)
	`
	updateRowStatement = `
		UPDATE stress_test SET updates=updates+1, op_counter=%d WHERE id=%d
	`
	deleteRowStatement = `
		DELETE FROM stress_test WHERE id=%d AND updates=1
	`
	selectCountRowsStatement = `
		SELECT
			COUNT(*) AS num_rows,
			CAST(SUM(updates) AS SIGNED) AS sum_updates,
			MAX(op_counter) AS op_counter,
			COUNT(DISTINCT op_counter) AS distinct_op_counter
		FROM stress_test
	`
	writeMetrics WriteMetrics
)

const (
	baseSleepInterval    = 5 * time.Millisecond
	maxTableRows         = 4096
	migrationWaitTimeout = 90 * time.Second
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		schemaChangeDirectory = path.Join("/tmp", fmt.Sprintf("schema_change_dir_%d", clusterInstance.GetAndReserveTabletUID()))
		defer os.RemoveAll(schemaChangeDirectory)
		defer clusterInstance.Teardown()

		if _, err := os.Stat(schemaChangeDirectory); os.IsNotExist(err) {
			_ = os.Mkdir(schemaChangeDirectory, 0700)
		}

		clusterInstance.VtctldExtraArgs = []string{
			"--schema_change_dir", schemaChangeDirectory,
			"--schema_change_controller", "local",
			"--schema_change_check_interval", "1s",
		}

		clusterInstance.VtTabletExtraArgs = []string{
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", "15s",
			"--migration_check_interval", "2s",
			"--watch_replication_stream",
			// Test VPlayer batching mode.
			fmt.Sprintf("--vreplication_experimental_flags=%d",
				vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage|vttablet.VReplicationExperimentalFlagOptimizeInserts|vttablet.VReplicationExperimentalFlagVPlayerBatching),
		}
		clusterInstance.VtGateExtraArgs = []string{
			"--ddl_strategy", "online",
		}

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}

		// No need for replicas in this stress test
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"1"}, 1, false); err != nil {
			return 1, err
		}

		// Collect table paths and ports
		tablets = clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "primary" {
				primaryTablet = tablet
			} else {
				replicaTablet = tablet
			}
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		// Start vtgate
		if err := vtgateInstance.Setup(); err != nil {
			return 1, err
		}
		// ensure it is torn down during cluster TearDown
		clusterInstance.VtgateProcess = *vtgateInstance
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
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

func runRoutineThrottleCheck(t testing.TB, ctx context.Context, flags *throttle.CheckFlags) {
	throttleWorkload.Store(true) // ensure throttler is checked and is happy before allowing writes.
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			resp, err := throttler.CheckThrottler(clusterInstance, primaryTablet, throttlerapp.TestingName, flags)
			assert.NoError(t, err)
			switch {
			case err != nil:
			case resp == nil:
			case resp.Check == nil:
			default:
				shouldThrottle := resp.Check.ResponseCode != tabletmanagerdatapb.CheckThrottlerResponseCode_OK
				throttleWorkload.Store(shouldThrottle)
				if shouldThrottle {
					go func() {
						flags := &throttle.CheckFlags{SkipRequestHeartbeats: true, Scope: base.SelfScope}
						resp, _ := throttler.CheckThrottler(clusterInstance, primaryTablet, throttlerapp.TestingName, flags)
						log.Infof("Self throttle check: %v", resp)
					}()
				}
				go log.Infof("Throttle check: %v", resp.Check)
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()
}

func waitForThrottleCheckOK(t testing.TB, ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		if !throttleWorkload.Load() {
			return
		}
		select {
		case <-ctx.Done():
			assert.FailNow(t, "throttleWorkload still true after timeout")
			return
		case <-ticker.C:
		}
	}
}

func generateWorkload(t testing.TB, ctx context.Context, wg *sync.WaitGroup) {
	// Create work for vplayer.
	// This workload will consider throttling state and avoid generating DMLs if throttled.
	wg.Add(1)
	go func() {
		defer t.Logf("Terminating workload")
		defer wg.Done()
		runMultipleConnections(ctx, t)
	}()

}

func TestSchemaChange(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	require.NotNil(t, clusterInstance)
	require.NotNil(t, primaryTablet)
	require.NotNil(t, replicaTablet)
	require.Equal(t, 2, len(tablets))

	require.NotEmpty(t, clusterInstance.Keyspaces)
	shards = clusterInstance.Keyspaces[0].Shards
	require.Equal(t, 1, len(shards))

	throttler.EnableLagThrottlerAndWaitForStatus(t, clusterInstance)

	t.Run("flow", func(t *testing.T) {
		t.Run("create schema", func(t *testing.T) {
			testWithInitialSchema(t)
		})
		t.Run("init table", func(t *testing.T) {
			// Populates table. Makes work for vcopier.
			initTable(t)
		})
		t.Run("migrate", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			workloadCtx, cancelWorkload := context.WithCancel(ctx)
			defer cancelWorkload()

			t.Run("routine throttler check", func(t *testing.T) {
				flags := &throttle.CheckFlags{SkipRequestHeartbeats: true}
				runRoutineThrottleCheck(t, workloadCtx, flags)
			})

			var wg sync.WaitGroup
			t.Run("generate workload", func(t *testing.T) {
				generateWorkload(t, workloadCtx, &wg)
			})

			hint := "post_completion_hint"
			var uuid string
			t.Run("submit migration", func(t *testing.T) {
				uuid = testOnlineDDLStatement(t, fmt.Sprintf(alterHintStatement, hint), "online --force-cut-over-after=1s", "", true)
			})
			t.Run("wait for migration completion", func(t *testing.T) {
				_ = onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
			})
			t.Run("validate table schema", func(t *testing.T) {
				checkMigratedTable(t, tableName, hint)
			})

			cancelWorkload() // Early break
			cancel()         // Early break
			wg.Wait()

			t.Run("validate table metrics", func(t *testing.T) {
				testSelectTableMetrics(t)
			})
		})
	})
}

func testWithInitialSchema(t testing.TB) {
	// Create the stress table
	err := clusterInstance.VtctldClientProcess.ApplySchema(keyspaceName, createStatements)
	require.Nil(t, err)

	// Check if table is created
	checkTable(t, tableName)
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t testing.TB, alterStatement string, ddlStrategy string, expectHint string, skipWait bool) (uuid string) {
	row := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, alterStatement, "").Named().Row()
	require.NotNil(t, row)
	uuid = row.AsString("uuid", "")
	uuid = strings.TrimSpace(uuid)
	require.NotEmpty(t, uuid)
	t.Logf("# Generated UUID (for debug purposes):")
	t.Logf("<%s>", uuid)

	strategySetting, err := schema.ParseDDLStrategy(ddlStrategy)
	assert.NoError(t, err)

	if !strategySetting.Strategy.IsDirect() && !skipWait && uuid != "" {
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		t.Logf("# Migration status (for debug purposes): <%s>", status)
	}

	if expectHint != "" {
		checkMigratedTable(t, tableName, expectHint)
	}
	return uuid
}

// checkTable checks the number of tables in the first two shards.
func checkTable(t testing.TB, showTableName string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], showTableName, 1)
	}
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t testing.TB, tablet *cluster.Vttablet, showTableName string, expectCount int) {
	query := fmt.Sprintf(`show tables like '%%%s%%';`, showTableName)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	rowcount := 0

	for {
		queryResult, err := tablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
		require.Nil(t, err)
		rowcount = len(queryResult.Rows)
		if rowcount > 0 {
			break
		}

		select {
		case <-ticker.C:
			continue // Keep looping
		case <-ctx.Done():
			// Break below to the assertion
		}

		break
	}

	assert.Equal(t, expectCount, rowcount)
}

// checkMigratedTables checks the CREATE STATEMENT of a table after migration
func checkMigratedTable(t testing.TB, tableName, expectHint string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		createStatement := getCreateTableStatement(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], tableName)
		assert.Contains(t, createStatement, expectHint)
	}
}

// getCreateTableStatement returns the CREATE TABLE statement for a given table
func getCreateTableStatement(t testing.TB, tablet *cluster.Vttablet, tableName string) (statement string) {
	queryResult, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("show create table %s;", tableName), keyspaceName, true)
	require.Nil(t, err)

	assert.Equal(t, len(queryResult.Rows), 1)
	assert.Equal(t, len(queryResult.Rows[0]), 2) // table name, create statement
	statement = queryResult.Rows[0][1].ToString()
	return statement
}

func reviewError(err error) error {
	if err == nil {
		return err
	}
	sqlErr, ok := err.(*sqlerror.SQLError)
	if !ok {
		return err
	}

	// Let's try and account for all known errors:
	switch sqlErr.Number() {
	case sqlerror.ERDupEntry: // happens since we hammer the tables randomly
		return nil
	case sqlerror.ERTooManyUserConnections: // can happen in Online DDL cut-over
		return nil
	case sqlerror.ERUnknownError: // happens when query buffering times out
		return nil
	case sqlerror.ERQueryInterrupted: // cancelled due to context expiration
		return nil
	case sqlerror.ERLockDeadlock:
		return nil // bummer, but deadlocks can happen, it's a legit error.
	case sqlerror.ERLockNowait:
		return nil // For some queries we use NOWAIT. Bummer, but this can happen, it's a legit error.
	case sqlerror.ERQueryTimeout:
		return nil // query timed out, not a FK error
	}
	return err
}

func generateInsert(t testing.TB, conn *mysql.Conn) error {
	opCounter := writeMetrics.opCounter.Add(1)
	id := rand.Int32N(int32(maxTableRows))
	query := fmt.Sprintf(insertRowStatement, id, opCounter)
	qr, err := conn.ExecuteFetch(query, 1, false)
	if err == nil {
		totalAppliedDML.Add(1)
	}

	func() {
		writeMetrics.mu.Lock()
		defer writeMetrics.mu.Unlock()

		writeMetrics.insertsAttempts++
		if err != nil {
			writeMetrics.insertsFailures++
			return
		}
		assert.Less(t, qr.RowsAffected, uint64(2))
		if qr.RowsAffected == 0 {
			writeMetrics.insertsNoops++
			return
		}
		writeMetrics.inserts++
	}()
	return reviewError(err)
}

func generateUpdate(t testing.TB, conn *mysql.Conn) error {
	opCounter := writeMetrics.opCounter.Add(1)
	id := rand.Int32N(int32(maxTableRows))
	query := fmt.Sprintf(updateRowStatement, opCounter, id)
	qr, err := conn.ExecuteFetch(query, 1, false)
	if err == nil {
		totalAppliedDML.Add(1)
	}

	func() {
		writeMetrics.mu.Lock()
		defer writeMetrics.mu.Unlock()

		writeMetrics.updatesAttempts++
		if err != nil {
			writeMetrics.updatesFailures++
			return
		}
		assert.Less(t, qr.RowsAffected, uint64(2))
		if qr.RowsAffected == 0 {
			writeMetrics.updatesNoops++
			return
		}
		writeMetrics.updates++
	}()
	return reviewError(err)
}

func generateDelete(t testing.TB, conn *mysql.Conn) error {
	writeMetrics.opCounter.Add(1)
	id := rand.Int32N(int32(maxTableRows))
	query := fmt.Sprintf(deleteRowStatement, id)
	qr, err := conn.ExecuteFetch(query, 1, false)
	if err == nil {
		totalAppliedDML.Add(1)
	}

	func() {
		writeMetrics.mu.Lock()
		defer writeMetrics.mu.Unlock()

		writeMetrics.deletesAttempts++
		if err != nil {
			writeMetrics.deletesFailures++
			return
		}
		assert.Less(t, qr.RowsAffected, uint64(2))
		if qr.RowsAffected == 0 {
			writeMetrics.deletesNoops++
			return
		}
		writeMetrics.deletes++
	}()
	return reviewError(err)
}

func runSingleConnection(ctx context.Context, t testing.TB, sleepInterval time.Duration) {
	log.Infof("Running single connection")
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("set autocommit=1", 1000, true)
	require.Nil(t, err)
	_, err = conn.ExecuteFetch("set transaction isolation level read committed", 1000, true)
	require.Nil(t, err)

	ticker := time.NewTicker(sleepInterval)
	defer ticker.Stop()

	for {
		if !throttleWorkload.Load() {
			switch rand.Int32N(3) {
			case 0:
				err = generateInsert(t, conn)
			case 1:
				err = generateUpdate(t, conn)
			case 2:
				err = generateDelete(t, conn)
			}
		}
		select {
		case <-ctx.Done():
			log.Infof("Terminating single connection")
			return
		case <-ticker.C:
		}
		assert.Nil(t, err)
	}
}

func runMultipleConnections(ctx context.Context, t testing.TB) {
	// The workload for a 16 vCPU machine is:
	// - Concurrency of 16
	// - 2ms interval between queries for each connection
	// As the number of vCPUs decreases, so do we decrease concurrency, and increase intervals. For example, on a 8 vCPU machine
	// we run concurrency of 8 and interval of 4ms. On a 4 vCPU machine we run concurrency of 4 and interval of 8ms.
	maxConcurrency := runtime.NumCPU()
	sleepModifier := 16.0 / float64(maxConcurrency)
	singleConnectionSleepIntervalNanoseconds := float64(baseSleepInterval.Nanoseconds()) * sleepModifier
	sleepInterval := time.Duration(int64(singleConnectionSleepIntervalNanoseconds))

	log.Infof("Running multiple connections: maxConcurrency=%v, sleep interval=%v", maxConcurrency, sleepInterval)
	var wg sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runSingleConnection(ctx, t, sleepInterval)
		}()
	}
	wg.Wait()
	log.Infof("Running multiple connections: done")
}

func initTable(t testing.TB) {
	log.Infof("initTable begin")
	defer log.Infof("initTable complete")

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	writeMetrics.Clear()
	appliedDMLStart := totalAppliedDML.Load()

	for i := 0; i < maxTableRows/2; i++ {
		generateInsert(t, conn)
	}
	for i := 0; i < maxTableRows/4; i++ {
		generateUpdate(t, conn)
	}
	for i := 0; i < maxTableRows/4; i++ {
		generateDelete(t, conn)
	}
	appliedDMLEnd := totalAppliedDML.Load()
	assert.Greater(t, appliedDMLEnd, appliedDMLStart)
	assert.GreaterOrEqual(t, appliedDMLEnd-appliedDMLStart, int64(maxTableRows))
}

func testSelectTableMetrics(t testing.TB) {
	writeMetrics.mu.Lock()
	defer writeMetrics.mu.Unlock()

	log.Infof("%s", writeMetrics.String())

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	rs, err := conn.ExecuteFetch(selectCountRowsStatement, 1000, true)
	require.Nil(t, err)

	row := rs.Named().Row()
	require.NotNil(t, row)
	log.Infof("testSelectTableMetrics, row: %v", row)
	numRows := row.AsInt64("num_rows", 0)
	sumUpdates := row.AsInt64("sum_updates", 0)
	opCounter := row.AsInt64("op_counter", 0)
	distinctOpCounter := row.AsInt64("distinct_op_counter", 0)
	assert.NotZero(t, numRows)
	assert.NotZero(t, sumUpdates)
	assert.NotZero(t, writeMetrics.inserts)
	assert.NotZero(t, writeMetrics.deletes)
	assert.NotZero(t, writeMetrics.updates)
	assert.Equal(t, writeMetrics.inserts-writeMetrics.deletes, numRows)
	assert.Equal(t, writeMetrics.updates-writeMetrics.deletes, sumUpdates) // because we DELETE WHERE updates=1
	assert.Equal(t, numRows, distinctOpCounter)
	assert.LessOrEqual(t, opCounter, writeMetrics.opCounter.Load())
	log.Infof("numRows=%d, sumUpdates=%d, opCounter=%d, distinctOpCounter=%d", numRows, sumUpdates, opCounter, distinctOpCounter)
}

func BenchmarkWorkloadSingleConn(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testWithInitialSchema(b)
	initTable(b)
	throttler.EnableLagThrottlerAndWaitForStatus(b, clusterInstance)
	flags := &throttle.CheckFlags{SkipRequestHeartbeats: false}
	runRoutineThrottleCheck(b, ctx, flags)
	waitForThrottleCheckOK(b, ctx)

	ticker := time.NewTicker(baseSleepInterval)
	defer ticker.Stop()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(b, err)
	defer conn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for throttleWorkload.Load() {
			<-ticker.C
		}
		for range len(ticker.C) {
			<-ticker.C
		}
		switch rand.Int32N(3) {
		case 0:
			err = generateInsert(b, conn)
		case 1:
			err = generateUpdate(b, conn)
		case 2:
			err = generateDelete(b, conn)
		}
		assert.Nil(b, err)
	}
}

func BenchmarkWorkloadMultiConn(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testWithInitialSchema(b)
	initTable(b)
	throttler.EnableLagThrottlerAndWaitForStatus(b, clusterInstance)
	flags := &throttle.CheckFlags{SkipRequestHeartbeats: false}
	runRoutineThrottleCheck(b, ctx, flags)
	waitForThrottleCheckOK(b, ctx)

	ticker := time.NewTicker(baseSleepInterval)
	defer ticker.Stop()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		conn, err := mysql.Connect(ctx, &vtParams)
		require.Nil(b, err)
		defer conn.Close()

		for pb.Next() {
			for throttleWorkload.Load() {
				<-ticker.C
			}
			for range len(ticker.C) {
				<-ticker.C
			}
			switch rand.Int32N(3) {
			case 0:
				err = generateInsert(b, conn)
			case 1:
				err = generateUpdate(b, conn)
			case 2:
				err = generateDelete(b, conn)
			}
			assert.Nil(b, err)
		}
	})
}
func BenchmarkWorkloadMultiConnThrottlerDisabled(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	throttler.DisableLagThrottlerAndWaitForStatus(b, clusterInstance)
	throttleWorkload.Store(false)
	testWithInitialSchema(b)
	initTable(b)

	ticker := time.NewTicker(baseSleepInterval)
	defer ticker.Stop()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		conn, err := mysql.Connect(ctx, &vtParams)
		require.Nil(b, err)
		defer conn.Close()

		for pb.Next() {
			for throttleWorkload.Load() {
				<-ticker.C
			}
			for range len(ticker.C) {
				<-ticker.C
			}
			switch rand.Int32N(3) {
			case 0:
				err = generateInsert(b, conn)
			case 1:
				err = generateUpdate(b, conn)
			case 2:
				err = generateDelete(b, conn)
			}
			assert.Nil(b, err)
		}
	})
}

func BenchmarkOnlineDDLWithWorkload(b *testing.B) {
	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testWithInitialSchema(b)
	initTable(b)
	throttler.EnableLagThrottlerAndWaitForStatus(b, clusterInstance)
	flags := &throttle.CheckFlags{SkipRequestHeartbeats: false}
	runRoutineThrottleCheck(b, ctx, flags)
	waitForThrottleCheckOK(b, ctx)

	ticker := time.NewTicker(baseSleepInterval)
	defer ticker.Stop()

	generateWorkload(b, ctx, &wg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hint := "post_completion_hint"
		uuid := testOnlineDDLStatement(b, fmt.Sprintf(alterHintStatement, hint), "online --force-cut-over-after=1s", "", true)
		_ = onlineddl.WaitForMigrationStatus(b, &vtParams, shards, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
	}
	cancel()
	wg.Wait()
	testSelectTableMetrics(b)
}

func BenchmarkOnlineDDLWithWorkloadThrottlerDisabled(b *testing.B) {
	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	throttler.DisableLagThrottlerAndWaitForStatus(b, clusterInstance)
	throttleWorkload.Store(false)
	testWithInitialSchema(b)
	initTable(b)

	ticker := time.NewTicker(baseSleepInterval)
	defer ticker.Stop()

	generateWorkload(b, ctx, &wg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hint := "post_completion_hint"
		uuid := testOnlineDDLStatement(b, fmt.Sprintf(alterHintStatement, hint), "online --force-cut-over-after=1s", "", true)
		_ = onlineddl.WaitForMigrationStatus(b, &vtParams, shards, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
	}
	cancel()
	wg.Wait()
	testSelectTableMetrics(b)
}
