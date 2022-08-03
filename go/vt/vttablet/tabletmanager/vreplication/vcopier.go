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

package vreplication

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/vterrors"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/bytes2"

	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type vcopier struct {
	vr               *vreplicator
	throttlerAppName string
}

type vcopierWorker struct {
	ch              chan error
	closeFunc       func()
	copyStateUpdate *sqlparser.ParsedQuery
	dbClient        *vdbClient
	pkFields        []*querypb.Field
	sqlbuffer       bytes2.Buffer
	tablePlan       *TablePlan
}

type vcopierWorkerPool struct {
	async           bool
	commitQ         chan *vcopierWorkerTask
	dbClientFactory func() (*vdbClient, func(), error)
	doneQ           chan *vcopierWorkerTask
	errorRecorder   concurrency.ErrorRecorder
	inQ             chan *vcopierWorkerTask
	resourcePool    *pools.ResourcePool
	quitCh          chan bool
	size            int
	started         bool
	stats           *binlogplayer.Stats
}

type vcopierWorkerTask struct {
	commitCh chan bool
	ctx      context.Context
	lastErr  error
	rows     *binlogdatapb.VStreamRowsResponse
	worker   *vcopierWorker
}

func getCopyInsertConcurrency() int {
	copyInsertConcurrency := int(*vreplicationParallelBulkInserts)
	if !isExperimentalParallelBulkInsertsEnabled() {
		copyInsertConcurrency = 1
	}
	return copyInsertConcurrency
}

func isExperimentalParallelBulkInsertsEnabled() bool {
	return *vreplicationExperimentalFlags /**/ & /**/ vreplicationExperimentalParallelizeBulkInserts != 0
}

func newVCopier(vr *vreplicator) *vcopier {
	return &vcopier{
		vr:               vr,
		throttlerAppName: vr.throttlerAppName(),
	}
}

func newVCopierWorkerPool(
	async bool,
	dbClientFactory func() (*vdbClient, func(), error),
	size int,
	stats *binlogplayer.Stats,
) *vcopierWorkerPool {
	return &vcopierWorkerPool{
		async:           async,
		commitQ:         make(chan *vcopierWorkerTask, size),
		doneQ:           make(chan *vcopierWorkerTask, size),
		dbClientFactory: dbClientFactory,
		errorRecorder:   &concurrency.AllErrorRecorder{},
		inQ:             make(chan *vcopierWorkerTask, size),
		quitCh:          make(chan bool),
		size:            size,
		stats:           stats,
	}
}

// initTablesForCopy (phase 1) identifies the list of tables to be copied and inserts
// them into copy_state. If there are no tables to copy, it explicitly stops
// the stream. Otherwise, the copy phase (phase 2) may think that all tables are copied.
// This will cause us to go into the replication phase (phase 3) without a starting position.
func (vc *vcopier) initTablesForCopy(ctx context.Context) error {
	defer vc.vr.dbClient.Rollback()

	plan, err := buildReplicatorPlan(vc.vr.source, vc.vr.colInfoMap, nil, vc.vr.stats)
	if err != nil {
		return err
	}
	if err := vc.vr.dbClient.Begin(); err != nil {
		return err
	}
	// Insert the table list only if at least one table matches.
	if len(plan.TargetTables) != 0 {
		var buf strings.Builder
		buf.WriteString("insert into _vt.copy_state(vrepl_id, table_name) values ")
		prefix := ""
		for name := range plan.TargetTables {
			fmt.Fprintf(&buf, "%s(%d, %s)", prefix, vc.vr.id, encodeString(name))
			prefix = ", "
		}
		if _, err := vc.vr.dbClient.Execute(buf.String()); err != nil {
			return err
		}
		if err := vc.vr.setState(binlogplayer.VReplicationCopying, ""); err != nil {
			return err
		}
		if err := vc.vr.insertLog(LogCopyStart, fmt.Sprintf("Copy phase started for %d table(s)",
			len(plan.TargetTables))); err != nil {
			return err
		}
	} else {
		if err := vc.vr.setState(binlogplayer.BlpStopped, "There is nothing to replicate"); err != nil {
			return err
		}
	}
	return vc.vr.dbClient.Commit()
}

// copyNext performs a multi-step process on each iteration.
// Step 1: catchup: During this step, it replicates from the source from the last position.
// This is a partial replication: events are applied only to tables or subsets of tables
// that have already been copied. This goes on until replication catches up.
// Step 2: Start streaming. This returns the initial field info along with the GTID
// as of which the snapshot is being streamed.
// Step 3: fastForward: The target is fast-forwarded to the GTID obtained. This should
// be quick because we were mostly caught up as of step 1. This ensures that the
// snapshot of the rows are consistent with the position where the target stopped.
// Step 4: copy rows: Copy the next set of rows from the stream that was started in Step 2.
// This goes on until all rows are copied, or a timeout. In both cases, copyNext
// returns, and the replicator decides whether to invoke copyNext again, or to
// go to the next phase if all the copying is done.
// Steps 2, 3 and 4 are performed by copyTable.
// copyNext also builds the copyState metadata that contains the tables and their last
// primary key that was copied. A nil Result means that nothing has been copied.
// A table that was fully copied is removed from copyState.
func (vc *vcopier) copyNext(ctx context.Context, settings binlogplayer.VRSettings) error {
	qr, err := vc.vr.dbClient.Execute(fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id=%d", vc.vr.id))
	if err != nil {
		return err
	}
	var tableToCopy string
	copyState := make(map[string]*sqltypes.Result)
	for _, row := range qr.Rows {
		tableName := row[0].ToString()
		lastpk := row[1].ToString()
		if tableToCopy == "" {
			tableToCopy = tableName
		}
		copyState[tableName] = nil
		if lastpk != "" {
			var r querypb.QueryResult
			if err := prototext.Unmarshal([]byte(lastpk), &r); err != nil {
				return err
			}
			copyState[tableName] = sqltypes.Proto3ToResult(&r)
		}
	}
	if len(copyState) == 0 {
		return fmt.Errorf("unexpected: there are no tables to copy")
	}
	if err := vc.catchup(ctx, copyState); err != nil {
		return err
	}
	return vc.copyTable(ctx, tableToCopy, copyState)
}

// catchup replays events to the subset of the tables that have been copied
// until replication is caught up. In order to stop, the seconds behind primary has
// to fall below replicationLagTolerance.
func (vc *vcopier) catchup(ctx context.Context, copyState map[string]*sqltypes.Result) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer vc.vr.stats.PhaseTimings.Record("catchup", time.Now())

	settings, err := binlogplayer.ReadVRSettings(vc.vr.dbClient, vc.vr.id)
	if err != nil {
		return err
	}
	// If there's no start position, it means we're copying the
	// first table. So, there's nothing to catch up to.
	if settings.StartPos.IsZero() {
		return nil
	}

	// Start vreplication.
	errch := make(chan error, 1)
	go func() {
		errch <- newVPlayer(vc.vr, settings, copyState, mysql.Position{}, "catchup").play(ctx)
	}()

	// Wait for catchup.
	tkr := time.NewTicker(waitRetryTime)
	defer tkr.Stop()
	seconds := int64(*replicaLagTolerance / time.Second)
	for {
		sbm := vc.vr.stats.ReplicationLagSeconds.Get()
		if sbm < seconds {
			cancel()
			// Make sure vplayer returns before returning.
			<-errch
			return nil
		}
		select {
		case err := <-errch:
			if err != nil {
				return err
			}
			return io.EOF
		case <-ctx.Done():
			// Make sure vplayer returns before returning.
			<-errch
			return io.EOF
		case <-tkr.C:
		}
	}
}

// copyTable performs the synchronized copy of the next set of rows from
// the current table being copied. Each packet received is transactionally
// committed with the lastpk. This allows for consistent resumability.
func (vc *vcopier) copyTable(ctx context.Context, tableName string, copyState map[string]*sqltypes.Result) error {
	defer vc.vr.dbClient.Rollback()
	defer vc.vr.stats.PhaseTimings.Record("copy", time.Now())
	defer vc.vr.stats.CopyLoopCount.Add(1)

	plan, err := buildReplicatorPlan(vc.vr.source, vc.vr.colInfoMap, nil, vc.vr.stats)
	if err != nil {
		return err
	}

	initialPlan, ok := plan.TargetTables[tableName]
	if !ok {
		return fmt.Errorf("plan not found for table: %s, current plans are: %#v", tableName, plan.TargetTables)
	}

	ctx, cancel := context.WithTimeout(ctx, *copyPhaseDuration)
	defer cancel()

	var lastpkpb *querypb.QueryResult
	if lastpkqr := copyState[tableName]; lastpkqr != nil {
		lastpkpb = sqltypes.ResultToProto3(lastpkqr)
	}

	rowsCopiedTicker := time.NewTicker(rowsCopiedUpdateInterval)
	defer rowsCopiedTicker.Stop()

	async := isExperimentalParallelBulkInsertsEnabled()
	copyInsertConcurrency := getCopyInsertConcurrency()

	dbClientFactory := func() (*vdbClient, func(), error) {
		dbClient, err := vc.newClientConnection()
		return dbClient, dbClient.Close, err
	}
	if !async {
		dbClientFactory = func() (*vdbClient, func(), error) {
			return vc.vr.dbClient, func() {}, nil
		}
	}

	workerPool := newVCopierWorkerPool(
		async,                 /* async */
		dbClientFactory,       /* db client factory */
		copyInsertConcurrency, /* pool size */
		vc.vr.stats,           /* stats recorders */
	)

	defer workerPool.stop()

	var pkFields []*querypb.Field
	var lastpk *querypb.Row

	err = vc.vr.sourceVStreamer.VStreamRows(ctx, initialPlan.SendRule.Filter, lastpkpb, func(rows *binlogdatapb.VStreamRowsResponse) error {
		for {
			select {
			case <-rowsCopiedTicker.C:
				update := binlogplayer.GenerateUpdateRowsCopied(vc.vr.id, vc.vr.stats.CopyRowCount.Get())
				_, _ = vc.vr.dbClient.Execute(update)
			case <-ctx.Done():
				return io.EOF
			default:
			}
			// Collect and return any async errors.
			if err := workerPool.error(); err != nil {
				return err
			}
			if rows.Throttled {
				_ = vc.vr.updateTimeThrottled(RowStreamerComponentName)
				return nil
			}
			if rows.Heartbeat {
				_ = vc.vr.updateHeartbeatTime(time.Now().Unix())
				return nil
			}
			// verify throttler is happy, otherwise keep looping
			if vc.vr.vre.throttlerClient.ThrottleCheckOKOrWaitAppName(ctx, vc.throttlerAppName) {
				break // out of 'for' loop
			} else { // we're throttled
				_ = vc.vr.updateTimeThrottled(VCopierComponentName)
			}
		}
		if !workerPool.started {
			if len(rows.Fields) == 0 {
				return fmt.Errorf("expecting field event first, got: %v", rows)
			}
			if err := vc.fastForward(ctx, copyState, rows.Gtid); err != nil {
				return err
			}
			fieldEvent := &binlogdatapb.FieldEvent{
				TableName: initialPlan.SendRule.Match,
			}
			fieldEvent.Fields = append(fieldEvent.Fields, rows.Fields...)
			tablePlan, err := plan.buildExecutionPlan(fieldEvent)
			if err != nil {
				return err
			}
			pkFields = append(pkFields, rows.Pkfields...)
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf(
				"update _vt.copy_state set lastpk=%a where vrepl_id=%s and table_name=%s", ":lastpk",
				strconv.Itoa(int(vc.vr.id)),
				encodeString(tableName),
			)
			workerPool.start(buf.ParsedQuery(), pkFields, tablePlan)
		}
		if len(rows.Rows) == 0 {
			return nil
		}
		return workerPool.copy(ctx, rows)
	})

	// Stop the worker pool, waiting for any inflight workers to finish.
	// Return any uncollected errors.
	workerPool.stop()
	if err := workerPool.error(); err != nil {
		return err
	}

	// If there was a timeout, return without an error.
	select {
	case <-ctx.Done():
		log.Infof("Copy of %v stopped at lastpk: %v", tableName, lastpk)
		return nil
	default:
	}
	if err != nil {
		return err
	}
	log.Infof("Copy of %v finished at lastpk: %v", tableName, lastpk)
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf(
		"delete from _vt.copy_state where vrepl_id=%s and table_name=%s",
		strconv.Itoa(int(vc.vr.id)),
		encodeString(tableName),
	)
	if _, err := vc.vr.dbClient.Execute(buf.String()); err != nil {
		return err
	}
	return nil
}

func (vc *vcopier) fastForward(ctx context.Context, copyState map[string]*sqltypes.Result, gtid string) error {
	defer vc.vr.stats.PhaseTimings.Record("fastforward", time.Now())
	pos, err := mysql.DecodePosition(gtid)
	if err != nil {
		return err
	}
	settings, err := binlogplayer.ReadVRSettings(vc.vr.dbClient, vc.vr.id)
	if err != nil {
		return err
	}
	if settings.StartPos.IsZero() {
		update := binlogplayer.GenerateUpdatePos(vc.vr.id, pos, time.Now().Unix(), 0, vc.vr.stats.CopyRowCount.Get(), *vreplicationStoreCompressedGTID)
		_, err := vc.vr.dbClient.Execute(update)
		return err
	}
	return newVPlayer(vc.vr, settings, copyState, pos, "fastforward").play(ctx)
}

func (vc *vcopier) newClientConnection() (*vdbClient, error) {
	dbc := vc.vr.vre.dbClientFactoryFiltered()
	if err := dbc.Connect(); err != nil {
		return nil, vterrors.Wrap(err, "can't connect to database")
	}
	dbClient := newVDBClient(dbc, vc.vr.stats)
	_, err := dbClient.Execute("set foreign_key_checks=0;")
	if err != nil {
		return nil, err
	}
	return dbClient, nil
}

// Implement pools.Resource.
func (vcw *vcopierWorker) Close() {
	vcw.closeFunc()
}

func (vcw *vcopierWorker) begin() error {
	if err := vcw.dbClient.Begin(); err != nil {
		return err
	}
	return nil
}

func (vcw *vcopierWorker) commit() error {
	return vcw.dbClient.Commit()
}

// Internal. Used by doCopy.
func (vcw *vcopierWorker) insertRows(ctx context.Context, rows []*querypb.Row) (*sqltypes.Result, error) {
	return vcw.tablePlan.applyBulkInsert(&vcw.sqlbuffer, rows, func(sql string) (*sqltypes.Result, error) {
		qr, err := vcw.dbClient.ExecuteWithRetry(ctx, sql)
		if err != nil {
			return nil, err
		}
		return qr, nil
	})
}

// Internal. Used by doCopy.
func (vcw *vcopierWorker) updateCopyState(ctx context.Context, lastpk *querypb.Row) error {
	var buf []byte
	buf, err := prototext.Marshal(&querypb.QueryResult{
		Fields: vcw.pkFields,
		Rows:   []*querypb.Row{lastpk},
	})
	if err != nil {
		return err
	}
	bv := map[string]*querypb.BindVariable{
		"lastpk": {
			Type:  sqltypes.VarBinary,
			Value: buf,
		},
	}
	copyStateUpdate, err := vcw.copyStateUpdate.GenerateQuery(bv, nil)
	if err != nil {
		return err
	}
	if _, err := vcw.dbClient.Execute(copyStateUpdate); err != nil {
		return err
	}
	return nil
}

// Internal. Consume enqueued tasks in a loop. Move tasks from the inQ to the
// commitQ to the doneQ. Inserts rows and updates copy state in a transaction.
// Inserts may happen out-of-order, but commits happen in the order that tasks
// were enqueued.
func (vcwp *vcopierWorkerPool) consume() {
	var nextCommitTask *vcopierWorkerTask

	defer func() {
		vcwp.quitCh <- true
	}()

	for {
		select {
		case task := <-vcwp.inQ:
			// Get in line to commit.
			vcwp.commitQ <- task
			// Copy rows asynchronously
			go vcwp.doCopy(task)
		case task := <-vcwp.doneQ:
			// Unblock the next commit.
			if task == nextCommitTask {
				nextCommitTask = nil
			}
		default:
		}

		if nextCommitTask != nil {
			continue
		}

		select {
		// Signal the next commit may proceed.
		case nextCommitTask := <-vcwp.commitQ:
			nextCommitTask.commitCh <- nextCommitTask.lastErr == nil
		// Check the quitCh.
		case <-vcwp.quitCh:
			return
		default:
		}
	}
}

func (vcwp *vcopierWorkerPool) copy(ctx context.Context, rows *binlogdatapb.VStreamRowsResponse) error {
	if !vcwp.started {
		return fmt.Errorf("worker pool is not started")
	}

	// Prepare a task.
	task := &vcopierWorkerTask{
		commitCh: make(chan bool, 1),
		ctx:      ctx,
		rows:     rows,
	}

	// Copy (a)synchronously.
	if vcwp.async {
		// Clone rows, since pointer values will change while async work is
		// happening.
		task.rows = proto.Clone(task.rows).(*binlogdatapb.VStreamRowsResponse)
		return vcwp.copyAsync(task)
	}

	return vcwp.copySync(task)
}

// Internal. Use copy instead.
func (vcwp *vcopierWorkerPool) copyAsync(task *vcopierWorkerTask) error {
	vcwp.inQ <- task
	return nil
}

// Internal. Use copy instead.
func (vcwp *vcopierWorkerPool) copySync(task *vcopierWorkerTask) error {
	task.commitCh <- true
	vcwp.doCopy(task)
	select {
	case <-vcwp.doneQ:
	default:
	}
	return vcwp.error()
}

// Internal. Use copy instead.
func (vcwp *vcopierWorkerPool) doCopy(task *vcopierWorkerTask) {
	ctx := task.ctx
	lastpk := task.rows.Lastpk
	rows := task.rows.Rows
	numRows := len(rows)

	start := time.Now()

	var bandwidth int64
	var commit bool
	var elapsedTime float64
	var err error
	var rs pools.Resource
	var worker *vcopierWorker

	// Get a worker from pool. Return worker to pool at func exit.
	rs, err = vcwp.resourcePool.Get(ctx)
	if err != nil {
		task.lastErr = fmt.Errorf("failed to get vcopier worker: %s", err.Error())
		goto DONE
	} else {
		defer vcwp.resourcePool.Put(rs)
		worker = rs.(*vcopierWorker)
	}

	// Do actual work.
	if err = worker.begin(); err != nil {
		task.lastErr = fmt.Errorf("error beginning copy-update-state transaction: %s", err.Error())
		goto DONE
	}
	if _, err = worker.insertRows(ctx, rows); err != nil {
		task.lastErr = fmt.Errorf("error inserting rows: %s", err.Error())
		goto DONE
	}
	if err = worker.updateCopyState(ctx, lastpk); err != nil {
		task.lastErr = fmt.Errorf("error updating _vt.copy_state: %s", err.Error())
		goto DONE
	}
	// Wait for our turn to commit.
	if commit = <-task.commitCh; !commit {
		goto DONE
	}
	if err = worker.commit(); err != nil {
		task.lastErr = fmt.Errorf("error commiting copy-update-state transaction: %s", err.Error())
		goto DONE
	}

	// Update stats.
	elapsedTime = float64(time.Now().UnixNano()-start.UnixNano()) / 1e9
	bandwidth = int64(float64(numRows) / elapsedTime)
	vcwp.stats.CopyBandwidth.Set(bandwidth)
	vcwp.stats.CopyRowCount.Add(int64(numRows))
	vcwp.stats.QueryCount.Add("copy", 1)

DONE:
	// Handle errors.
	if task.lastErr != nil {
		vcwp.stats.ErrorCounts.Add([]string{"BulkCopy"}, 1)
		vcwp.errorRecorder.RecordError(task.lastErr)
	}

	vcwp.doneQ <- task
}

func (vcwp *vcopierWorkerPool) error() error {
	if vcwp.errorRecorder.HasErrors() {
		return vcwp.errorRecorder.Error()
	}
	return nil
}

func (vcwp *vcopierWorkerPool) start(
	copyStateUpdate *sqlparser.ParsedQuery,
	pkFields []*querypb.Field,
	tablePlan *TablePlan,
) {
	if vcwp.started {
		return
	}

	vcwp.started = true

	vcwp.resourcePool = pools.NewResourcePool(
		/* factory */
		func(ctx context.Context) (pools.Resource, error) {
			dbClient, closeFunc, err := vcwp.dbClientFactory()
			if err != nil {
				return nil, err
			}
			return &vcopierWorker{
				ch:              make(chan error, 1),
				copyStateUpdate: copyStateUpdate,
				closeFunc:       closeFunc,
				dbClient:        dbClient,
				pkFields:        pkFields,
				sqlbuffer:       bytes2.Buffer{},
				tablePlan:       tablePlan,
			}, nil
		},
		vcwp.size, /* initial capacity */
		vcwp.size, /* max capacity */
		0,         /* idle timeout */
		nil,       /* log wait */
		nil,       /* refresh check */
		0,         /* refresh interval */
	)

	// Consume tasks from the inQ.
	if vcwp.async {
		go vcwp.consume()
	}
}

func (vcwp *vcopierWorkerPool) stop() {
	if !vcwp.started {
		return
	}

	vcwp.started = false

	vcwp.resourcePool.Close()

	// Signal the quitCh to tell consume() to exit,
	// and wait for consume() to signal back to us.
	if vcwp.async {
		vcwp.quitCh <- true
		<-vcwp.quitCh
	}
}
