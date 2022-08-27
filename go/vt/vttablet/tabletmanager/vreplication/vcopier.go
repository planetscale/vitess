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
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/bytes2"

	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type vcopier struct {
	vr               *vreplicator
	throttlerAppName string
}

// vcopierCopyTaskArgs stores the input of a copy task.
type vcopierCopyTask struct {
	args      *vcopierCopyTaskArgs
	id        int
	lifecycle *vcopierCopyTaskLifecycle
}

// vcopierCopyTaskArgs stores the input of a copy task.
type vcopierCopyTaskArgs struct {
	lastpk *querypb.Row
	rows   []*query.Row
}

type vcopierCopyTaskHooks struct {
	fns []func(context.Context, *vcopierCopyTaskArgs) error
}

// vcopierCopyTaskLifecycle can be used to inject additional behaviors into the vcopierCopyTask execution.
type vcopierCopyTaskLifecycle struct {
	hooks       map[string]*vcopierCopyTaskHooks
	resultHooks *vcopierCopyTaskResultHooks
}

type vcopierCopyTaskResult struct {
	args      *vcopierCopyTaskArgs
	err       error
	startedAt time.Time
	state     vcopierCopyTaskState
}

type vcopierCopyTaskResultHooks struct {
	fns []func(context.Context, *vcopierCopyTaskResult)
}

// vcopierCopyTaskState marks the states and sub-states that a copy task goes
// through.
//
//  1. Pending
//  2. Begin
//  3. Insert rows
//  4. Update copy state
//  5. Commit
//  6. One of:
//     - Complete
//     - Cancel
//     - Fail
type vcopierCopyTaskState int

const (
	vcopierCopyTaskPending vcopierCopyTaskState = iota
	vcopierCopyTaskBegin
	vcopierCopyTaskInsertRows
	vcopierCopyTaskUpdateCopyState
	vcopierCopyTaskCommit
	vcopierCopyTaskCancel
	vcopierCopyTaskComplete
	vcopierCopyTaskFail
)

// vcopierCopyWorkQueue accepts tasks via Enqueue, and distributes those tasks
// concurrently (or synchronously) to internal workers.
type vcopierCopyWorkQueue struct {
	concurrent    bool
	isOpen        bool
	maxDepth      int
	workerFactory func(context.Context) (*vcopierCopyWorker, error)
	workerPool    *pools.ResourcePool
}

// vcopierCopyWorker will Execute a single task at a time in the calling
// thread.
type vcopierCopyWorker struct {
	*vdbClient
	closeDbClient   bool
	copyStateUpdate *sqlparser.ParsedQuery
	isOpen          bool
	pkfields        []*querypb.Field
	sqlbuffer       bytes2.Buffer
	tablePlan       *TablePlan
}

func newVCopier(vr *vreplicator) *vcopier {
	return &vcopier{
		vr:               vr,
		throttlerAppName: vr.throttlerAppName(),
	}
}

func newVCopierCopyTask(id int, args *vcopierCopyTaskArgs) *vcopierCopyTask {
	return &vcopierCopyTask{
		args:      args,
		id:        id,
		lifecycle: newVCopierCopyTaskLifecycle(),
	}
}

// newVCopierCopyTask returns a blank vcopierCopyTask.
func newVCopierCopyTaskArgs(rows []*querypb.Row, lastpk *querypb.Row) *vcopierCopyTaskArgs {
	return &vcopierCopyTaskArgs{
		rows:   rows,
		lastpk: lastpk,
	}
}

func newVCopierCopyTaskHooks() *vcopierCopyTaskHooks {
	return &vcopierCopyTaskHooks{
		fns: make([]func(context.Context, *vcopierCopyTaskArgs) error, 0),
	}
}

func newVCopierCopyTaskLifecycle() *vcopierCopyTaskLifecycle {
	return &vcopierCopyTaskLifecycle{
		hooks:       make(map[string]*vcopierCopyTaskHooks),
		resultHooks: newVCopierCopyTaskResultHooks(),
	}
}

func newVCopierCopyTaskResult(args *vcopierCopyTaskArgs, startedAt time.Time, state vcopierCopyTaskState, err error) *vcopierCopyTaskResult {
	return &vcopierCopyTaskResult{
		args:      args,
		err:       err,
		startedAt: startedAt,
		state:     state,
	}
}

func newVCopierCopyTaskResultHooks() *vcopierCopyTaskResultHooks {
	return &vcopierCopyTaskResultHooks{
		fns: make([]func(context.Context, *vcopierCopyTaskResult), 0),
	}
}

func newVCopierCopyWorkQueue(
	concurrent bool,
	maxDepth int,
	workerFactory func(ctx context.Context) (*vcopierCopyWorker, error),
) *vcopierCopyWorkQueue {
	maxDepth = int(math.Max(float64(maxDepth), 1))
	return &vcopierCopyWorkQueue{
		concurrent:    concurrent,
		maxDepth:      maxDepth,
		workerFactory: workerFactory,
	}
}

func newVCopierCopyWorker(
	closeDbClient bool,
	vdbClient *vdbClient,
) *vcopierCopyWorker {
	return &vcopierCopyWorker{
		closeDbClient: closeDbClient,
		vdbClient:     vdbClient,
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

	log.Infof("Copying table %s, lastpk: %v", tableName, copyState[tableName])

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

	copyWorkerFactory := vc.newCopyWorkerFactory()
	copyWorkQueue := vc.newCopyWorkQueue(copyWorkerFactory)
	defer copyWorkQueue.Close()

	// Allocate a result channel to collect results from tasks.
	resultCh := make(chan *vcopierCopyTaskResult, getCopyInsertConcurrency()*4)
	defer close(resultCh)

	taskID := 0

	var lastpk *querypb.Row
	var pkfields []*querypb.Field
	var prevT *vcopierCopyTask

	serr := vc.vr.sourceVStreamer.VStreamRows(ctx, initialPlan.SendRule.Filter, lastpkpb, func(rows *binlogdatapb.VStreamRowsResponse) error {
		for {
			select {
			case <-rowsCopiedTicker.C:
				update := binlogplayer.GenerateUpdateRowsCopied(vc.vr.id, vc.vr.stats.CopyRowCount.Get())
				_, _ = vc.vr.dbClient.Execute(update)
			case <-ctx.Done():
				return io.EOF
			default:
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
		if !copyWorkQueue.IsOpen() {
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
			pkfields = append(pkfields, rows.Pkfields...)
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf(
				"update _vt.copy_state set lastpk=%a where vrepl_id=%s and table_name=%s", ":lastpk",
				strconv.Itoa(int(vc.vr.id)),
				encodeString(tableName),
			)
			copyStateUpdate := buf.ParsedQuery()
			copyWorkQueue.Open(copyStateUpdate, pkfields, tablePlan)
		}
		if len(rows.Rows) == 0 {
			return nil
		}

		// Clone rows, since pointer values will change while async work is
		// happening.
		rows = proto.Clone(rows).(*binlogdatapb.VStreamRowsResponse)

		// Prepare a vcopierCopyTask for the current batch of work.
		// TODO(maxeng) see if using a pre-allocated pool will speed things up.
		currT := newVCopierCopyTask(taskID, newVCopierCopyTaskArgs(rows.Rows, rows.Lastpk))
		taskID++

		// Sequence the previous task with the current task so that:
		// * The previous task is completed before we begin updating
		//   _vt.copy_state for the current task.
		// * If the previous task fails or is canceled, the current task is
		//   canceled.
		if prevT != nil {
			prevCh := make(chan *vcopierCopyTaskResult, 1)
			prevT.Lifecycle().OnResult().SendTo(prevCh)
			currT.Lifecycle().Before(vcopierCopyTaskUpdateCopyState).AwaitCompletion(prevCh)
		}

		// Store currT in prevT, because the next task will need to reference
		// currT in order to set up sequencing logic like the above.
		prevT = currT

		// Update stats after task is done.
		currT.Lifecycle().OnResult().Do(func(_ context.Context, result *vcopierCopyTaskResult) {
			if result.State() == vcopierCopyTaskFail {
				vc.vr.stats.ErrorCounts.Add([]string{"Copy"}, 1)
			}
			if result.State() == vcopierCopyTaskComplete {
				vc.vr.stats.CopyRowCount.Add(int64(len(result.Args().Rows())))
				vc.vr.stats.QueryCount.Add("copy", 1)
				vc.vr.stats.TableCopyRowCounts.Add(tableName, int64(len(result.Args().Rows())))
				vc.vr.stats.TableCopyTimings.Add(tableName, time.Since(result.StartedAt()))
			}
		})

		// Send result to resultCh.
		currT.Lifecycle().OnResult().SendTo(resultCh)

		if err := copyWorkQueue.Enqueue(ctx, currT); err != nil {
			log.Warningf("failed to enqueue task: %s", err.Error())
			return err
		}

		// When async execution is not enabled, a done task will be available
		// in the resultCh after each Enqueue, unless there was a queue state
		// error (e.g. couldn't obtain a worker from pool).
		//
		// When async execution is enabled, it's still preferable to
		// aggressively read from this channel so that:
		//
		// * resultCh doesn't fill up. If it does fill up then tasks won't be able to add their results to the channel,
		//   and progress in this thread will be blocked.
		// * We keep lastpk up-to-date, to minimize the likelihook that
		//   VStreamRows selects a batch that's in the process of inserting.
		select {
		case result := <-resultCh:
			if result != nil {
				switch result.State() {
				case vcopierCopyTaskCancel:
					log.Warningf("task was canceled")
					return io.EOF
				case vcopierCopyTaskComplete:
					// Collect lastpk. Needed for logging at the end.
					lastpk = result.Args().Lastpk()
				case vcopierCopyTaskFail:
					return fmt.Errorf("task error: %s", result.Error())
				}
			} else {
				return io.EOF
			}
		default:
		}

		return nil
	})

	copyWorkQueue.Close()

	// When tasks are executed async, there may be tasks that complete
	// after the last VStreamRows callback. Get the lastpk from completed tasks,
	// or errors from failed ones.
	var empty bool
	var terr error
	for !empty {
		select {
		case result := <-resultCh:
			switch result.State() {
			case vcopierCopyTaskComplete:
				lastpk = result.Args().Lastpk()
			case vcopierCopyTaskFail:
				if terr != nil {
					terr = result.Error()
				}
			}
		default:
			empty = true
		}
	}
	if terr != nil {
		log.Warningf("task error: %s", terr.Error())
		return fmt.Errorf("task error: %s", terr.Error())
	}

	// Get the last committed pk into a loggable form.
	var lastpkbuf []byte
	lastpkbuf, merr := prototext.Marshal(&querypb.QueryResult{
		Fields: pkfields,
		Rows:   []*querypb.Row{lastpk},
	})
	if merr != nil {
		return fmt.Errorf("failed to marshal pk fields and value into query result: %s", merr.Error())
	}
	lastpkbv := map[string]*querypb.BindVariable{
		"lastpk": {
			Type:  sqltypes.VarBinary,
			Value: lastpkbuf,
		},
	}

	// If there was a timeout, return without an error.
	select {
	case <-ctx.Done():
		log.Infof("Copy of %v stopped at lastpk: %v", tableName, lastpkbv)
		return nil
	default:
	}
	if serr != nil {
		return serr
	}

	log.Infof("Copy of %v finished at lastpk: %v", tableName, lastpkbv)
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

func (vc *vcopier) newClientConnection(ctx context.Context) (*vdbClient, error) {
	dbc := vc.vr.vre.dbClientFactoryFiltered()
	if err := dbc.Connect(); err != nil {
		return nil, vterrors.Wrap(err, "can't connect to database")
	}
	dbClient := newVDBClient(dbc, vc.vr.stats)
	if _, err := vc.vr.setSQLMode(ctx, dbClient); err != nil {
		return nil, vterrors.Wrap(err, "failed to set sql_mode")
	}
	if err := vc.vr.clearFKCheck(dbClient); err != nil {
		return nil, vterrors.Wrap(err, "failed to clear foreign key check")
	}
	return dbClient, nil
}

func (vc *vcopier) newCopyWorkQueue(
	workerFactory func(context.Context) (*vcopierCopyWorker, error),
) *vcopierCopyWorkQueue {
	concurrent := false
	maxDepth := 1
	if isExperimentalParallelBulkInsertsEnabled() {
		concurrent = true
		maxDepth = getCopyInsertConcurrency()
	}
	return newVCopierCopyWorkQueue(concurrent, maxDepth, workerFactory)
}

func (vc *vcopier) newCopyWorkerFactory() func(context.Context) (*vcopierCopyWorker, error) {
	workerFactory := func(_ context.Context) (*vcopierCopyWorker, error) {
		return newVCopierCopyWorker(
			false, /* close db client */
			vc.vr.dbClient,
		), nil
	}
	if isExperimentalParallelBulkInsertsEnabled() {
		workerFactory = func(ctx context.Context) (*vcopierCopyWorker, error) {
			dbClient, err := vc.newClientConnection(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to create new db client: %s", err.Error())
			}
			return newVCopierCopyWorker(
				true, /* close db client */
				dbClient,
			), nil
		}
	}
	return workerFactory
}

func (vcq *vcopierCopyWorkQueue) Enqueue(ctx context.Context, currT *vcopierCopyTask) error {
	if !vcq.IsOpen() {
		return fmt.Errorf("work queue is not open")
	}

	// Get a handle on an unused worker.
	poolH, err := vcq.workerPool.Get(ctx, []string{})
	if err != nil {
		return fmt.Errorf("failed to get a worker from pool: %s", err.Error())
	}

	currW := poolH.(*vcopierCopyWorker)

	execute := func(task *vcopierCopyTask) {
		currW.Execute(ctx, task)
		vcq.workerPool.Put(poolH)
	}

	// If the work queue is configured to work concurrently, execute the task
	// in a separate goroutine. Otherwise execute the task in the calling
	// goroutine.
	if vcq.concurrent {
		go execute(currT)
	} else {
		execute(currT)
	}

	return nil
}

func (vcq *vcopierCopyWorkQueue) Close() {
	if !vcq.IsOpen() {
		return
	}
	vcq.workerPool.Close()
	vcq.isOpen = false
}

func (vcq *vcopierCopyWorkQueue) IsOpen() bool {
	return vcq.isOpen
}

func (vcq *vcopierCopyWorkQueue) Open(
	copyStateUpdate *sqlparser.ParsedQuery,
	pkfields []*querypb.Field,
	tablePlan *TablePlan,
) {
	if vcq.IsOpen() {
		return
	}

	poolCapacity := int(math.Max(float64(vcq.maxDepth), 1))
	vcq.workerPool = pools.NewResourcePool(
		/* factory */
		func(ctx context.Context) (pools.Resource, error) {
			worker, err := vcq.workerFactory(ctx)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to create copier for resource pool: %s",
					err.Error(),
				)
			}
			worker.Open(copyStateUpdate, pkfields, tablePlan)
			return worker, nil
		},
		poolCapacity, /* initial capacity */
		poolCapacity, /* max capacity */
		0,            /* idle timeout */
		nil,          /* log wait */
		nil,          /* refresh check */
		0,            /* refresh interval */
	)

	vcq.isOpen = true
}

func (vct *vcopierCopyTask) Args() *vcopierCopyTaskArgs {
	return vct.args
}

func (vct *vcopierCopyTask) ID() int {
	return vct.id
}

func (vct *vcopierCopyTask) Lifecycle() *vcopierCopyTaskLifecycle {
	return vct.lifecycle
}

// Lastpk returns the lastpk to use to update _vt.copy_state.
func (vct *vcopierCopyTaskArgs) Lastpk() *querypb.Row {
	return vct.lastpk
}

// Rows returns the rows to be inserted into the target table.
func (vct *vcopierCopyTaskArgs) Rows() []*querypb.Row {
	return vct.rows
}

func (vtl *vcopierCopyTaskLifecycle) After(state vcopierCopyTaskState) *vcopierCopyTaskHooks {
	key := "after:" + state.String()
	if _, ok := vtl.hooks[key]; !ok {
		vtl.hooks[key] = newVCopierCopyTaskHooks()
	}
	return vtl.hooks[key]
}

func (vtl *vcopierCopyTaskLifecycle) Before(state vcopierCopyTaskState) *vcopierCopyTaskHooks {
	key := "before:" + state.String()
	if _, ok := vtl.hooks[key]; !ok {
		vtl.hooks[key] = newVCopierCopyTaskHooks()
	}
	return vtl.hooks[key]
}

func (vtl *vcopierCopyTaskLifecycle) OnResult() *vcopierCopyTaskResultHooks {
	return vtl.resultHooks
}

// TryAdvance is a convenient way of wrapping up lifecycle hooks with task
// execution steps. E.g.:
//
//	if err := task.Lifecycle().Before(nextState).Notify(ctx, args); err != nil {
//		return err
//	}
//	if err := fn(ctx, args); err != nil {
//		return err
//	}
//	if err := task.Lifecycle().After(nextState).Notify(ctx, args); err != nil {
//		return err
//	}
//
// Is equivalent to:
//
//	if err := task.Lifecycle.TryAdvance(ctx, args, nextState, fn); err != nil {
//	  return err
//	}
func (vtl *vcopierCopyTaskLifecycle) TryAdvance(
	ctx context.Context,
	args *vcopierCopyTaskArgs,
	nextState vcopierCopyTaskState,
	fn func(context.Context, *vcopierCopyTaskArgs) error,
) (newState vcopierCopyTaskState, err error) {
	defer func() {
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				newState = vcopierCopyTaskCancel
			} else {
				newState = vcopierCopyTaskFail
			}
		}
	}()
	newState = nextState
	if err = ctx.Err(); err != nil {
		return
	}
	if err = vtl.Before(nextState).Notify(ctx, args); err != nil {
		newState = vcopierCopyTaskFail
		return
	}
	if err = fn(ctx, args); err != nil {
		newState = vcopierCopyTaskFail
		return
	}
	if err = vtl.After(nextState).Notify(ctx, args); err != nil {
		newState = vcopierCopyTaskFail
		return
	}
	return
}

func (vrh *vcopierCopyTaskResultHooks) Do(fn func(context.Context, *vcopierCopyTaskResult)) {
	vrh.fns = append(vrh.fns, fn)
}

func (vrh *vcopierCopyTaskResultHooks) Notify(ctx context.Context, result *vcopierCopyTaskResult) {
	for _, fn := range vrh.fns {
		fn(ctx, result)
	}
}

// SendTo registers a hook that accepts a result and sends the result to the
// provided channel. E.g.:
//
//	resultCh := make(chan *vcopierCopyTaskResult, 1)
//	task.Lifecycle().OnResult().SendTo(resultCh)
//	defer func() {
//	  result := <-resultCh
//	}()
func (vrh *vcopierCopyTaskResultHooks) SendTo(ch chan<- *vcopierCopyTaskResult) {
	vrh.Do(func(ctx context.Context, result *vcopierCopyTaskResult) {
		select {
		case ch <- result:
		case <-ctx.Done():
			// Log a warning?
		}
	})
}

// AwaitCompletion registers a hook that returns an error unless the provided
// chan produces a vcopierTaskResult in a complete state.
//
// This is useful for sequencing vcopierCopyTasks, e.g.:
//
//	resultCh := make(chan *vcopierCopyTaskResult, 1)
//	prevT.Lifecycle().OnResult().SendTo(resultCh)
//	currT.lifecycle().Before(vcopierCopyTaskUpdateCopyState).AwaitCompletion(resultCh)
func (vth *vcopierCopyTaskHooks) AwaitCompletion(resultCh <-chan *vcopierCopyTaskResult) {
	vth.Do(func(ctx context.Context, args *vcopierCopyTaskArgs) error {
		select {
		case result := <-resultCh:
			if result == nil {
				return fmt.Errorf("channel was closed before a result received")
			}
			if !result.State().IsDone() {
				return fmt.Errorf("received result is not done")
			}
			if result.State() != vcopierCopyTaskComplete {
				return fmt.Errorf("received result is not complete")
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func (vth *vcopierCopyTaskHooks) Do(fn func(context.Context, *vcopierCopyTaskArgs) error) {
	vth.fns = append(vth.fns, fn)
}

func (vth *vcopierCopyTaskHooks) Notify(ctx context.Context, args *vcopierCopyTaskArgs) error {
	for _, fn := range vth.fns {
		if err := fn(ctx, args); err != nil {
			return err
		}
	}
	return nil
}

func (vtr *vcopierCopyTaskResult) Args() *vcopierCopyTaskArgs {
	return vtr.args
}

func (vtr *vcopierCopyTaskResult) Error() error {
	return vtr.err
}

func (vtr *vcopierCopyTaskResult) StartedAt() time.Time {
	return vtr.startedAt
}

func (vtr *vcopierCopyTaskResult) State() vcopierCopyTaskState {
	return vtr.state
}

// IsDone returns true if the provided state is in one of these three states:
//
// * vcopierCopyTaskCancel
// * vcopierCopyTaskComplete
// * vcopierCopyTaskFail
func (vts vcopierCopyTaskState) IsDone() bool {
	return vts == vcopierCopyTaskCancel ||
		vts == vcopierCopyTaskComplete ||
		vts == vcopierCopyTaskFail
}

// Next returns the optimistic next state. The Next state after
// vcopierCopyTaskPending is vcopierCopyTaskInProgress, followed
// by vcopierCopyTaskInsertRows, etc.
func (vts vcopierCopyTaskState) Next() vcopierCopyTaskState {
	switch vts {
	case vcopierCopyTaskPending:
		return vcopierCopyTaskBegin
	case vcopierCopyTaskBegin:
		return vcopierCopyTaskInsertRows
	case vcopierCopyTaskInsertRows:
		return vcopierCopyTaskUpdateCopyState
	case vcopierCopyTaskUpdateCopyState:
		return vcopierCopyTaskCommit
	case vcopierCopyTaskCommit:
		return vcopierCopyTaskComplete
	}
	return vts
}

func (vts vcopierCopyTaskState) String() string {
	switch vts {
	case vcopierCopyTaskPending:
		return "pending"
	case vcopierCopyTaskBegin:
		return "begin"
	case vcopierCopyTaskInsertRows:
		return "insert-rows"
	case vcopierCopyTaskUpdateCopyState:
		return "update-copy-state"
	case vcopierCopyTaskCommit:
		return "commit"
	case vcopierCopyTaskCancel:
		return "done:cancel"
	case vcopierCopyTaskComplete:
		return "done:complete"
	case vcopierCopyTaskFail:
		return "done:fail"
	}

	return fmt.Sprintf("undefined(%d)", int(vts))
}

// ApplySettings implements pool.Resource.
func (vbc *vcopierCopyWorker) ApplySettings(context.Context, []string) error {
	return nil
}

// Execute advances a task through each state until it is canceled, completed
// or failed.
func (vbc *vcopierCopyWorker) Execute(ctx context.Context, task *vcopierCopyTask) *vcopierCopyTaskResult {
	startedAt := time.Now()
	state := vcopierCopyTaskPending

	var err error

	for nextState := state.Next(); !state.IsDone(); nextState = state.Next() {
		var advanceFn func(context.Context, *vcopierCopyTaskArgs) error

		switch nextState {
		case vcopierCopyTaskBegin:
			advanceFn = func(context.Context, *vcopierCopyTaskArgs) error {
				// Rollback to make sure we're in a clean state.
				if err := vbc.vdbClient.Rollback(); err != nil {
					return fmt.Errorf("failed to rollback: %s", err.Error())
				}
				// Begin transaction.
				if err := vbc.vdbClient.Begin(); err != nil {
					return fmt.Errorf("failed to start transaction: %s", err.Error())
				}
				return nil
			}
		case vcopierCopyTaskInsertRows:
			advanceFn = func(ctx context.Context, args *vcopierCopyTaskArgs) error {
				if _, err := vbc.insertRows(ctx, args.Rows()); err != nil {
					return fmt.Errorf("failed inserting rows: %s", err.Error())
				}
				return nil
			}
		case vcopierCopyTaskUpdateCopyState:
			advanceFn = func(ctx context.Context, args *vcopierCopyTaskArgs) error {
				if err := vbc.updateCopyState(ctx, args.Lastpk()); err != nil {
					return fmt.Errorf("error updating _vt.copy_state: %s", err.Error())
				}
				return nil
			}
		case vcopierCopyTaskCommit:
			advanceFn = func(context.Context, *vcopierCopyTaskArgs) error {
				// Commit.
				if err := vbc.vdbClient.Commit(); err != nil {
					return fmt.Errorf("error commiting transaction: %s", err.Error())
				}
				return nil
			}
		case vcopierCopyTaskComplete:
			advanceFn = func(context.Context, *vcopierCopyTaskArgs) error { return nil }
		}

		if advanceFn == nil {
			err = fmt.Errorf("don't know how to advance from %s to %s", state, nextState)
			state = vcopierCopyTaskFail
			break
		}

		if state, err = task.Lifecycle().TryAdvance(ctx, task.Args(), nextState, advanceFn); err != nil {
			break
		}
	}

	result := newVCopierCopyTaskResult(task.Args(), startedAt, state, err)

	task.Lifecycle().OnResult().Notify(ctx, result)

	return result
}

func (vbc *vcopierCopyWorker) IsOpen() bool {
	return vbc.isOpen
}

// IsSameSetting implements pool.Resource.
func (vbc *vcopierCopyWorker) IsSameSetting([]string) bool {
	return true
}

// IsSettingsApplied implements pool.Resource.
func (vbc *vcopierCopyWorker) IsSettingsApplied() bool {
	return false
}

func (vbc *vcopierCopyWorker) Open(
	copyStateUpdate *sqlparser.ParsedQuery,
	pkfields []*querypb.Field,
	tablePlan *TablePlan,
) {
	if vbc.isOpen {
		return
	}
	vbc.copyStateUpdate = copyStateUpdate
	vbc.isOpen = true
	vbc.pkfields = pkfields
	vbc.tablePlan = tablePlan
}

func (vbc *vcopierCopyWorker) Close() {
	if !vbc.IsOpen() {
		return
	}

	if vbc.closeDbClient {
		vbc.vdbClient.Close()
	}
	vbc.isOpen = false
}

func (vbc *vcopierCopyWorker) insertRows(ctx context.Context, rows []*querypb.Row) (*sqltypes.Result, error) {
	return vbc.tablePlan.applyBulkInsert(
		&vbc.sqlbuffer,
		rows,
		func(sql string) (*sqltypes.Result, error) {
			return vbc.vdbClient.ExecuteWithRetry(ctx, sql)
		},
	)
}

func (vbc *vcopierCopyWorker) updateCopyState(ctx context.Context, lastpk *querypb.Row) error {
	var buf []byte
	buf, err := prototext.Marshal(&querypb.QueryResult{
		Fields: vbc.pkfields,
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
	copyStateUpdate, err := vbc.copyStateUpdate.GenerateQuery(bv, nil)
	if err != nil {
		return err
	}
	if _, err := vbc.vdbClient.Execute(copyStateUpdate); err != nil {
		return err
	}
	return nil
}
