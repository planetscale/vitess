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
	"math"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/vt/concurrency"
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
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type vcopier struct {
	vr               *vreplicator
	throttlerAppName string
}

type vcopierBasicCopier struct {
	*vdbClient

	closeDbClient   bool
	copyStateUpdate *sqlparser.ParsedQuery
	hooks           map[string]*event.Hooks
	isOpen          bool
	lastCommittedPk *querypb.Row
	pkfields        []*querypb.Field
	sqlbuffer       bytes2.Buffer
	stats           *binlogplayer.Stats
	tablePlan       *TablePlan
}

type vcopierConcurrentCopier struct {
	copierFactory   func() (vcopierCopier, error)
	copierPool      *pools.ResourcePool
	errors          concurrency.ErrorRecorder
	isOpen          bool
	lastCommitCh    chan bool
	lastCommittedPk *querypb.Row
	size            int
}

type vcopierCopier interface {
	pools.Resource

	BeforeUpdateCopyState() *event.Hooks

	Copy(ctx context.Context, rows []*querypb.Row, lastpk *querypb.Row) error
	Error() error
	IsOpen() bool
	LastCommittedPk() *querypb.Row
	Open(
		copyStateUpdate *sqlparser.ParsedQuery,
		pkfields []*querypb.Field,
		tablePlan *TablePlan,
	)
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

func newVCopierBasicCopier(
	closeDbClient bool,
	stats *binlogplayer.Stats,
	vdbClient *vdbClient,
) *vcopierBasicCopier {
	hooks := make(map[string]*event.Hooks)
	hooks["copy:after"] = &event.Hooks{}
	hooks["updateCopyState:before"] = &event.Hooks{}

	return &vcopierBasicCopier{
		closeDbClient: closeDbClient,
		hooks:         hooks,
		stats:         stats,
		vdbClient:     vdbClient,
	}
}

func newVCopierConcurrentCopier(
	copierFactory func() (vcopierCopier, error),
	size int,
	stats *binlogplayer.Stats,
) *vcopierConcurrentCopier {
	return &vcopierConcurrentCopier{
		copierFactory: copierFactory,
		errors:        &concurrency.AllErrorRecorder{},
		size:          int(math.Max(float64(size), 1)),
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

	copier := vc.newCopier()
	defer copier.Close()

	var pkfields []*querypb.Field

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
			// Collect and return any async errors.
			if err := copier.Error(); err != nil {
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
		if !copier.IsOpen() {
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
			copier.Open(copyStateUpdate, pkfields, tablePlan)
		}
		if len(rows.Rows) == 0 {
			return nil
		}

		// Clone rows, since pointer values will change while async work is
		// happening.
		rows = proto.Clone(rows).(*binlogdatapb.VStreamRowsResponse)

		return copier.Copy(ctx, rows.Rows, rows.Lastpk)
	})

	// Stop the worker pool, waiting for any inflight workers to finish.
	// Return any uncollected errors.
	copier.Close()
	if cerr := copier.Error(); cerr != nil {
		return fmt.Errorf("encountered an error after closing copier: %s", cerr.Error())
	}

	// Get the last committed pk into a loggable form.
	var lastpkbuf []byte
	lastpkbuf, merr := prototext.Marshal(&querypb.QueryResult{
		Fields: pkfields,
		Rows:   []*querypb.Row{copier.LastCommittedPk()},
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

func (vc *vcopier) newCopier() vcopierCopier {
	if isExperimentalParallelBulkInsertsEnabled() {
		return newVCopierConcurrentCopier(
			func() (vcopierCopier, error) {
				dbClient, err := vc.newClientConnection()
				if err != nil {
					return nil, fmt.Errorf("failed to create new db client: %s", err.Error())
				}
				return newVCopierBasicCopier(true, vc.vr.dbClient.stats, dbClient), nil
			},
			getCopyInsertConcurrency(),
			vc.vr.stats,
		)
	}
	return newVCopierBasicCopier(false, vc.vr.dbClient.stats, vc.vr.dbClient)
}

func (vbc *vcopierBasicCopier) BeforeUpdateCopyState() *event.Hooks {
	return vbc.hooks["updateCopyState:before"]
}

func (vbc *vcopierBasicCopier) Copy(ctx context.Context, rows []*querypb.Row, lastpk *querypb.Row) error {
	start := time.Now()

	var err error

	// Begin transaction and insert rows.
	if err = vbc.vdbClient.Begin(); err != nil {
		err = fmt.Errorf("error beginning copy-update-state transaction: %s", err.Error())
		goto DONE
	}

	if _, err = vbc.insertRows(ctx, rows); err != nil {
		err = fmt.Errorf("error inserting rows: %s", err.Error())
		goto DONE
	}

	// Update copy state.
	if err = vbc.updateCopyState(ctx, lastpk); err != nil {
		err = fmt.Errorf("error updating _vt.copy_state: %s", err.Error())
		goto DONE
	}

	// Commit.
	if err = vbc.vdbClient.Commit(); err != nil {
		err = fmt.Errorf("error commiting copy-update-state transaction: %s", err.Error())
		goto DONE
	}

	vbc.lastCommittedPk = lastpk

DONE:
	// Update stats.
	elapsedTime := float64(time.Now().UnixNano()-start.UnixNano()) / 1e9
	bandwidth := int64(float64(len(rows)) / elapsedTime)
	vbc.stats.CopyBandwidth.Set(bandwidth)
	vbc.stats.CopyRowCount.Add(int64(len(rows)))
	vbc.stats.QueryCount.Add("copy", 1)

	// Handle errors.
	if err != nil {
		vbc.stats.ErrorCounts.Add([]string{"BulkCopy"}, 1)
	}

	return err
}

func (vbc *vcopierBasicCopier) Error() error {
	return nil
}

func (vbc *vcopierBasicCopier) IsOpen() bool {
	return vbc.isOpen
}

func (vbc *vcopierBasicCopier) LastCommittedPk() *querypb.Row {
	return vbc.lastCommittedPk
}

func (vbc *vcopierBasicCopier) Open(
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

func (vbc *vcopierBasicCopier) Close() {
	if !vbc.IsOpen() {
		return
	}

	if vbc.closeDbClient {
		vbc.vdbClient.Close()
	}

	vbc.isOpen = false
}

func (vbc *vcopierBasicCopier) insertRows(ctx context.Context, rows []*querypb.Row) (*sqltypes.Result, error) {
	if !vbc.IsOpen() {
		return nil, fmt.Errorf("failed to insert rows with vcopier basic copier: not open")
	}

	return vbc.tablePlan.applyBulkInsert(&vbc.sqlbuffer, rows, func(sql string) (*sqltypes.Result, error) {
		return vbc.vdbClient.ExecuteWithRetry(ctx, sql)
	})
}

func (vbc *vcopierBasicCopier) updateCopyState(ctx context.Context, lastpk *querypb.Row) error {
	if !vbc.IsOpen() {
		return fmt.Errorf("failed to update copy state with vcopioer basic copier: not open")
	}

	vbc.hooks["updateCopyState:before"].Fire()

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

func (vcc *vcopierConcurrentCopier) AfterCopy() *event.Hooks {
	panic("not implemented")
}

func (vcc *vcopierConcurrentCopier) BeforeUpdateCopyState() *event.Hooks {
	panic("not implemented")
}

func (vcc *vcopierConcurrentCopier) Copy(ctx context.Context, rows []*querypb.Row, lastpk *querypb.Row) error {
	if !vcc.IsOpen() {
		return fmt.Errorf("concurrent copier is not open")
	}

	// Set up previous and next commit channels.
	lastCommitCh := vcc.lastCommitCh
	nextCommitCh := make(chan bool, 1)

	// Get a worker from pool.
	rs, err := vcc.copierPool.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire worker from pool: %s", err.Error())
	}
	copier := rs.(vcopierCopier)

	copier.BeforeUpdateCopyState().Clear()
	copier.BeforeUpdateCopyState().Add(func() {
		if lastCommitCh != nil {
			// Wait for previous copy operation to commit.
			<-lastCommitCh
		}
	})

	vcc.lastCommitCh = nextCommitCh

	go func() {
		if err := copier.Copy(ctx, rows, lastpk); err != nil {
			vcc.errors.RecordError(err)
		}
		// Save the lastpk.
		vcc.lastCommittedPk = copier.LastCommittedPk()
		// Signal the next copy operation to commit.
		nextCommitCh <- true
		// Return the copier to the pool.
		vcc.copierPool.Put(copier)
	}()

	return vcc.Error()
}

func (vcc *vcopierConcurrentCopier) Error() error {
	if vcc.errors.HasErrors() {
		return vcc.errors.Error()
	}
	return nil
}

func (vcc *vcopierConcurrentCopier) IsOpen() bool {
	return vcc.isOpen
}

func (vcc *vcopierConcurrentCopier) LastCommittedPk() *querypb.Row {
	return vcc.lastCommittedPk
}

func (vcc *vcopierConcurrentCopier) Open(
	copyStateUpdate *sqlparser.ParsedQuery,
	pkfields []*querypb.Field,
	tablePlan *TablePlan,
) {
	if vcc.IsOpen() {
		return
	}

	vcc.copierPool = pools.NewResourcePool(
		/* factory */
		func(_ context.Context) (pools.Resource, error) {
			copier, err := vcc.copierFactory()
			if err != nil {
				return nil, fmt.Errorf("failed to create copier for resource pool: %s", err.Error())
			}
			copier.Open(copyStateUpdate, pkfields, tablePlan)
			return copier, nil
		},
		vcc.size, /* initial capacity */
		vcc.size, /* max capacity */
		0,        /* idle timeout */
		nil,      /* log wait */
		nil,      /* refresh check */
		0,        /* refresh interval */
	)

	vcc.isOpen = true
}

func (vcc *vcopierConcurrentCopier) Close() {
	if !vcc.IsOpen() {
		return
	}

	vcc.copierPool.Close()
	vcc.isOpen = false
}
