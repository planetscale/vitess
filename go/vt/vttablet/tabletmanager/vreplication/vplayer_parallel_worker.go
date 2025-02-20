/*
Copyright 2025 The Vitess Authors.
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
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

type parallelWorker struct {
	index             int
	dbClient          *vdbClient
	queryFunc         func(ctx context.Context, sql string) (*sqltypes.Result, error)
	commitFunc        func() error
	vp                *vplayer
	aggregatedPosChan chan string

	producer *parallelProducer

	events              chan *binlogdatapb.VEvent
	stats               *VrLogStats
	sequenceNumbers     []int64
	commitSubscribers   map[int64]chan error // subscribing to commit events
	commitSubscribersMu sync.RWMutex

	// foreignKeyChecksEnabled is the current state of the foreign key checks for the current session.
	// It reflects what we have set the @@session.foreign_key_checks session variable to.
	foreignKeyChecksEnabled bool
	// foreignKeyChecksStateInitialized is set to true once we have initialized the foreignKeyChecksEnabled.
	// The initialization is done on the first row event that this vplayer sees.
	foreignKeyChecksStateInitialized bool

	// TODO(shlomi): remove this
	numCommits    int
	numSubscribes int

	updatedPos          replication.Position
	updatedPosTimestamp int64
}

func newParallelWorker(index int, producer *parallelProducer, capacity int) *parallelWorker {
	log.Errorf("======= QQQ newParallelWorker index: %v", index)
	return &parallelWorker{
		index:             index,
		producer:          producer,
		events:            make(chan *binlogdatapb.VEvent, capacity),
		aggregatedPosChan: make(chan string),
		sequenceNumbers:   make([]int64, maxWorkerEvents),
		commitSubscribers: make(map[int64]chan error),
		vp:                producer.vp,
	}
}

func (w *parallelWorker) subscribeCommitWorkerEvent(sequenceNumber int64) chan error {
	w.commitSubscribersMu.Lock()
	defer w.commitSubscribersMu.Unlock()

	w.numSubscribes++
	c := make(chan error, 1)
	w.commitSubscribers[sequenceNumber] = c
	return c
}

// updatePos should get called at a minimum of vreplicationMinimumHeartbeatUpdateInterval.
func (w *parallelWorker) updatePos(ctx context.Context, posStr string, transactionTimestamp int64, singleGTID bool) (posReached bool, err error) {
	if w.dbClient.InTransaction {
		// We're assuming there's multiple calls to updatePos within this
		// transaction. We don't write them at this time. Instead, we
		// aggregate the given positions and write them in the commit.
		if singleGTID {
			// Faster to ParseMysql56GTID than DecodeMySQL56Position when it's just the one entry
			gtid, err := replication.ParseMysql56GTID(posStr)
			if err != nil {
				return false, err
			}
			w.updatedPos = replication.AppendGTIDInPlace(w.updatedPos, gtid)
		} else {
			pos, err := binlogplayer.DecodeMySQL56Position(posStr)
			if err != nil {
				return false, err
			}
			w.updatedPos = replication.AppendGTIDSetInPlace(w.updatedPos, pos.GTIDSet)
		}
		w.updatedPosTimestamp = max(w.updatedPosTimestamp, transactionTimestamp)
		return false, nil
	}
	update := binlogplayer.GenerateUpdateWorkerPos(w.vp.vr.id, w.index, posStr, transactionTimestamp)
	if _, err := w.queryFunc(ctx, update); err != nil {
		return false, fmt.Errorf("error updating position: %v", err)
	}
	// TODO (shlomi): handle these
	// vp.numAccumulatedHeartbeats = 0
	// vp.unsavedEvent = nil
	// vp.timeLastSaved = time.Now()
	// vp.vr.stats.SetLastPosition(vp.pos)

	return posReached, nil
}

func (w *parallelWorker) updatePosByEvent(ctx context.Context, event *binlogdatapb.VEvent) error {
	if _, err := w.updatePos(ctx, event.EventGtid, event.Timestamp, true); err != nil {
		return err
	}
	return nil
}

func (w *parallelWorker) commitEvents() chan error {
	event := w.producer.commitWorkerEvent()
	log.Errorf("========== QQQ commitEvents: %v", event)
	c := w.subscribeCommitWorkerEvent(event.SequenceNumber)
	log.Errorf("========== QQQ commitEvents: subscribed to %v in worker %v", event.SequenceNumber, w.index)
	w.events <- event
	log.Errorf("========== QQQ commitEvents: pushed event")
	return c
}

func (w *parallelWorker) applyQueuedEvents(ctx context.Context) (err error) {
	log.Errorf("========== QQQ applyQueuedEvents")
	defer func() {
		log.Errorf("========== QQQ applyQueuedEvents *********** DONE %v *********** err=%v", w.index, err)
	}()

	defer func() {
		// Anything that's not committed should be rolled back
		w.dbClient.Rollback()
	}()

	defer func() {
		log.Errorf("========== QQQ applyQueuedEvents worker %v num commits=%v, numSubscribes=%v", w.index, w.numCommits, w.numSubscribes)
	}()

	ticker := time.NewTicker(maxIdleWorkerDuration / 2)
	defer ticker.Stop()

	var lastTickerTime time.Time
	var lastAppliedTickerTime time.Time
	var lastEventWasSkippedCommit bool

	applyEvent := func(event *binlogdatapb.VEvent) error {
		lastEventWasSkippedCommit = false
		if err := w.applyQueuedEvent(ctx, event); err != nil {
			return err
		}
		lastAppliedTickerTime = lastTickerTime
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case lastTickerTime = <-ticker.C:
			if lastEventWasSkippedCommit && lastTickerTime.Sub(lastAppliedTickerTime) >= maxIdleWorkerDuration {
				// The last event was a commit, which we did nto actually apply, as we figured we'd
				// follow up with more statements. But it's been a while and there's been no statement since.
				// So we're just sitting idly with a bunch of uncommitted statements. Better to commit now.
				log.Errorf("========== QQQ applyQueuedEvents worker %v idle with skipped commit and %v events. COMMITTING", w.index, len(w.sequenceNumbers))
				if err := applyEvent(w.producer.commitWorkerEvent()); err != nil {
					return err
				}
			}
		case pos := <-w.aggregatedPosChan:
			if _, err := w.updatePos(ctx, pos, 0, false); err != nil {
				return err
			}
		case event := <-w.events:
			if event.SequenceNumber >= 0 {
				// Negative values are happen in commitWorkerEvent(). These are not real events.
				w.sequenceNumbers = append(w.sequenceNumbers, event.SequenceNumber)
			}

			if isConsiderCommitWorkerEvent(event) {
				if !lastEventWasSkippedCommit {
					continue
				}
				log.Errorf("========== QQQ applyQueuedEvents worker %v idle with isConsiderCommitWorkerEvent commit and %v events. COMMITTING", w.index, len(w.sequenceNumbers))
			}

			skippable := func() bool {
				if event.Type != binlogdatapb.VEventType_COMMIT {
					return false
				}
				if event.Skippable {
					// At this time only COMMIT events are Skippable, so checking for the type is not
					// strictly necessary. But it's safer to add that check.
					return true
				}
				if len(w.sequenceNumbers) < maxWorkerEvents {
					// We don't want to commit yet. We're waiting for more events.
					return true
				}
				return false
			}

			if skippable() {
				// At this time only COMMIT events are Skippable, so checking for the type is not
				// strictly necessary. But it's safer to add that check.
				lastEventWasSkippedCommit = true
				continue
			}
			if err := applyEvent(event); err != nil {
				return err
			}
		}
	}
}

func (w *parallelWorker) applyQueuedEvent(ctx context.Context, event *binlogdatapb.VEvent) error {
	// log.Errorf("========== QQQ applyQueuedEvent, START worker=%v, event.Type=%v", w.index, event.Type)
	// defer log.Errorf("========== QQQ applyQueuedEvent, DONE  worker=%v, event.Type=%v", w.index, event.Type)
	// ctx, cancel := context.WithCancel(ctx)
	// defer cancel()

	// //
	// t := time.NewTimer(5 * time.Second)
	// defer t.Stop()
	// go func() {
	// 	select {
	// 	case <-t.C:
	// 		log.Errorf("========== QQQ applyQueuedEvent worker %v TIMED OUT. event=%v", w.index, event.Type)
	// 		if event.Type == binlogdatapb.VEventType_ROW {
	// 			log.Errorf("========== QQQ applyQueuedEvent worker %v TIMED OUT. event=%v. table=%v", w.index, event.Type, event.RowEvent.TableName)
	// 		}
	// 	case <-ctx.Done():
	// 		return
	// 	}
	// }()

	stats := NewVrLogStats(event.Type.String())
	switch event.Type {
	case binlogdatapb.VEventType_GTID:
		if err := w.updatePosByEvent(ctx, event); err != nil {
			return err
		}
		return nil
	case binlogdatapb.VEventType_BEGIN:
		// No-op: begin is called as needed.
	case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_UNKNOWN:
		return w.applyQueuedCommit(ctx, event)
	case binlogdatapb.VEventType_FIELD:
		if err := w.dbClient.Begin(); err != nil {
			return err
		}
		onField := func() error {
			w.vp.planMu.Lock()
			defer w.vp.planMu.Unlock()

			tplan, err := w.vp.replicatorPlan.buildExecutionPlan(event.FieldEvent)
			if err != nil {
				return err
			}

			w.vp.tablePlans[event.FieldEvent.TableName] = tplan
			return nil
		}
		if err := onField(); err != nil {
			return err
		}
		go func() { stats.Send(fmt.Sprintf("%v", event.FieldEvent)) }()

	case binlogdatapb.VEventType_INSERT, binlogdatapb.VEventType_DELETE, binlogdatapb.VEventType_UPDATE,
		binlogdatapb.VEventType_REPLACE, binlogdatapb.VEventType_SAVEPOINT:
		// use event.Statement if available, preparing for deprecation in 8.0
		sql := event.Statement
		if sql == "" {
			sql = event.Dml
		}
		// If the event is for one of the AWS RDS "special" or pt-table-checksum tables, we skip
		if !strings.Contains(sql, " mysql.rds_") && !strings.Contains(sql, " percona.checksums") {
			// This is a player using statement based replication
			if err := w.dbClient.Begin(); err != nil {
				return err
			}
			if err := w.applyQueuedStmtEvent(ctx, event); err != nil {
				return err
			}
			go stats.Send(sql)
		}
	case binlogdatapb.VEventType_ROW:
		if err := w.dbClient.Begin(); err != nil {
			return err
		}
		if err := w.applyQueuedRowEvent(ctx, event); err != nil {
			return err
		}
		// Row event is logged AFTER RowChanges are applied so as to calculate the total elapsed
		// time for the Row event.
		go func() { stats.Send(fmt.Sprintf("%v", event.RowEvent)) }()
	case binlogdatapb.VEventType_OTHER:
		if w.dbClient.InTransaction {
			// Unreachable
			log.Errorf("internal error: vplayer is in a transaction on event: %v", event)
			return fmt.Errorf("internal error: vplayer is in a transaction on event: %v", event)
		}
		// Just update the position.
		if err := w.updatePosByEvent(ctx, event); err != nil {
			return err
		}
	case binlogdatapb.VEventType_DDL:
		if w.dbClient.InTransaction {
			// Unreachable
			log.Errorf("internal error: vplayer is in a transaction on event: %v", event)
			return fmt.Errorf("internal error: vplayer is in a transaction on event: %v", event)
		}
		w.vp.vr.stats.DDLEventActions.Add(w.vp.vr.source.OnDdl.String(), 1) // Record the DDL handling
		switch w.vp.vr.source.OnDdl {
		case binlogdatapb.OnDDLAction_IGNORE:
			// We still have to update the position.
			if err := w.updatePosByEvent(ctx, event); err != nil {
				return err
			}
		case binlogdatapb.OnDDLAction_STOP:
			if err := w.dbClient.Begin(); err != nil {
				return err
			}
			if err := w.updatePosByEvent(ctx, event); err != nil {
				return err
			}
			if err := w.setVRState(binlogdatapb.VReplicationWorkflowState_Stopped, fmt.Sprintf("Stopped at DDL %s", event.Statement)); err != nil {
				return err
			}
			if err := w.commitFunc(); err != nil {
				return err
			}
			return io.EOF
		case binlogdatapb.OnDDLAction_EXEC:
			// It's impossible to save the position transactionally with the statement.
			// So, we apply the DDL first, and then save the position.
			// Manual intervention may be needed if there is a partial
			// failure here.
			if _, err := w.queryFunc(ctx, event.Statement); err != nil {
				return err
			}
			go func() { stats.Send(fmt.Sprintf("%v", event.Statement)) }()
			if err := w.updatePosByEvent(ctx, event); err != nil {
				return err
			}
		case binlogdatapb.OnDDLAction_EXEC_IGNORE:
			if _, err := w.queryFunc(ctx, event.Statement); err != nil {
				log.Infof("Ignoring error: %v for DDL: %s", err, event.Statement)
			}
			go func() { stats.Send(fmt.Sprintf("%v", event.Statement)) }()
			if err := w.updatePosByEvent(ctx, event); err != nil {
				return err
			}
		}
	case binlogdatapb.VEventType_JOURNAL:
		if w.dbClient.InTransaction {
			// Unreachable
			log.Errorf("internal error: vplayer is in a transaction on event: %v", event)
			return fmt.Errorf("internal error: vplayer is in a transaction on event: %v", event)
		}
		// Ensure that we don't have a partial set of table matches in the journal.
		switch event.Journal.MigrationType {
		case binlogdatapb.MigrationType_SHARDS:
			// All tables of the source were migrated. So, no validation needed.
		case binlogdatapb.MigrationType_TABLES:
			// Validate that all or none of the tables are in the journal.
			jtables := make(map[string]bool)
			for _, table := range event.Journal.Tables {
				jtables[table] = true
			}
			found := false
			notFound := false
			for tableName := range w.vp.replicatorPlan.TablePlans {
				if _, ok := jtables[tableName]; ok {
					found = true
				} else {
					notFound = true
				}
			}
			switch {
			case found && notFound:
				// Some were found and some were not found. We can't handle this.
				if err := w.setVRState(binlogdatapb.VReplicationWorkflowState_Stopped, "unable to handle journal event: tables were partially matched"); err != nil {
					return err
				}
				return io.EOF
			case notFound:
				// None were found. Ignore journal.
				return nil
			}
			// All were found. We must register journal.
		}
		log.Infof("Binlog event registering journal event %+v", event.Journal)
		if err := w.vp.vr.vre.registerJournal(event.Journal, w.vp.vr.id); err != nil {
			if err := w.setVRState(binlogdatapb.VReplicationWorkflowState_Stopped, err.Error()); err != nil {
				return err
			}
			return io.EOF
		}
		go func() { stats.Send(fmt.Sprintf("%v", event.Journal)) }()
		return io.EOF
	case binlogdatapb.VEventType_HEARTBEAT:
		if event.Throttled {
			if err := w.vp.vr.updateTimeThrottled(throttlerapp.VStreamerName, event.ThrottledReason); err != nil {
				return err
			}
		}
		if !w.dbClient.InTransaction {
			w.vp.numAccumulatedHeartbeats++
			if err := w.vp.recordHeartbeat(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *parallelWorker) setVRState(state binlogdatapb.VReplicationWorkflowState, message string) error {
	// TODO (shlomi): handle race conditions in vr.state
	if message != "" {
		w.vp.vr.stats.History.Add(&binlogplayer.StatsHistoryRecord{
			Time:    time.Now(),
			Message: message,
		})
	}
	w.vp.vr.stats.State.Store(state.String())
	query := fmt.Sprintf("update _vt.vreplication set state='%v', message=%v where id=%v", state, encodeString(binlogplayer.MessageTruncate(message)), w.vp.vr.id)
	// If we're batching a transaction, then include the state update
	// in the current transaction batch.
	dbClient := w.dbClient
	if dbClient.InTransaction && dbClient.maxBatchSize > 0 {
		dbClient.AddQueryToTrxBatch(query)
	} else { // Otherwise, send it down the wire
		if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
			return fmt.Errorf("could not set state: %v: %v", query, err)
		}
	}
	if state == w.vp.vr.state {
		return nil
	}
	insertLog(dbClient, LogStateChange, w.vp.vr.id, state.String(), message)
	w.vp.vr.state = state

	return nil
}

// applyQueuedStmtEvent applies an actual DML statement received from the source, directly onto the backend database
func (w *parallelWorker) applyQueuedStmtEvent(ctx context.Context, event *binlogdatapb.VEvent) error {
	vp := w.vp
	sql := event.Statement
	if sql == "" {
		sql = event.Dml
	}
	if event.Type == binlogdatapb.VEventType_SAVEPOINT || vp.canAcceptStmtEvents {
		start := time.Now()
		_, err := w.queryFunc(ctx, sql)
		vp.vr.stats.QueryTimings.Record(vp.phase, start)
		vp.vr.stats.QueryCount.Add(vp.phase, 1)
		return err
	}
	return fmt.Errorf("filter rules are not supported for SBR replication: %v", vp.vr.source.Filter.GetRules())
}

// updateFKCheck updates the @@session.foreign_key_checks variable based on the binlog row event flags.
// The function only does it if it has changed to avoid redundant updates, using the cached vplayer.foreignKeyChecksEnabled
// The foreign_key_checks value for a transaction is determined by the 2nd bit (least significant) of the flags:
// - If set (1), foreign key checks are disabled.
// - If unset (0), foreign key checks are enabled.
// updateFKCheck also updates the state for the first row event that this vplayer, and hence the db connection, sees.
func (w *parallelWorker) updateFKCheck(ctx context.Context, flags2 uint32) error {
	mustUpdate := false
	if w.vp.vr.WorkflowSubType == int32(binlogdatapb.VReplicationWorkflowSubType_AtomicCopy) {
		// If this is an atomic copy, we must update the foreign_key_checks state even when the vplayer runs during
		// the copy phase, i.e., for catchup and fastforward.
		mustUpdate = true
	} else if w.vp.vr.state == binlogdatapb.VReplicationWorkflowState_Running {
		// If the vreplication workflow is in Running state, we must update the foreign_key_checks
		// state for all workflow types.
		mustUpdate = true
	}
	if !mustUpdate {
		return nil
	}
	dbForeignKeyChecksEnabled := true
	if flags2&NoForeignKeyCheckFlagBitmask == NoForeignKeyCheckFlagBitmask {
		dbForeignKeyChecksEnabled = false
	}

	if w.foreignKeyChecksStateInitialized /* already set earlier */ &&
		dbForeignKeyChecksEnabled == w.foreignKeyChecksEnabled /* no change in the state, no need to update */ {
		return nil
	}
	log.Infof("Setting this session's foreign_key_checks to %s", strconv.FormatBool(dbForeignKeyChecksEnabled))
	if _, err := w.queryFunc(ctx, "set @@session.foreign_key_checks="+strconv.FormatBool(dbForeignKeyChecksEnabled)); err != nil {
		return fmt.Errorf("failed to set session foreign_key_checks: %w", err)
	}
	w.foreignKeyChecksEnabled = dbForeignKeyChecksEnabled
	if !w.foreignKeyChecksStateInitialized {
		log.Infof("First foreign_key_checks update to: %s", strconv.FormatBool(dbForeignKeyChecksEnabled))
		w.foreignKeyChecksStateInitialized = true
	}
	return nil
}

func (w *parallelWorker) applyQueuedRowEvent(ctx context.Context, vevent *binlogdatapb.VEvent) error {
	if err := w.updateFKCheck(ctx, vevent.RowEvent.Flags); err != nil {
		return err
	}
	var tplan *TablePlan
	func() {
		w.vp.planMu.RLock()
		defer w.vp.planMu.RUnlock()
		tplan = w.vp.tablePlans[vevent.RowEvent.TableName]
	}()
	if tplan == nil {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "unexpected event on table %s that has no plan yet", vevent.RowEvent.TableName)
	}
	applyFunc := func(sql string) (*sqltypes.Result, error) {
		stats := NewVrLogStats("ROWCHANGE")
		start := time.Now()
		qr, err := w.queryFunc(ctx, sql)
		w.vp.vr.stats.QueryCount.Add(w.vp.phase, 1)
		w.vp.vr.stats.QueryTimings.Record(w.vp.phase, start)
		go stats.Send(sql)
		return qr, err
	}

	rowEvent := vevent.RowEvent
	if w.vp.batchMode && len(rowEvent.RowChanges) > 1 {
		// If we have multiple delete row events for a table with a single PK column
		// then we can perform a simple bulk DELETE using an IN clause.
		if (rowEvent.RowChanges[0].Before != nil && rowEvent.RowChanges[0].After == nil) &&
			tplan.MultiDelete != nil {
			_, err := tplan.applyBulkDeleteChanges(rowEvent.RowChanges, applyFunc, w.dbClient.maxBatchSize)
			return err
		}
		// If we're done with the copy phase then we will be replicating all INSERTS
		// regardless of the PK value and can use a single INSERT statment with
		// multiple VALUES clauses.
		// TODO(shlomi): race condition over w.vp.copyState
		if len(w.vp.copyState) == 0 && (rowEvent.RowChanges[0].Before == nil && rowEvent.RowChanges[0].After != nil) {
			_, err := tplan.applyBulkInsertChanges(rowEvent.RowChanges, applyFunc, w.dbClient.maxBatchSize)
			return err
		}
	}
	{
		// Measure parallel vplayer concurrency (TODO(shlomi): remove)
		currentConcurrency := w.producer.currentConcurrency.Add(1)
		defer w.producer.currentConcurrency.Add(-1)
		if currentConcurrency > w.producer.maxConcurrency.Load() {
			w.producer.maxConcurrency.Store(currentConcurrency)
		}
	}

	for _, change := range vevent.RowEvent.RowChanges {
		if _, err := tplan.applyChange(change, applyFunc); err != nil {
			return err
		}
	}
	return nil
}

func (w *parallelWorker) applyQueuedCommit(ctx context.Context, event *binlogdatapb.VEvent) error {
	switch {
	case event.Type == binlogdatapb.VEventType_COMMIT:
	case isCommitWorkerEvent(event):
	default:
		// Not a commit
		return nil
	}
	// As a very simple optimization, we will only commit if we have any events at all to commit.
	shouldActuallyCommit := len(w.sequenceNumbers) > 0
	var err error
	if shouldActuallyCommit {
		if !w.updatedPos.IsZero() {
			update := binlogplayer.GenerateUpdateWorkerPos(w.vp.vr.id, w.index, w.updatedPos.String(), w.updatedPosTimestamp)
			if _, err := w.queryFunc(ctx, update); err != nil {
				return err
			}
			w.updatedPos = replication.Position{}
		}
		err = w.commitFunc()
	}
	func() {
		// Notify subsribers of commit event
		if event.SequenceNumber >= 0 {
			// Not a subscribed event
			return
		}
		w.commitSubscribersMu.Lock()
		defer w.commitSubscribersMu.Unlock()
		if subs, ok := w.commitSubscribers[event.SequenceNumber]; ok {
			subs <- err
			delete(w.commitSubscribers, event.SequenceNumber)
		}
	}()
	if err != nil {
		return err
	}
	// Commit successful
	if shouldActuallyCommit {
		// Parallel VPlayer metrics. TODO(shlomi): remove
		w.producer.numCommits.Add(1)
		w.numCommits++
	}
	// We now let the producer know that we've completed the sequence numbers.
	// It will deassign these sequence numebrs from the worker.
	for _, sequenceNumber := range w.sequenceNumbers {
		w.producer.completedSequenceNumbers <- sequenceNumber
	}
	w.sequenceNumbers = w.sequenceNumbers[:0]
	if w.producer.posReached.Load() {
		return io.EOF
	}
	return nil
}
