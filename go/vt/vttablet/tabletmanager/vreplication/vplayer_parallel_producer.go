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
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

const (
	countWorkers          = 6
	maxWorkerEvents       = 100
	maxIdleWorkerDuration = 100 * time.Millisecond

	aggregatePosInterval = 250 * time.Millisecond
)

type parallelProducer struct {
	vp *vplayer

	workers []*parallelWorker

	posReached                atomic.Bool
	workerErrors              chan error
	sequenceToWorkersMap      map[int64]int // sequence number => worker index
	sequenceToWorkersMapMu    sync.RWMutex
	completedSequenceNumbers  chan int64
	commitWorkerEventSequence atomic.Int64
	assignSequence            int64

	newDBClient                 func() (*vdbClient, error)
	aggregateWorkersPosQuery    string
	lastAggregatedWorkersPosStr string

	numCommits         atomic.Int64 // temporary. TODO: remove
	currentConcurrency atomic.Int64 // temporary. TODO: remove
	maxConcurrency     atomic.Int64 // temporary. TODO: remove
}

func newParallelProducer(ctx context.Context, dbClientGen dbClientGenerator, vp *vplayer) (*parallelProducer, error) {
	p := &parallelProducer{
		vp:                       vp,
		workers:                  make([]*parallelWorker, countWorkers),
		workerErrors:             make(chan error, countWorkers),
		sequenceToWorkersMap:     make(map[int64]int),
		completedSequenceNumbers: make(chan int64, countWorkers),
		aggregateWorkersPosQuery: binlogplayer.ReadVReplicationCombinedWorkersGTIDs(vp.vr.id),
	}

	p.newDBClient = func() (*vdbClient, error) {
		dbClient, err := dbClientGen()
		if err != nil {
			return nil, err
		}
		vdbClient := newVDBClient(dbClient, vp.vr.stats, 0)
		_, err = vp.vr.setSQLMode(ctx, vdbClient)
		if err != nil {
			return nil, err
		}
		vdbClient.maxBatchSize = vp.vr.dbClient.maxBatchSize
		return vdbClient, nil
	}
	for i := range p.workers {
		w := newParallelWorker(i, p, vp.vr.workflowConfig.RelayLogMaxItems)
		var err error
		if w.dbClient, err = p.newDBClient(); err != nil {
			return nil, err
		}
		w.queryFunc = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
			if !w.dbClient.InTransaction { // Should be sent down the wire immediately
				return w.dbClient.Execute(sql)
			}
			return nil, w.dbClient.AddQueryToTrxBatch(sql) // Should become part of the trx batch
		}
		w.commitFunc = func() error {
			return w.dbClient.CommitTrxQueryBatch() // Commit the current trx batch
		}
		// INSERT a row into _vt.vreplication_worker_pos with an empty position
		if _, err := w.dbClient.ExecuteFetch(binlogplayer.GenerateInitWorkerPos(vp.vr.id, w.index), -1); err != nil {
			return nil, err
		}

		p.workers[i] = w
	}

	return p, nil
}

func (p *parallelProducer) commitWorkerEvent() *binlogdatapb.VEvent {
	return &binlogdatapb.VEvent{
		Type:           binlogdatapb.VEventType_UNKNOWN,
		SequenceNumber: p.commitWorkerEventSequence.Add(-1),
	}
}

func (p *parallelProducer) assignTransactionToWorker(sequenceNumber int64, lastCommitted int64, currentWorkerIndex int, preferCurrentWorker bool) (workerIndex int) {
	p.sequenceToWorkersMapMu.RLock()
	defer p.sequenceToWorkersMapMu.RUnlock()

	if workerIndex, ok := p.sequenceToWorkersMap[sequenceNumber]; ok {
		// All events of same sequence should be executed by same worker
		return workerIndex
	}
	if workerIndex, ok := p.sequenceToWorkersMap[lastCommitted]; ok {
		// Transaction depends on another transaction, that is still being owned by some worker.
		// Use that same worker, so that this transaction is non-blocking.
		p.sequenceToWorkersMap[sequenceNumber] = workerIndex
		return workerIndex
	}
	// No specific transaction dependency constraints (any parent transactions were long since
	// committed, and no worker longer indicates it owns any such transactions)
	if preferCurrentWorker {
		// Prefer the current worker, if it has capacity. On one hand, we want
		// to batch queries as possible. On the other hand, we want to spread the
		// the load across workers.
		workerIndex = currentWorkerIndex
	} else {
		// Even if not specifically requested to assign current worker, we still
		// want to do some batching. We batch `maxWorkerEvents` events per worker.
		workerIndex = int(p.assignSequence/maxWorkerEvents) % len(p.workers)
	}
	p.assignSequence++
	p.sequenceToWorkersMap[sequenceNumber] = workerIndex
	return workerIndex
}

// commitAll commits all workers and waits for them to complete.
func (p *parallelProducer) commitAll(ctx context.Context, except *parallelWorker) error {
	var eg errgroup.Group
	for _, w := range p.workers {
		w := w
		if except != nil && w.index == except.index {
			continue
		}
		eg.Go(func() error {
			return <-w.commitEvents()
		})
	}
	return eg.Wait()
}

// updatePos updates _vt.vreplication with the given position and timestamp.
// This producer updates said position based on the aggregation of all committed workers positions.
func (p *parallelProducer) updatePos(ctx context.Context, pos replication.Position, ts int64, dbClient *vdbClient) (posReached bool, err error) {
	update := binlogplayer.GenerateUpdatePos(p.vp.vr.id, pos, time.Now().Unix(), ts, p.vp.vr.stats.CopyRowCount.Get(), p.vp.vr.workflowConfig.StoreCompressedGTID)
	if _, err := dbClient.ExecuteWithRetry(ctx, update); err != nil {
		return false, fmt.Errorf("error %v updating position", err)
	}
	// p.vp.numAccumulatedHeartbeats = 0
	// p.vp.unsavedEvent = nil
	// p.vp.timeLastSaved = time.Now()
	// p.vp.vr.stats.SetLastPosition(p.vp.pos)
	return posReached, nil
}

// updateTimeThrottled updates the time_throttled field in the _vt.vreplication record
// with a rate limit so that it's only saved in the database at most once per
// throttleUpdatesRateLimiter.tickerTime.
// It also increments the throttled count in the stats to keep track of how many
// times a VReplication workflow, and the specific sub-component, is throttled by the
// tablet throttler over time. It also increments the global throttled count to keep
// track of how many times in total vreplication has been throttled across all workflows
// (both ones that currently exist and ones that no longer do).
func (p *parallelProducer) updateTimeThrottled(appThrottled throttlerapp.Name, reasonThrottled string, dbClient *vdbClient) error {
	appName := appThrottled.String()
	p.vp.vr.stats.ThrottledCounts.Add([]string{"tablet", appName}, 1)
	globalStats.ThrottledCount.Add(1)
	err := p.vp.vr.throttleUpdatesRateLimiter.Do(func() error {
		tm := time.Now().Unix()
		update, err := binlogplayer.GenerateUpdateTimeThrottled(p.vp.vr.id, tm, appName, reasonThrottled)
		if err != nil {
			return err
		}
		if _, err := dbClient.ExecuteFetch(update, maxRows); err != nil {
			return fmt.Errorf("error %v updating time throttled", err)
		}
		return nil
	})
	return err
}

// aggregateWorkersPos aggregates the committed GTID positions of all workers, along with their transaction timestamps.
// If any change since last call is detected, the combined position is written to _vt.vreplication.
func (p *parallelProducer) aggregateWorkersPos(ctx context.Context, dbClient *vdbClient, onlyFirstContiguous bool) (aggregatedWorkersPos replication.Position, combinedPos replication.Position, err error) {
	qr, err := dbClient.ExecuteFetch(p.aggregateWorkersPosQuery, -1)
	if err != nil {
		log.Errorf("Error fetching vreplication worker positions: %v. isclosed? %v", err, dbClient.IsClosed())
		return aggregatedWorkersPos, combinedPos, err
	}
	var aggregatedWorkersPosStr string
	var lastEventTimestamp int64
	for _, row := range qr.Rows { // there's actually exactly one row in this result set
		aggregatedWorkersPosStr = row[0].ToString()
		lastEventTimestamp, err = row[1].ToInt64()
		if err != nil {
			return aggregatedWorkersPos, combinedPos, err
		}
	}
	if aggregatedWorkersPosStr == p.lastAggregatedWorkersPosStr {
		// Nothing changed since last visit. Skip all the parsing and updates.
		return aggregatedWorkersPos, combinedPos, nil
	}
	p.lastAggregatedWorkersPosStr = aggregatedWorkersPosStr
	aggregatedWorkersPos, err = binlogplayer.DecodeMySQL56Position(aggregatedWorkersPosStr)
	if err != nil {
		return aggregatedWorkersPos, combinedPos, err
	}

	if onlyFirstContiguous && aggregatedWorkersPos.GTIDSet != nil {
		// This is a performance optimization which, for the running duration, only
		// considers the first contiguous part of the aggregated GTID set.
		// The idea is that with out-of-order workers, a worker's GTID set
		// is punctured, and that means its representation is very long.
		// Consider something like:
		// 9ee2b9ca-e848-11ef-b80c-091a65af3e28:4697:4699:4702:4704:4706:4708:4710:4713:4717:4719:4723:4726:4728:4730:4732:4734:4737:4739:4742:4744:4746:4748:4751:4753:4756:4758:4760:4762:4765:4768-4769:4772-4774:...
		// Even when combined with multiple workers, it's enough that one worker is behind,
		// that the same amount of punctures exists in the aggregated GTID set.
		// What this leads to is:
		// - Longer and more complex parsing of GTID sets.
		// - More data to be sent over the wire.
		// - More data to be stored in the database. Easily surpassing VARCHAR(10000) limitation.
		// And the observation is that we don't really need all of this data _at this time_.
		// Consider: when do we stop the workflow?
		// - Either startPos is defined (and in all likelihood it is contiguous, as in 9ee2b9ca-e848-11ef-b80c-091a65af3e28:1-100000)
		// - Or sommething is running VReplicationWaitForPos (e.g. Online DDL), and again, it's contiguous.
		// So the conditions for which we wait don't care about the punctures. These are as good as "not applied"
		// when it comes to comparing to the expected terminal position.
		// However, when the workflow is stopped, we do need to know the full GTID set, so that we
		// have accurate information about what was applied and what wasn't (consider Online DDL reverts
		// that need to start at that precise position). And so when the workflow is stopped, we will
		// have onlyFirstContiguous==false.
		mysql56gtid := aggregatedWorkersPos.GTIDSet.(replication.Mysql56GTIDSet)
		for sid := range mysql56gtid {
			mysql56gtid[sid] = mysql56gtid[sid][:1]
		}
	}
	// The aggregatedWorkersPos only looks at the GTI entries actually processed in the binary logs.
	// The combinedPos also takes into account the startPos. combinedPos is what we end up storing
	// in _vt.vreplication, and it's what will be compared to stopPosition or used by VReplicationWaitForPos.
	combinedPos = replication.AppendGTIDSet(aggregatedWorkersPos, p.vp.startPos.GTIDSet)
	p.vp.pos = combinedPos // TODO(shlomi) potential for race condition

	// Update _vt.vreplication. This write reflects everything we could read from the workers table,
	// which means that data was committed by the workers, which means this is a de-factor "what's been
	// actually applied"."
	if _, err := p.updatePos(ctx, combinedPos, lastEventTimestamp, dbClient); err != nil {
		return aggregatedWorkersPos, combinedPos, err
	}
	return aggregatedWorkersPos, combinedPos, nil
}

// watchPos runs in a goroutine and is in charge of peridocially aggregating workers
// positions and writing the aggregated value to _vt.vreplication, as well as
// sending the aggregated value to the workers themselves.
func (p *parallelProducer) watchPos(ctx context.Context) error {
	dbClient, err := p.newDBClient()
	if err != nil {
		return err
	}
	defer dbClient.Close()

	aggregatePosTicker := time.NewTicker(aggregatePosInterval)
	defer aggregatePosTicker.Stop()
	// noProgressRateLimiter := timer.NewRateLimiter(time.Second)
	// defer noProgressRateLimiter.Stop()
	// var lastCombinedPos replication.Position
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-aggregatePosTicker.C:
			aggregatedWorkersPos, combinedPos, err := p.aggregateWorkersPos(ctx, dbClient, true)
			if err != nil {
				log.Errorf("Error aggregating vreplication worker positions: %v. isclosed? %v", err, dbClient.IsClosed())
				continue
			}
			if aggregatedWorkersPos.IsZero() {
				// This happens when there's been no change since last polling. It's a performance
				// optimization to save the cost of updating the position.
				log.Errorf("========== QQQ watchPos aggregatedWorkersPos is IDLE: %v", p.lastAggregatedWorkersPosStr)
				continue
			}
			log.Errorf("========== QQQ watchPos aggregatedWorkersPos: %v, combinedPos: %v, stop: %v", aggregatedWorkersPos, combinedPos, p.vp.stopPos)

			// Write back this combined pos to all workers, so that we condense their otherwise sparse GTID sets.
			for _, w := range p.workers {
				go func() { w.aggregatedPosChan <- aggregatedWorkersPos.String() }()
			}
			// log.Errorf("========== QQQ watchPos pushed combined pos")
			// if combinedPos.GTIDSet.Equal(lastCombinedPos.GTIDSet) {
			// 	// no progress has been made
			// 	err := noProgressRateLimiter.Do(func() error {
			// 		log.Errorf("========== QQQ watchPos no progress!! committing all")
			// 		return p.commitAll(ctx, nil)
			// 	})
			// 	if err != nil {
			// 		return err
			// 	}
			// } else {
			// 	// progress has been made
			// 	lastCombinedPos.GTIDSet = combinedPos.GTIDSet
			// }
			if !p.vp.stopPos.IsZero() && combinedPos.AtLeast(p.vp.stopPos) {
				if err := p.commitAll(ctx, nil); err != nil {
					return err
				}
				p.posReached.Store(true)
				return io.EOF
			}
		}
	}
}

// process is a goroutine that reads events from the input channel and assigns them to workers.
func (p *parallelProducer) process(ctx context.Context, events chan *binlogdatapb.VEvent) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case sequenceNumber := <-p.completedSequenceNumbers:
				p.sequenceToWorkersMapMu.Lock()
				delete(p.sequenceToWorkersMap, sequenceNumber)
				p.sequenceToWorkersMapMu.Unlock()
			}
		}
	}()
	currentWorker := p.workers[0]
	for {
		if p.posReached.Load() {
			return io.EOF
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-events:
			// log.Errorf("========== QQQ process event type: %v", event.Type)
			canApplyInParallel := false
			switch event.Type {
			case binlogdatapb.VEventType_BEGIN,
				// binlogdatapb.VEventType_FIELD,
				binlogdatapb.VEventType_ROW,
				binlogdatapb.VEventType_COMMIT,
				binlogdatapb.VEventType_GTID:
				// We can parallelize these events.
				canApplyInParallel = true
			case binlogdatapb.VEventType_PREVIOUS_GTIDS:
				// This `case` is not required, but let's make this very explicit:
				// The transaction dependency graph is scoped to per-binary log.
				// When rotating into a new binary log, we must wait until all
				// existing workers have completed, as there is no information
				// about dependencies cross binlogs.
				canApplyInParallel = false
			}
			if !canApplyInParallel {
				// As an example, thus could be a DDL.
				// Wait for all existing workers to complete, including the one we are about to assign to.
				if err := p.commitAll(ctx, nil); err != nil {
					return err
				}
			}
			workerIndex := p.assignTransactionToWorker(event.SequenceNumber, event.CommitParent, currentWorker.index, event.PinWorker)
			currentWorker = p.workers[workerIndex]
			select {
			case <-ctx.Done():
				return ctx.Err()
			case currentWorker.events <- event:
			}
			if !canApplyInParallel {
				// Say this was a DDL. Then we need to wait until it is absolutely complete, before we allow the next event to be processed.
				if err := <-currentWorker.commitEvents(); err != nil {
					return err
				}
			}
		}
	}
}

// applyEvents is a parallel variation on VPlayer's applyEvents() function. It spawns the necessary
// goroutines, starts the workers, processes the vents, and looks for errors.
func (p *parallelProducer) applyEvents(ctx context.Context, relay *relayLog) error {
	defer log.Errorf("========== QQQ applyEvents defer")

	dbClient, err := p.newDBClient()
	if err != nil {
		return err
	}
	defer dbClient.Close()

	go func() {
		if err := p.watchPos(ctx); err != nil {
			p.workerErrors <- err
		}
	}()
	for _, w := range p.workers {
		w := w
		go func() {
			p.workerErrors <- w.applyQueuedEvents(ctx)
		}()
	}

	estimateLag := func() {
		behind := time.Now().UnixNano() - p.vp.lastTimestampNs - p.vp.timeOffsetNs
		p.vp.vr.stats.ReplicationLagSeconds.Store(behind / 1e9)
		p.vp.vr.stats.VReplicationLags.Add(strconv.Itoa(int(p.vp.vr.id)), time.Duration(behind/1e9)*time.Second)
	}

	eventQueue := make(chan *binlogdatapb.VEvent, 1000)
	go p.process(ctx, eventQueue)

	// If we're not running, set ReplicationLagSeconds to be very high.
	// TODO(sougou): if we also stored the time of the last event, we
	// can estimate this value more accurately.
	defer p.vp.vr.stats.ReplicationLagSeconds.Store(math.MaxInt64)
	defer p.vp.vr.stats.VReplicationLags.Add(strconv.Itoa(int(p.vp.vr.id)), math.MaxInt64)
	var lagSecs int64
	for {
		if p.posReached.Load() {
			return io.EOF
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Check throttler.
		if checkResult, ok := p.vp.vr.vre.throttlerClient.ThrottleCheckOKOrWaitAppName(ctx, throttlerapp.Name(p.vp.throttlerAppName)); !ok {
			go func() {
				_ = p.updateTimeThrottled(throttlerapp.VPlayerName, checkResult.Summary(), dbClient)
				estimateLag()
			}()
			continue
		}

		items, err := relay.Fetch()
		if err != nil {
			return err
		}
		var pinWorker bool
		lagSecs = -1
		for i, events := range items {
			for j, event := range events {
				// event's GTID is singular, but we parse it as a GTIDSet
				if !p.vp.stopPos.IsZero() {
					_, eventGTID, err := replication.DecodePositionMySQL56(event.Gtid)
					if err != nil {
						return err
					}
					if !p.vp.stopPos.GTIDSet.Contains(eventGTID) {
						// This event goes beyond the stop position. We skip it.
						continue
					}
				}
				if event.Timestamp != 0 {
					// If the event is a heartbeat sent while throttled then do not update
					// the lag based on it.
					// If the batch consists only of throttled heartbeat events then we cannot
					// determine the actual lag, as the vstreamer is fully throttled, and we
					// will estimate it after processing the batch.
					if !(event.Type == binlogdatapb.VEventType_HEARTBEAT && event.Throttled) {
						p.vp.lastTimestampNs = event.Timestamp * 1e9
						p.vp.timeOffsetNs = time.Now().UnixNano() - event.CurrentTime
						lagSecs = event.CurrentTime/1e9 - event.Timestamp
					}
				}
				switch event.Type {
				case binlogdatapb.VEventType_COMMIT:
					// If we've reached the stop position, we must save the current commit
					// even if it's empty. So, the next applyEvent is invoked with the
					// mustSave flag.
					if !p.vp.stopPos.IsZero() && p.vp.pos.AtLeast(p.vp.stopPos) {
						// We break early, so we never set `event.Skippable = true`.
						// This is the equivalent of `mustSave` in sequential VPlayer
						break
					}
					// In order to group multiple commits into a single one, we look ahead for
					// the next commit. If there is one, we skip the current commit, which ends up
					// applying the next set of events as part of the current transaction. This approach
					// also handles the case where the last transaction is partial. In that case,
					// we only group the transactions with commits we've seen so far.
					if hasAnotherCommit(items, i, j+1) {
						pinWorker = true
						event.Skippable = true
						// continue
					} else {
						pinWorker = false
					}
				case binlogdatapb.VEventType_DDL, binlogdatapb.VEventType_OTHER, binlogdatapb.VEventType_JOURNAL:
					pinWorker = false
				}
				event.PinWorker = pinWorker
				select {
				case eventQueue <- event: // to be consumed by p.Process()
				case <-ctx.Done():
					return ctx.Err()
				case err := <-p.workerErrors:
					if err != io.EOF {
						p.vp.vr.stats.ErrorCounts.Add([]string{"Apply"}, 1)
						var table, tableLogMsg, gtidLogMsg string
						switch {
						case event.GetFieldEvent() != nil:
							table = event.GetFieldEvent().TableName
						case event.GetRowEvent() != nil:
							table = event.GetRowEvent().TableName
						}
						if table != "" {
							tableLogMsg = fmt.Sprintf(" for table %s", table)
						}
						pos := getNextPosition(items, i, j+1)
						if pos != "" {
							gtidLogMsg = fmt.Sprintf(" while processing position %s", pos)
						}
						log.Errorf("Error applying event%s%s: %s", tableLogMsg, gtidLogMsg, err.Error())
						err = vterrors.Wrapf(err, "error applying event%s%s", tableLogMsg, gtidLogMsg)
					}
					return err
				}
			}
		}

		if lagSecs >= 0 {
			p.vp.vr.stats.ReplicationLagSeconds.Store(lagSecs)
			p.vp.vr.stats.VReplicationLags.Add(strconv.Itoa(int(p.vp.vr.id)), time.Duration(lagSecs)*time.Second)
		} else { // We couldn't determine the lag, so we need to estimate it
			estimateLag()
		}
	}
}
