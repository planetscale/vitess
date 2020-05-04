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

package tabletserver

import (
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/dbconfigs"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/dtids"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txlimiter"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type txEngineState int

// The TxEngine can be in any of these states
const (
	NotServing txEngineState = iota
	Transitioning
	AcceptingReadAndWrite
	AcceptingReadOnly
)

func (state txEngineState) String() string {
	names := [...]string{
		"NotServing",
		"Transitioning",
		"AcceptReadWrite",
		"AcceptingReadOnly"}

	if state < NotServing || state > AcceptingReadOnly {
		return fmt.Sprintf("Unknown - %d", int(state))
	}

	return names[state]
}

// IConnectionResourceHandler is what the tx engine needs to do it's work
type IConnectionResourceHandler interface {
	ClaimExclusiveConnection(ctx context.Context, options *querypb.ExecuteOptions) (*ExclusiveConn, error)

	// Open allows new connections to be available
	Open(appParams, dbaParams, appDebugParams dbconfigs.Connector)
	Close()
	GetOutdated(age time.Duration, purpose string) []*ExclusiveConn
	WaitForEmpty()
}

// TxEngine is responsible for handling the tx-pool and keeping read-write, read-only or not-serving
// states. It will start and shut down the underlying tx-pool as required.
// It does this in a concurrently safe way.
type TxEngine struct {
	env tabletenv.Env
	// the following four fields are interconnected. `state` and `nextState` should be protected by the
	// `stateLock`
	//
	// `nextState` is used when state is Transitioning. This means that in order to change the state of
	// the transaction engine, we had to close transactions. `nextState` is the state we'll end up in
	// once the transactions are closed
	// while transitioning, `transitionSignal` will contain an open channel. Once the transition is
	// over, the channel is closed to signal to any waiting goroutines that the state change is done.
	stateLock        sync.Mutex
	state            txEngineState
	nextState        txEngineState
	transitionSignal chan struct{}

	// beginRequests is used to make sure that we do not make a state
	// transition while creating new transactions
	beginRequests sync.WaitGroup

	twopcEnabled        bool
	shutdownGracePeriod time.Duration
	coordinatorAddress  string
	abandonAge          time.Duration
	twoPCticks          *timer.Timer

	txPool       *ConnectionResourceHandler
	preparedPool *TxPreparedPool
	twoPC        *TwoPC

	limiter            txlimiter.TxLimiter
	connHandler        IConnectionResourceHandler
	transactionTimeout sync2.AtomicDuration
	txTicks            *timer.Timer
}

type queries struct {
	setIsolationLevel string
	openTransaction   string
}

var txIsolations = map[querypb.ExecuteOptions_TransactionIsolation]queries{
	querypb.ExecuteOptions_DEFAULT:                       {setIsolationLevel: "", openTransaction: "begin"},
	querypb.ExecuteOptions_REPEATABLE_READ:               {setIsolationLevel: "REPEATABLE READ", openTransaction: "begin"},
	querypb.ExecuteOptions_READ_COMMITTED:                {setIsolationLevel: "READ COMMITTED", openTransaction: "begin"},
	querypb.ExecuteOptions_READ_UNCOMMITTED:              {setIsolationLevel: "READ UNCOMMITTED", openTransaction: "begin"},
	querypb.ExecuteOptions_SERIALIZABLE:                  {setIsolationLevel: "SERIALIZABLE", openTransaction: "begin"},
	querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY: {setIsolationLevel: "REPEATABLE READ", openTransaction: "start transaction with consistent snapshot, read only"},
}

// These consts identify how a transaction was resolved.
const (
	TxClose    = "close"
	TxCommit   = "commit"
	TxRollback = "rollback"
	TxKill     = "kill"
)

const txLogInterval = 1 * time.Minute

// NewTxEngine creates a new TxEngine.
func NewTxEngine(env tabletenv.Env) *TxEngine {
	config := env.Config()
	te := &TxEngine{
		env:                 env,
		shutdownGracePeriod: time.Duration(config.ShutdownGracePeriodSeconds * 1e9),
	}
	limiter := txlimiter.New(env)
	transactionTimeout := time.Duration(config.Oltp.TxTimeoutSeconds * 1e9)
	te.transactionTimeout = sync2.NewAtomicDuration(transactionTimeout)
	te.txTicks = timer.NewTimer(transactionTimeout / 10)

	// Careful: conns also exports name+"xxx" vars,
	// but we know it doesn't export Timeout.
	env.Exporter().NewGaugeDurationFunc("TransactionTimeout", "Transaction timeout", te.transactionTimeout.Get)

	te.txPool = NewTxPool(env, limiter)
	te.twopcEnabled = config.TwoPCEnable
	if te.twopcEnabled {
		if config.TwoPCCoordinatorAddress == "" {
			log.Error("Coordinator address not specified: Disabling 2PC")
			te.twopcEnabled = false
		}
		if config.TwoPCAbandonAge <= 0 {
			log.Error("2PC abandon age not specified: Disabling 2PC")
			te.twopcEnabled = false
		}
	}
	te.coordinatorAddress = config.TwoPCCoordinatorAddress
	te.abandonAge = time.Duration(config.TwoPCAbandonAge * 1e9)
	te.twoPCticks = timer.NewTimer(te.abandonAge / 2)

	// Set the prepared pool capacity to something lower than
	// tx pool capacity. Those spare connections are needed to
	// perform metadata state change operations. Without this,
	// the system can deadlock if all connections get moved to
	// the TxPreparedPool.
	te.preparedPool = NewTxPreparedPool(config.TxPool.Size - 2)
	readPool := connpool.NewPool(env, "TxReadPool", tabletenv.ConnPoolConfig{
		Size:               3,
		IdleTimeoutSeconds: env.Config().TxPool.IdleTimeoutSeconds,
	})
	te.twoPC = NewTwoPC(readPool)
	te.transitionSignal = make(chan struct{})
	// By immediately closing this channel, all state changes can simply be made blocking by issuing the
	// state change desired, and then selecting on this channel. It will contain an open channel while
	// transitioning.
	close(te.transitionSignal)
	te.nextState = -1
	te.state = NotServing
	return te
}

// Stop will stop accepting any new transactions. Transactions are immediately aborted.
func (te *TxEngine) Stop() error {
	te.beginRequests.Wait()
	te.stateLock.Lock()

	switch te.state {
	case NotServing:
		// Nothing to do. We are already stopped or stopping
		te.stateLock.Unlock()
		return nil

	case AcceptingReadAndWrite:
		return te.transitionTo(NotServing)

	case AcceptingReadOnly:
		// We are not master, so it's safe to kill all read-only transactions
		te.close(true)
		te.state = NotServing
		te.stateLock.Unlock()
		return nil

	case Transitioning:
		te.nextState = NotServing
		te.stateLock.Unlock()
		te.blockUntilEndOfTransition()
		return nil

	default:
		te.stateLock.Unlock()
		return te.unknownStateError()
	}
}

// AcceptReadWrite will start accepting all transactions.
// If transitioning from RO mode, transactions might need to be
// rolled back before new transactions can be accepts.
func (te *TxEngine) AcceptReadWrite() error {
	te.beginRequests.Wait()
	te.stateLock.Lock()

	switch te.state {
	case AcceptingReadAndWrite:
		// Nothing to do
		te.stateLock.Unlock()
		return nil

	case NotServing:
		te.state = AcceptingReadAndWrite
		te.open()
		te.stateLock.Unlock()
		return nil

	case Transitioning:
		te.nextState = AcceptingReadAndWrite
		te.stateLock.Unlock()
		te.blockUntilEndOfTransition()
		return nil

	case AcceptingReadOnly:
		// We need to restart the tx-pool to make sure we handle 2PC correctly
		te.close(true)
		te.state = AcceptingReadAndWrite
		te.open()
		te.stateLock.Unlock()
		return nil

	default:
		return te.unknownStateError()
	}
}

// AcceptReadOnly will start accepting read-only transactions, but not full read and write transactions.
// If the engine is currently accepting full read and write transactions, they need to
// be rolled back.
func (te *TxEngine) AcceptReadOnly() error {
	te.beginRequests.Wait()
	te.stateLock.Lock()
	switch te.state {
	case AcceptingReadOnly:
		// Nothing to do
		te.stateLock.Unlock()
		return nil

	case NotServing:
		te.state = AcceptingReadOnly
		te.open()
		te.stateLock.Unlock()
		return nil

	case AcceptingReadAndWrite:
		return te.transitionTo(AcceptingReadOnly)

	case Transitioning:
		te.nextState = AcceptingReadOnly
		te.stateLock.Unlock()
		te.blockUntilEndOfTransition()
		return nil

	default:
		te.stateLock.Unlock()
		return te.unknownStateError()
	}
}

// Begin begins a transaction, and returns the associated transaction id and
// the statements (if any) executed to initiate the transaction. In autocommit
// mode the statement will be "".
//
// Subsequent statements can access the connection through the transaction id.
func (te *TxEngine) Begin(ctx context.Context, options *querypb.ExecuteOptions) (*ExclusiveConn, string, error) {
	span, ctx := trace.NewSpan(ctx, "TxEngine.Begin")
	defer span.Finish()
	var conn *ExclusiveConn
	immediateCaller := callerid.ImmediateCallerIDFromContext(ctx)
	effectiveCaller := callerid.EffectiveCallerIDFromContext(ctx)

	if !te.limiter.Get(immediateCaller, effectiveCaller) {
		return nil, "", vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "per-user transaction pool connection limit exceeded")
	}

	var beginSucceeded bool
	defer func() {
		if beginSucceeded {
			return
		}

		if conn != nil {
			conn.Recycle()
		}
		te.limiter.Release(immediateCaller, effectiveCaller)
	}()

	conn, err := te.connHandler.ClaimExclusiveConnection(ctx, options)
	if err != nil {
		return nil, "", err
	}
	conn.EffectiveCallerID = effectiveCaller
	conn.ImmediateCallerID = immediateCaller

	autocommitTransaction := false
	beginQueries := ""
	if queries, ok := txIsolations[options.GetTransactionIsolation()]; ok {
		if queries.setIsolationLevel != "" {
			if _, err := conn.Exec(ctx, "set transaction isolation level "+queries.setIsolationLevel, 1, false); err != nil {
				return nil, "", err
			}

			beginQueries = queries.setIsolationLevel + "; "
		}

		if _, err := conn.Exec(ctx, queries.openTransaction, 1, false); err != nil {
			return nil, "", err
		}
		beginQueries = beginQueries + queries.openTransaction
	} else if options.GetTransactionIsolation() == querypb.ExecuteOptions_AUTOCOMMIT {
		autocommitTransaction = true
	} else {
		return nil, "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "don't know how to open a transaction of this type: %v", options.GetTransactionIsolation())
	}

	conn.Autocommit = autocommitTransaction
	beginSucceeded = true

	return conn, beginQueries, nil
}

// Commit commits the specified transaction.
func (te *TxEngine) Commit(ctx context.Context, conn *ExclusiveConn) (string, error) {
	span, ctx := trace.NewSpan(ctx, "TxEngine.Commit")
	defer span.Finish()
	defer conn.release(TxCommit, "transaction committed")

	if conn.Autocommit {
		return "", nil
	}

	if _, err := conn.Exec(ctx, "commit", 1, false); err != nil {
		conn.Close()
		return "", err
	}
	return "commit", nil
}

func (te *TxEngine) unknownStateError() error {
	return vterrors.Errorf(vtrpc.Code_INTERNAL, "unknown state %v", te.state)
}

func (te *TxEngine) blockUntilEndOfTransition() error {
	<-te.transitionSignal
	return nil
}

func (te *TxEngine) transitionTo(nextState txEngineState) error {
	te.state = Transitioning
	te.nextState = nextState
	te.transitionSignal = make(chan struct{})
	te.stateLock.Unlock()

	// We do this outside the lock so others can see our state while we close up waiting transactions
	te.close(true)

	te.stateLock.Lock()
	defer func() {
		// we use a lambda to make it clear in which order things need to happen
		te.stateLock.Unlock()
		close(te.transitionSignal)
	}()

	if te.state != Transitioning {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "this should never happen. the goroutine starting the transition should also finish it")
	}

	// Once we reach this point, it's as if our state is NotServing,
	// and we need to decide what the next step is
	switch te.nextState {
	case AcceptingReadAndWrite, AcceptingReadOnly:
		te.state = te.nextState
		te.open()
	case NotServing:
		te.state = NotServing
	case Transitioning:
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "this should never happen. nextState cannot be transitioning")
	}

	te.nextState = -1

	return nil
}

// Init must be called once when vttablet starts for setting
// up the metadata tables.
func (te *TxEngine) Init() error {
	if te.twopcEnabled {
		return te.twoPC.Init(te.env.DBConfigs().DbaWithDB())
	}
	return nil
}

// open opens the TxEngine. If 2pc is enabled, it restores
// all previously prepared transactions from the redo log.
// this should only be called when the state is already locked
func (te *TxEngine) open() {
	te.connHandler.Open(te.env.DBConfigs().AppWithDB(), te.env.DBConfigs().DbaWithDB(), te.env.DBConfigs().AppDebugWithDB())
	te.txTicks.Start(func() { te.transactionKiller() })

	if te.twopcEnabled && te.state == AcceptingReadAndWrite {
		te.twoPC.Open(te.env.DBConfigs())
		if err := te.prepareFromRedo(); err != nil {
			// If this operation fails, we choose to raise an alert and
			// continue anyway. Serving traffic is considered more important
			// than blocking everything for the sake of a few transactions.
			te.env.Stats().InternalErrors.Add("TwopcResurrection", 1)
			log.Errorf("Could not prepare transactions: %v", err)
		}
		te.startWatchdog()
	}
}

// StopGently will disregard common rules for when to kill transactions
// and wait forever for transactions to wrap up
func (te *TxEngine) StopGently() {
	te.stateLock.Lock()
	defer te.stateLock.Unlock()
	te.close(false)
	te.state = NotServing
}

// Close closes the TxEngine. If the immediate flag is on,
// then all current transactions are immediately rolled back.
// Otherwise, the function waits for all current transactions
// to conclude. If a shutdown grace period was specified,
// the transactions are rolled back if they're not resolved
// by that time.
func (te *TxEngine) close(immediate bool) {
	// Shut down functions are idempotent.
	// No need to check if 2pc is enabled.
	te.stopWatchdog()

	poolEmpty := make(chan bool)
	rollbackDone := make(chan bool)
	// This goroutine decides if transactions have to be
	// forced to rollback, and if so, when. Once done,
	// the function closes rollbackDone, which can be
	// verified to make sure it won't kick in later.
	go func() {
		defer func() {
			te.env.LogError()
			close(rollbackDone)
		}()
		if immediate {
			// Immediately rollback everything and return.
			log.Info("Immediate shutdown: rolling back now.")
			te.rollbackTransactions()
			return
		}
		if te.shutdownGracePeriod <= 0 {
			// No grace period was specified. Never rollback.
			te.rollbackPrepared()
			log.Info("No grace period specified: performing normal wait.")
			return
		}
		tmr := time.NewTimer(te.shutdownGracePeriod)
		defer tmr.Stop()
		select {
		case <-tmr.C:
			log.Info("Grace period exceeded: rolling back now.")
			te.rollbackTransactions()
		case <-poolEmpty:
			// The pool cleared before the timer kicked in. Just return.
			log.Info("Transactions completed before grace period: shutting down.")
		}
	}()
	te.connHandler.WaitForEmpty()
	// If the goroutine is still running, signal that it can exit.
	close(poolEmpty)
	// Make sure the goroutine has returned.
	<-rollbackDone

	te.txTicks.Stop()
	te.connHandler.Close()
	te.twoPC.Close()
}

// prepareFromRedo replays and prepares the transactions
// from the redo log, loads previously failed transactions
// into the reserved list, and adjusts the txPool LastID
// to ensure there are no future collisions.
func (te *TxEngine) prepareFromRedo() error {
	ctx := tabletenv.LocalContext()
	var allErr concurrency.AllErrorRecorder
	prepared, failed, err := te.twoPC.ReadAllRedo(ctx)
	if err != nil {
		return err
	}

	maxid := int64(0)
outer:
	for _, tx := range prepared {
		txid, err := dtids.TransactionID(tx.Dtid)
		if err != nil {
			log.Errorf("Error extracting transaction ID from ditd: %v", err)
		}
		if txid > maxid {
			maxid = txid
		}
		conn, _, err := te.Begin(ctx, &querypb.ExecuteOptions{})
		if err != nil {
			allErr.RecordError(err)
			continue
		}
		for _, stmt := range tx.Queries {
			conn.RecordQuery(stmt)
			_, err := conn.Exec(ctx, stmt, 1, false)
			if err != nil {
				allErr.RecordError(err)
				te.Rollback(ctx, conn)
				continue outer
			}
		}
		// We should not use the external Prepare because
		// we don't want to write again to the redo log.
		err = te.preparedPool.Put(conn, tx.Dtid)
		if err != nil {
			allErr.RecordError(err)
			continue
		}
	}
	for _, tx := range failed {
		txid, err := dtids.TransactionID(tx.Dtid)
		if err != nil {
			log.Errorf("Error extracting transaction ID from ditd: %v", err)
		}
		if txid > maxid {
			maxid = txid
		}
		te.preparedPool.SetFailed(tx.Dtid)
	}
	te.txPool.AdjustLastID(maxid)
	log.Infof("Prepared %d transactions, and registered %d failures.", len(prepared), len(failed))
	return allErr.Error()
}

// rollbackTransactions rolls back all open transactions
// including the prepared ones.
// This is used for transitioning from a master to a non-master
// serving type.
func (te *TxEngine) rollbackTransactions() {
	ctx := tabletenv.LocalContext()
	for _, c := range te.preparedPool.FetchAll() {
		te.Rollback(ctx, c)
	}
	// The order of rollbacks is currently not material because
	// we don't allow new statements or commits during
	// this function. In case of any such change, this will
	// have to be revisited.
	te.RollbackNonBusy(ctx)
}

func (te *TxEngine) rollbackPrepared() {
	ctx := tabletenv.LocalContext()
	for _, c := range te.preparedPool.FetchAll() {
		te.Rollback(ctx, c)
	}
}

// startWatchdog starts the watchdog goroutine, which looks for abandoned
// transactions and calls the notifier on them.
func (te *TxEngine) startWatchdog() {
	te.twoPCticks.Start(func() {
		ctx, cancel := context.WithTimeout(tabletenv.LocalContext(), te.abandonAge/4)
		defer cancel()

		// Raise alerts on prepares that have been unresolved for too long.
		// Use 5x abandonAge to give opportunity for watchdog to resolve these.
		count, err := te.twoPC.CountUnresolvedRedo(ctx, time.Now().Add(-te.abandonAge*5))
		if err != nil {
			te.env.Stats().InternalErrors.Add("WatchdogFail", 1)
			log.Errorf("Error reading unresolved prepares: '%v': %v", te.coordinatorAddress, err)
		}
		te.env.Stats().Unresolved.Set("Prepares", count)

		// Resolve lingering distributed transactions.
		txs, err := te.twoPC.ReadAbandoned(ctx, time.Now().Add(-te.abandonAge))
		if err != nil {
			te.env.Stats().InternalErrors.Add("WatchdogFail", 1)
			log.Errorf("Error reading transactions for 2pc watchdog: %v", err)
			return
		}
		if len(txs) == 0 {
			return
		}

		coordConn, err := vtgateconn.Dial(ctx, te.coordinatorAddress)
		if err != nil {
			te.env.Stats().InternalErrors.Add("WatchdogFail", 1)
			log.Errorf("Error connecting to coordinator '%v': %v", te.coordinatorAddress, err)
			return
		}
		defer coordConn.Close()

		var wg sync.WaitGroup
		for tx := range txs {
			wg.Add(1)
			go func(dtid string) {
				defer wg.Done()
				if err := coordConn.ResolveTransaction(ctx, dtid); err != nil {
					te.env.Stats().InternalErrors.Add("WatchdogFail", 1)
					log.Errorf("Error notifying for dtid %s: %v", dtid, err)
				}
			}(tx)
		}
		wg.Wait()
	})
}

// stopWatchdog stops the watchdog goroutine.
func (te *TxEngine) stopWatchdog() {
	te.twoPCticks.Stop()
}

// BeginAgain commits the existing transaction and begins a new one
func (te *TxEngine) BeginAgain(ctx context.Context, exc *ExclusiveConn) error {
	if exc.dbConn == nil || exc.Autocommit {
		return nil
	}
	if _, err := exc.dbConn.Exec(ctx, "commit", 1, false); err != nil {
		return err
	}
	if _, err := exc.dbConn.Exec(ctx, "begin", 1, false); err != nil {
		return err
	}
	return nil
}

// Rollback does a rollback on the connection
func (te *TxEngine) Rollback(ctx context.Context, conn *ExclusiveConn) error {
	if conn.dbConn == nil {
		return nil
	}
	span, ctx := trace.NewSpan(ctx, "TxEngine.LocalRollback")
	defer span.Finish()
	if conn.Autocommit {
		conn.release(TxCommit, "returned to pool")
		return nil
	}
	defer conn.release(TxRollback, "transaction rolled back")
	if _, err := conn.Exec(ctx, "rollback", 1, false); err != nil {
		conn.Close()
		return err
	}
	return nil
}

// RollbackNonBusy rolls back all transactions that are not in use.
// Transactions can be in use for situations like executing statements
// or in prepared state.
func (te *TxEngine) RollbackNonBusy(ctx context.Context) {
	for _, conn := range te.connHandler.GetOutdated(time.Duration(0), "for transition") {
		te.Rollback(ctx, conn)
	}
}

func (te *TxEngine) transactionKiller() {
	defer te.env.LogError()
	for _, conn := range te.connHandler.GetOutdated(te.Timeout(), "for tx killer rollback") {
		log.Warningf("killing transaction (exceeded timeout: %v): %s", te.Timeout(), conn.String())
		te.env.Stats().KillCounters.Add("Transactions", 1)
		conn.Close()
		conn.release(TxKill, fmt.Sprintf("exceeded timeout: %v", te.Timeout()))
	}
}

// Timeout returns the transaction timeout.
func (te *TxEngine) Timeout() time.Duration {
	return te.transactionTimeout.Get()
}

// SetTimeout sets the transaction timeout.
func (te *TxEngine) SetTimeout(timeout time.Duration) {
	te.transactionTimeout.Set(timeout)
	te.twoPCticks.SetInterval(timeout / 10)
}
