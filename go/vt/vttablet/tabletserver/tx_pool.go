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
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txlimiter"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// These consts identify how a transaction was resolved.
const (
	TxClose    = "close"
	TxCommit   = "commit"
	TxRollback = "rollback"
	TxPrepare  = "prepare"
	TxKill     = "kill"
)

const txLogInterval = time.Duration(1 * time.Minute)

type queries struct {
	setIsolationLevel string
	openTransaction   string
}

var (
	txIsolations = map[querypb.ExecuteOptions_TransactionIsolation]queries{
		querypb.ExecuteOptions_DEFAULT:                       {setIsolationLevel: "", openTransaction: "begin"},
		querypb.ExecuteOptions_REPEATABLE_READ:               {setIsolationLevel: "REPEATABLE READ", openTransaction: "begin"},
		querypb.ExecuteOptions_READ_COMMITTED:                {setIsolationLevel: "READ COMMITTED", openTransaction: "begin"},
		querypb.ExecuteOptions_READ_UNCOMMITTED:              {setIsolationLevel: "READ UNCOMMITTED", openTransaction: "begin"},
		querypb.ExecuteOptions_SERIALIZABLE:                  {setIsolationLevel: "SERIALIZABLE", openTransaction: "begin"},
		querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY: {setIsolationLevel: "REPEATABLE READ", openTransaction: "start transaction with consistent snapshot, read only"},
	}
)

// TxPool is the transaction pool for the query service.
type TxPool struct {
	env tabletenv.Env

	// conns is the 'regular' pool. By default, connections
	// are pulled from here for starting transactions.
	conns *connpool.Pool

	// foundRowsPool is the alternate pool that creates
	// connections with CLIENT_FOUND_ROWS flag set. A separate
	// pool is needed because this option can only be set at
	// connection time.
	foundRowsPool      *connpool.Pool
	activePool         *pools.Numbered
	lastID             sync2.AtomicInt64
	transactionTimeout sync2.AtomicDuration
	ticks              *timer.Timer
	limiter            txlimiter.TxLimiter

	txStats *servenv.TimingsWrapper

	// Tracking culprits that cause tx pool full errors.
	logMu   sync.Mutex
	lastLog time.Time
}

// NewTxPool creates a new TxPool. It's not operational until it's Open'd.
func NewTxPool(env tabletenv.Env, limiter txlimiter.TxLimiter) *TxPool {
	config := env.Config()
	transactionTimeout := time.Duration(config.Oltp.TxTimeoutSeconds * 1e9)
	axp := &TxPool{
		env:                env,
		conns:              connpool.NewPool(env, "TransactionPool", config.TxPool),
		foundRowsPool:      connpool.NewPool(env, "FoundRowsPool", config.TxPool),
		activePool:         pools.NewNumbered(),
		lastID:             sync2.NewAtomicInt64(time.Now().UnixNano()),
		transactionTimeout: sync2.NewAtomicDuration(transactionTimeout),
		ticks:              timer.NewTimer(transactionTimeout / 10),
		limiter:            limiter,
		txStats:            env.Exporter().NewTimings("Transactions", "Transaction stats", "operation"),
	}
	// Careful: conns also exports name+"xxx" vars,
	// but we know it doesn't export Timeout.
	env.Exporter().NewGaugeDurationFunc("TransactionTimeout", "Transaction timeout", axp.transactionTimeout.Get)
	return axp
}

// Open makes the TxPool operational. This also starts the transaction killer
// that will kill long-running transactions.
func (tp *TxPool) Open(appParams, dbaParams, appDebugParams dbconfigs.Connector) {
	log.Infof("Starting transaction id: %d", tp.lastID)
	tp.conns.Open(appParams, dbaParams, appDebugParams)
	foundRowsParam, _ := appParams.MysqlParams()
	foundRowsParam.EnableClientFoundRows()
	appParams = dbconfigs.New(foundRowsParam)
	tp.foundRowsPool.Open(appParams, dbaParams, appDebugParams)
	tp.ticks.Start(func() { tp.transactionKiller() })
}

// Close closes the TxPool. A closed pool can be reopened.
func (tp *TxPool) Close() {
	tp.ticks.Stop()
	for _, v := range tp.activePool.GetOutdated(time.Duration(0), "for closing") {
		conn := v.(*TxConnection)
		log.Warningf("killing transaction for shutdown: %s", conn.Format(nil))
		tp.env.Stats().InternalErrors.Add("StrayTransactions", 1)
		conn.Close()
		conn.conclude(TxClose, "pool closed")
	}
	tp.conns.Close()
	tp.foundRowsPool.Close()
}

// AdjustLastID adjusts the last transaction id to be at least
// as large as the input value. This will ensure that there are
// no dtid collisions with future transactions.
func (tp *TxPool) AdjustLastID(id int64) {
	if current := tp.lastID.Get(); current < id {
		log.Infof("Adjusting transaction id to: %d", id)
		tp.lastID.Set(id)
	}
}

// RollbackNonBusy rolls back all transactions that are not in use.
// Transactions can be in use for situations like executing statements
// or in prepared state.
func (tp *TxPool) RollbackNonBusy(ctx context.Context) {
	for _, v := range tp.activePool.GetOutdated(time.Duration(0), "for transition") {
		tp.LocalConclude(ctx, v.(*TxConnection))
	}
}

func (tp *TxPool) transactionKiller() {
	defer tp.env.LogError()
	for _, v := range tp.activePool.GetOutdated(time.Duration(tp.Timeout()), "for tx killer rollback") {
		conn := v.(*TxConnection)
		log.Warningf("killing transaction (exceeded timeout: %v): %s", tp.Timeout(), conn.Format(nil))
		tp.env.Stats().KillCounters.Add("Transactions", 1)
		conn.Close()
		conn.conclude(TxKill, fmt.Sprintf("exceeded timeout: %v", tp.Timeout()))
	}
}

// WaitForEmpty waits until all active transactions are completed.
func (tp *TxPool) WaitForEmpty() {
	tp.activePool.WaitForEmpty()
}

// Begin begins a transaction, and returns the associated transaction id and
// the statements (if any) executed to initiate the transaction. In autocommit
// mode the statement will be "".
//
// Subsequent statements can access the connection through the transaction id.
func (tp *TxPool) Begin(ctx context.Context, options *querypb.ExecuteOptions) (int64, string, error) {
	span, ctx := trace.NewSpan(ctx, "TxPool.Begin")
	defer span.Finish()
	var conn *connpool.DBConn
	var err error
	immediateCaller := callerid.ImmediateCallerIDFromContext(ctx)
	effectiveCaller := callerid.EffectiveCallerIDFromContext(ctx)

	if !tp.limiter.Get(immediateCaller, effectiveCaller) {
		return 0, "", vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "per-user transaction pool connection limit exceeded")
	}

	var beginSucceeded bool
	defer func() {
		if beginSucceeded {
			return
		}

		if conn != nil {
			conn.Recycle()
		}
		tp.limiter.Release(immediateCaller, effectiveCaller)
	}()

	if options.GetClientFoundRows() {
		conn, err = tp.foundRowsPool.Get(ctx)
	} else {
		conn, err = tp.conns.Get(ctx)
	}
	if err != nil {
		switch err {
		case connpool.ErrConnPoolClosed:
			return 0, "", err
		case pools.ErrCtxTimeout:
			tp.LogActive()
			return 0, "", vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "transaction pool aborting request due to already expired context")
		case pools.ErrTimeout:
			tp.LogActive()
			return 0, "", vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "transaction pool connection limit exceeded")
		}
		return 0, "", err
	}

	autocommitTransaction := false
	beginQueries := ""
	if queries, ok := txIsolations[options.GetTransactionIsolation()]; ok {
		if queries.setIsolationLevel != "" {
			if _, err := conn.Exec(ctx, "set transaction isolation level "+queries.setIsolationLevel, 1, false); err != nil {
				return 0, "", err
			}

			beginQueries = queries.setIsolationLevel + "; "
		}

		if _, err := conn.Exec(ctx, queries.openTransaction, 1, false); err != nil {
			return 0, "", err
		}
		beginQueries = beginQueries + queries.openTransaction
	} else if options.GetTransactionIsolation() == querypb.ExecuteOptions_AUTOCOMMIT {
		autocommitTransaction = true
	} else {
		return 0, "", fmt.Errorf("don't know how to open a transaction of this type: %v", options.GetTransactionIsolation())
	}

	beginSucceeded = true
	transactionID := tp.lastID.Add(1)
	tp.activePool.Register(
		transactionID,
		newTxConnection(
			conn,
			transactionID,
			tp,
			immediateCaller,
			effectiveCaller,
			autocommitTransaction,
		),
		options.GetWorkload() != querypb.ExecuteOptions_DBA,
	)
	return transactionID, beginQueries, nil
}

// Commit commits the specified transaction.
func (tp *TxPool) Commit(ctx context.Context, transactionID int64) (string, error) {
	span, ctx := trace.NewSpan(ctx, "TxPool.Commit")
	defer span.Finish()
	conn, err := tp.Get(transactionID, "for commit")
	if err != nil {
		return "", err
	}
	return tp.LocalCommit(ctx, conn)
}

// Rollback rolls back the specified transaction.
func (tp *TxPool) Rollback(ctx context.Context, transactionID int64) error {
	span, ctx := trace.NewSpan(ctx, "TxPool.Rollback")
	defer span.Finish()

	conn, err := tp.Get(transactionID, "for rollback")
	if err != nil {
		return err
	}
	return tp.localRollback(ctx, conn)
}

// Get fetches the connection associated to the transactionID.
// You must call Recycle on TxConnection once done.
func (tp *TxPool) Get(transactionID int64, reason string) (*TxConnection, error) {
	v, err := tp.activePool.Get(transactionID, reason)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction %d: %v", transactionID, err)
	}
	return v.(*TxConnection), nil
}

// LocalBegin is equivalent to Begin->Get.
// It's used for executing transactions within a request. It's safe
// to always call LocalConclude at the end.
func (tp *TxPool) LocalBegin(ctx context.Context, options *querypb.ExecuteOptions) (*TxConnection, string, error) {
	span, ctx := trace.NewSpan(ctx, "TxPool.LocalBegin")
	defer span.Finish()

	transactionID, beginSQL, err := tp.Begin(ctx, options)
	if err != nil {
		return nil, "", err
	}
	conn, err := tp.Get(transactionID, "for local query")
	return conn, beginSQL, err
}

// LocalCommit is the commit function for LocalBegin.
func (tp *TxPool) LocalCommit(ctx context.Context, conn *TxConnection) (string, error) {
	span, ctx := trace.NewSpan(ctx, "TxPool.LocalCommit")
	defer span.Finish()
	defer conn.conclude(TxCommit, "transaction committed")

	if conn.Autocommit {
		return "", nil
	}

	if _, err := conn.Exec(ctx, "commit", 1, false); err != nil {
		conn.Close()
		return "", err
	}
	return "commit", nil
}

// LocalConclude concludes a transaction started by LocalBegin.
// If the transaction was not previously concluded, it's rolled back.
func (tp *TxPool) LocalConclude(ctx context.Context, conn *TxConnection) {
	if conn.dbConn == nil {
		return
	}
	span, ctx := trace.NewSpan(ctx, "TxPool.LocalConclude")
	defer span.Finish()
	_ = tp.localRollback(ctx, conn)
}

func (tp *TxPool) localRollback(ctx context.Context, conn *TxConnection) error {
	if conn.Autocommit {
		conn.conclude(TxCommit, "returned to pool")
		return nil
	}
	defer conn.conclude(TxRollback, "transaction rolled back")
	if _, err := conn.Exec(ctx, "rollback", 1, false); err != nil {
		conn.Close()
		return err
	}
	return nil
}

// LogActive causes all existing transactions to be logged when they complete.
// The logging is throttled to no more than once every txLogInterval.
func (tp *TxPool) LogActive() {
	tp.logMu.Lock()
	defer tp.logMu.Unlock()
	if time.Since(tp.lastLog) < txLogInterval {
		return
	}
	tp.lastLog = time.Now()
	conns := tp.activePool.GetAll()
	for _, c := range conns {
		c.(*TxConnection).LogToFile.Set(1)
	}
}

// Timeout returns the transaction timeout.
func (tp *TxPool) Timeout() time.Duration {
	return tp.transactionTimeout.Get()
}

// SetTimeout sets the transaction timeout.
func (tp *TxPool) SetTimeout(timeout time.Duration) {
	tp.transactionTimeout.Set(timeout)
	tp.ticks.SetInterval(timeout / 10)
}

// TxConnection is meant for executing transactions. It can return itself to
// the tx pool correctly. It also does not retry statements if there
// are failures.
type TxConnection struct {
	dbConn            *connpool.DBConn
	TransactionID     int64
	pool              *TxPool
	StartTime         time.Time
	EndTime           time.Time
	Queries           []string
	Conclusion        string
	LogToFile         sync2.AtomicInt32
	ImmediateCallerID *querypb.VTGateCallerID
	EffectiveCallerID *vtrpcpb.CallerID
	Autocommit        bool
}

func newTxConnection(conn *connpool.DBConn, transactionID int64, pool *TxPool, immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID, autocommit bool) *TxConnection {
	return &TxConnection{
		dbConn:            conn,
		TransactionID:     transactionID,
		pool:              pool,
		StartTime:         time.Now(),
		ImmediateCallerID: immediate,
		EffectiveCallerID: effective,
		Autocommit:        autocommit,
	}
}

// Close closes the connection.
func (txc *TxConnection) Close() {
	if txc.dbConn != nil {
		txc.dbConn.Close()
	}
}

// Exec executes the statement for the current transaction.
func (txc *TxConnection) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if txc.dbConn == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction was aborted: %v", txc.Conclusion)
	}
	r, err := txc.dbConn.ExecOnce(ctx, query, maxrows, wantfields)
	if err != nil {
		if mysql.IsConnErr(err) {
			select {
			case <-ctx.Done():
				// If the context is done, the query was killed.
				// So, don't trigger a mysql check.
			default:
				txc.pool.env.CheckMySQL()
			}
		}
		return nil, err
	}
	return r, nil
}

// BeginAgain commits the existing transaction and begins a new one
func (txc *TxConnection) BeginAgain(ctx context.Context) error {
	if txc.dbConn == nil || txc.Autocommit {
		return nil
	}
	if _, err := txc.dbConn.Exec(ctx, "commit", 1, false); err != nil {
		return err
	}
	if _, err := txc.dbConn.Exec(ctx, "begin", 1, false); err != nil {
		return err
	}
	return nil
}

// Recycle returns the connection to the pool. The transaction remains
// active.
func (txc *TxConnection) Recycle() {
	if txc.dbConn == nil {
		return
	}
	if txc.dbConn.IsClosed() {
		txc.conclude(TxClose, "closed")
	} else {
		txc.pool.activePool.Put(txc.TransactionID)
	}
}

// RecordQuery records the query against this transaction.
func (txc *TxConnection) RecordQuery(query string) {
	txc.Queries = append(txc.Queries, query)
}

func (txc *TxConnection) conclude(conclusion, reason string) {
	if txc.dbConn == nil {
		return
	}
	txc.pool.activePool.Unregister(txc.TransactionID, reason)
	txc.dbConn.Recycle()
	txc.dbConn = nil
	txc.pool.limiter.Release(txc.ImmediateCallerID, txc.EffectiveCallerID)
	txc.log(conclusion)
}

func (txc *TxConnection) log(conclusion string) {
	txc.Conclusion = conclusion
	txc.EndTime = time.Now()

	username := callerid.GetPrincipal(txc.EffectiveCallerID)
	if username == "" {
		username = callerid.GetUsername(txc.ImmediateCallerID)
	}
	duration := txc.EndTime.Sub(txc.StartTime)
	txc.pool.env.Stats().UserTransactionCount.Add([]string{username, conclusion}, 1)
	txc.pool.env.Stats().UserTransactionTimesNs.Add([]string{username, conclusion}, int64(duration))
	txc.pool.txStats.Add(conclusion, duration)
	if txc.LogToFile.Get() != 0 {
		log.Infof("Logged transaction: %s", txc.Format(nil))
	}
	tabletenv.TxLogger.Send(txc)
}

// EventTime returns the time the event was created.
func (txc *TxConnection) EventTime() time.Time {
	return txc.EndTime
}

// Format returns a printable version of the connection info.
func (txc *TxConnection) Format(params url.Values) string {
	return fmt.Sprintf(
		"%v\t'%v'\t'%v'\t%v\t%v\t%.6f\t%v\t%v\t\n",
		txc.TransactionID,
		callerid.GetPrincipal(txc.EffectiveCallerID),
		callerid.GetUsername(txc.ImmediateCallerID),
		txc.StartTime.Format(time.StampMicro),
		txc.EndTime.Format(time.StampMicro),
		txc.EndTime.Sub(txc.StartTime).Seconds(),
		txc.Conclusion,
		strings.Join(txc.Queries, ";"),
	)
}
