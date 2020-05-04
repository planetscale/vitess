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
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
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

// ConnectionResourceHandler is the transaction pool for the query service.
type ConnectionResourceHandler struct {
	env tabletenv.Env

	// conns is the 'regular' pool. By default, connections
	// are pulled from here for starting transactions.
	conns *connpool.Pool

	// foundRowsPool is the alternate pool that creates
	// connections with CLIENT_FOUND_ROWS flag set. A separate
	// pool is needed because this option can only be set at
	// connection time.
	foundRowsPool *connpool.Pool
	activePool    *pools.Numbered
	lastID        sync2.AtomicInt64
	limiter       txlimiter.TxLimiter

	txStats *servenv.TimingsWrapper

	// Tracking culprits that cause tx pool full errors.
	logMu   sync.Mutex
	lastLog time.Time
}

// NewTxPool creates a new ConnectionResourceHandler. It's not operational until it's Open'd.
func NewTxPool(env tabletenv.Env, limiter txlimiter.TxLimiter) *ConnectionResourceHandler {
	config := env.Config()

	axp := &ConnectionResourceHandler{
		env:           env,
		conns:         connpool.NewPool(env, "TransactionPool", config.TxPool),
		foundRowsPool: connpool.NewPool(env, "FoundRowsPool", config.TxPool),
		activePool:    pools.NewNumbered(),
		lastID:        sync2.NewAtomicInt64(time.Now().UnixNano()),
		limiter:       limiter,
		txStats:       env.Exporter().NewTimings("Transactions", "Transaction stats", "operation"),
	}
	return axp
}

// Open makes the ConnectionResourceHandler operational. This also starts the transaction killer
// that will kill long-running transactions.
func (crh *ConnectionResourceHandler) Open(appParams, dbaParams, appDebugParams dbconfigs.Connector) {
	log.Infof("Starting transaction id: %d", crh.lastID)
	crh.conns.Open(appParams, dbaParams, appDebugParams)
	foundRowsParam, _ := appParams.MysqlParams()
	foundRowsParam.EnableClientFoundRows()
	appParams = dbconfigs.New(foundRowsParam)
	crh.foundRowsPool.Open(appParams, dbaParams, appDebugParams)
}

// Close closes the ConnectionResourceHandler. A closed pool can be reopened.
func (crh *ConnectionResourceHandler) Close() {
	for _, v := range crh.activePool.GetOutdated(time.Duration(0), "for closing") {
		conn := v.(*ExclusiveConn)
		log.Warningf("killing transaction for shutdown: %s", conn.String())
		crh.env.Stats().InternalErrors.Add("StrayTransactions", 1)
		conn.Close()
		conn.release(TxClose, "pool closed")
	}
	crh.conns.Close()
	crh.foundRowsPool.Close()
}

// AdjustLastID adjusts the last transaction id to be at least
// as large as the input value. This will ensure that there are
// no dtid collisions with future transactions.
func (crh *ConnectionResourceHandler) AdjustLastID(id int64) {
	if current := crh.lastID.Get(); current < id {
		log.Infof("Adjusting transaction id to: %d", id)
		crh.lastID.Set(id)
	}
}

// WaitForEmpty waits until all active connections are recycled.
func (crh *ConnectionResourceHandler) WaitForEmpty() {
	crh.activePool.WaitForEmpty()
}

// Get fetches the connection associated to the transactionID.
// You must call Recycle on ExclusiveConn once done.
func (crh *ConnectionResourceHandler) Get(transactionID int64, reason string) (*ExclusiveConn, error) {
	v, err := crh.activePool.Get(transactionID, reason)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction %d: %v", transactionID, err)
	}
	return v.(*ExclusiveConn), nil
}

// LogActive causes all existing transactions to be logged when they complete.
// The logging is throttled to no more than once every txLogInterval.
func (crh *ConnectionResourceHandler) LogActive() {
	crh.logMu.Lock()
	defer crh.logMu.Unlock()
	if time.Since(crh.lastLog) < txLogInterval {
		return
	}
	crh.lastLog = time.Now()
	conns := crh.activePool.GetAll()
	for _, c := range conns {
		c.(*ExclusiveConn).LogToFile.Set(1)
	}
}

//ClaimExclusiveConnection gets a connection from a pool and claims it for this session
func (crh *ConnectionResourceHandler) ClaimExclusiveConnection(ctx context.Context, options *querypb.ExecuteOptions) (*ExclusiveConn, error) {
	var conn *connpool.DBConn
	var err error
	if options.GetClientFoundRows() {
		conn, err = crh.foundRowsPool.Get(ctx)
	} else {
		conn, err = crh.conns.Get(ctx)
	}
	if err != nil {
		switch err {
		case connpool.ErrConnPoolClosed:
			return nil, err
		case pools.ErrCtxTimeout:
			crh.LogActive()
			return nil, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "transaction pool aborting request due to already expired context")
		case pools.ErrTimeout:
			crh.LogActive()
			return nil, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "transaction pool connection limit exceeded")
		}
		return nil, err
	}
	transactionID := crh.lastID.Add(1)
	exclusiveConn := &ExclusiveConn{
		dbConn:       conn,
		ConnectionID: transactionID,
		pool:         crh,
		StartTime:    time.Now(),
	}

	crh.activePool.Register(
		transactionID,
		exclusiveConn,
		options.GetWorkload() != querypb.ExecuteOptions_DBA,
	)
	return exclusiveConn, nil
}

// ExclusiveConn is meant for executing transactions. It can return itself to
// the tx pool correctly. It also does not retry statements if there
// are failures.
type ExclusiveConn struct {
	dbConn            *connpool.DBConn
	ConnectionID      int64
	pool              *ConnectionResourceHandler
	StartTime         time.Time
	EndTime           time.Time
	Queries           []string
	Conclusion        string
	LogToFile         sync2.AtomicInt32
	ImmediateCallerID *querypb.VTGateCallerID
	EffectiveCallerID *vtrpcpb.CallerID
	Autocommit        bool
}

// Close closes the connection.
func (exc *ExclusiveConn) Close() {
	if exc.dbConn != nil {
		exc.dbConn.Close()
	}
}

// Exec executes the statement for the current transaction.
func (exc *ExclusiveConn) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if exc.dbConn == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "connection was closed: %v", exc.Conclusion)
	}
	r, err := exc.dbConn.ExecOnce(ctx, query, maxrows, wantfields)
	if err != nil {
		if mysql.IsConnErr(err) {
			select {
			case <-ctx.Done():
				// If the context is done, the query was killed.
				// So, don't trigger a mysql check.
			default:
				exc.pool.env.CheckMySQL()
			}
		}
		return nil, err
	}
	return r, nil
}

// Recycle returns the connection to the pool. The transaction remains
// active.
func (exc *ExclusiveConn) Recycle() {
	if exc.dbConn == nil {
		return
	}
	if exc.dbConn.IsClosed() {
		exc.release(TxClose, "closed")
	} else {
		exc.pool.activePool.Put(exc.ConnectionID)
	}
}

// RecordQuery records the query against this transaction.
func (exc *ExclusiveConn) RecordQuery(query string) {
	exc.Queries = append(exc.Queries, query)
}

func (exc *ExclusiveConn) release(conclusion, reason string) {
	if exc.dbConn == nil {
		return
	}
	exc.pool.activePool.Unregister(exc.ConnectionID, reason)
	exc.dbConn.Recycle()
	exc.dbConn = nil
	exc.pool.limiter.Release(exc.ImmediateCallerID, exc.EffectiveCallerID)
	exc.log(conclusion)
}

func (exc *ExclusiveConn) log(conclusion string) {
	exc.Conclusion = conclusion
	exc.EndTime = time.Now()

	username := callerid.GetPrincipal(exc.EffectiveCallerID)
	if username == "" {
		username = callerid.GetUsername(exc.ImmediateCallerID)
	}
	duration := exc.EndTime.Sub(exc.StartTime)
	exc.pool.env.Stats().UserTransactionCount.Add([]string{username, conclusion}, 1)
	exc.pool.env.Stats().UserTransactionTimesNs.Add([]string{username, conclusion}, int64(duration))
	exc.pool.txStats.Add(conclusion, duration)
	if exc.LogToFile.Get() != 0 {
		log.Infof("Logged transaction: %s", exc.String())
	}
	tabletenv.TxLogger.Send(exc)
}

// EventTime returns the time the event was created.
func (exc *ExclusiveConn) EventTime() time.Time {
	return exc.EndTime
}

// String returns a printable version of the connection info.
func (exc *ExclusiveConn) String() string {
	return fmt.Sprintf(
		"%v\t'%v'\t'%v'\t%v\t%v\t%.6f\t%v\t%v\t\n",
		exc.ConnectionID,
		callerid.GetPrincipal(exc.EffectiveCallerID),
		callerid.GetUsername(exc.ImmediateCallerID),
		exc.StartTime.Format(time.StampMicro),
		exc.EndTime.Format(time.StampMicro),
		exc.EndTime.Sub(exc.StartTime).Seconds(),
		exc.Conclusion,
		strings.Join(exc.Queries, ";"),
	)
}
