/*
Copyright 2018 The Vitess Authors

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

// Package direct allows anyone to instantiate an embedded TabletServer
// that allows you to talk directly to mysql instead of going through
// a connection to vttablet.
package direct

import (
	"fmt"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// templateOnce is used to change the tablet stats URLs to point
// to the embedded tablet servers instead of the remote vttablets.
var templateOnce sync.Once

func init() {
	tabletconn.RegisterDialer("direct", newEmbeddedTS)
}

// embeddedTS implements queryservice.QueryService by forwarding execution
// to an embedded TabletServer. It also connects to vttablet to proxy healthcheck
// streams as well as commands that should only be executed by vttablet.
// The functions need to marshal certain input and output variables because those
// values may be modified by the callers or callees. This is the case for
// bind vars, query results, and errors.
type embeddedTS struct {
	queryservice.QueryService
	ts         *tabletserver.TabletServer
	tabletConn queryservice.QueryService

	// We only expect one goroutine to be accessing target,
	// but mu is in place just in case StreamHealth later gets
	// called multiple times.
	mu     sync.Mutex
	target querypb.Target
}

func newEmbeddedTS(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
	templateOnce.Do(func() {
		*discovery.TabletURLTemplateString = "{{.EmbeddedStatusURL}}"
		discovery.ParseTabletURLTemplateFromFlag()
	})

	// Dial into vttablet for proxying some of the requests.
	tc, err := tabletconn.GetDialerByName("grpc")(tablet, failFast)
	if err != nil {
		return nil, err
	}

	config := tabletenv.DefaultQsConfig
	ets := &embeddedTS{
		ts:         tabletserver.NewCustomTabletServer(topoproto.TabletAliasString(tablet.Alias), config, nil, *tablet.Alias),
		tabletConn: tc,
		target: querypb.Target{
			Keyspace:   tablet.Keyspace,
			Shard:      tablet.Shard,
			TabletType: tablet.Type,
		},
	}
	// TODO(sougou): need to add a link to the remote vttablet here.
	ets.ts.AddStatusHeader()
	ets.ts.AddStatusPart()

	ets.ts.RegisterQueryRuleSource("embeddedMode")
	erules := rules.New()
	rule1 := rules.NewQueryRule("redirect_to_vttablet", "redirect_to_vttablet", rules.QRFailRetry)
	rule1.AddPlanCond(planbuilder.PlanNextval)
	rule1.AddPlanCond(planbuilder.PlanDDL)
	erules.Add(rule1)
	// TODO(sougou): we still don't have a way to intercept updates and deletes to messages.
	rule2 := rules.NewQueryRule("disallow_plans", "disallow_plans", rules.QRFail)
	rule2.AddPlanCond(planbuilder.PlanInsertMessage)
	rule2.AddPlanCond(planbuilder.PlanMessageStream)
	erules.Add(rule2)
	if err := ets.ts.SetQueryRules("embeddedMode", erules); err != nil {
		tc.Close(context.TODO())
		return nil, err
	}

	// Provide errors for unsupported functions.
	ets.QueryService = queryservice.Wrap(
		nil,
		func(ctx context.Context, target *querypb.Target, conn queryservice.QueryService, name string, inTransaction bool, inner func(context.Context, *querypb.Target, queryservice.QueryService) (bool, error)) error {
			return fmt.Errorf("directMysql does not implement %s", name)
		},
	)

	// TODO(sougou): still need a way to specify the SSL and charset parameters.
	connParams := mysql.ConnParams{
		Host:    topoproto.MysqlHostname(tablet),
		Port:    int(topoproto.MysqlPort(tablet)),
		Charset: "utf8",
	}
	dbcfgs := dbconfigs.NewDirectDBConfigs(connParams, topoproto.TabletDbName(tablet))

	err = ets.ts.InitDBConfig(ets.target, dbcfgs)
	if err != nil {
		tc.Close(context.TODO())
		return nil, err
	}
	return ets, nil
}

func (ets *embeddedTS) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	result, err := ets.ts.Execute(ctx, target, query, sqltypes.CopyBindVariables(bindVars), transactionID, options)
	if err != nil {
		if vterrors.Code(err) == vtrpcpb.Code_FAILED_PRECONDITION && strings.Contains(err.Error(), "due to rule: redirect_to_vttablet") {
			return ets.tabletConn.Execute(ctx, target, query, bindVars, 0, options)
		}
		return nil, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
	}
	return result.Copy(), nil
}

func (ets *embeddedTS) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	q := make([]*querypb.BoundQuery, len(queries))
	for i, query := range queries {
		q[i] = &querypb.BoundQuery{
			Sql:           query.Sql,
			BindVariables: sqltypes.CopyBindVariables(query.BindVariables),
		}
	}
	results, err := ets.ts.ExecuteBatch(ctx, target, q, asTransaction, transactionID, options)
	if err != nil {
		if vterrors.Code(err) == vtrpcpb.Code_FAILED_PRECONDITION && strings.Contains(err.Error(), "due to rule: redirect_to_vttablet") {
			return ets.tabletConn.ExecuteBatch(ctx, target, q, asTransaction, 0, options)
		}
		return nil, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
	}
	copied := make([]sqltypes.Result, len(results))
	for i, result := range results {
		copied[i] = *result.Copy()
	}
	return copied, nil
}

func (ets *embeddedTS) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	err := ets.ts.StreamExecute(ctx, target, query, sqltypes.CopyBindVariables(bindVars), transactionID, options, func(qr *sqltypes.Result) error {
		return callback(qr.Copy())
	})
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

func (ets *embeddedTS) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (int64, error) {
	transactionID, err := ets.ts.Begin(ctx, target, options)
	if err != nil {
		return 0, tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
	}
	return transactionID, nil
}

func (ets *embeddedTS) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	err := ets.ts.Commit(ctx, target, transactionID)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

func (ets *embeddedTS) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	err := ets.ts.Rollback(ctx, target, transactionID)
	return tabletconn.ErrorFromGRPC(vterrors.ToGRPC(err))
}

func (ets *embeddedTS) BeginExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error) {
	qr, transactionID, err := ets.ts.BeginExecute(ctx, target, query, sqltypes.CopyBindVariables(bindVars), options)
	if err != nil && vterrors.Code(err) == vtrpcpb.Code_FAILED_PRECONDITION && strings.Contains(err.Error(), "due to rule: redirect_to_vttablet") {
		// We can still send the requests to vttablet because these are transactionless statements.
		// TODO(sougou): but what do we do about this dangling transaction?
		qr, err = ets.tabletConn.Execute(ctx, target, query, bindVars, 0, options)
		return qr, transactionID, err
	}
	return qr.Copy(), transactionID, err
}

func (ets *embeddedTS) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	return ets.tabletConn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
		ets.mu.Lock()
		defer ets.mu.Unlock()

		shr2 := *shr

		if shr2.Target.TabletType != ets.target.TabletType || shr2.Serving != ets.ts.IsServing() {
			_, _ = ets.ts.SetServingType(shr2.Target.TabletType, shr2.Serving, nil)
			ets.target.TabletType = shr2.Target.TabletType
			// If the embedded TabletServer is not serving, override what's coming from vttablet.
			if !ets.ts.IsServing() {
				shr2.Serving = false
			}
		}
		return callback(&shr2)
	})
}

func (ets *embeddedTS) HandlePanic(err *error) {
	ets.ts.HandlePanic(err)
}

func (ets *embeddedTS) Close(ctx context.Context) error {
	// TODO(sougou): implement Embedder.Reset and call it here.
	ets.tabletConn.Close(ctx)
	ets.ts.StopService()
	return nil
}
