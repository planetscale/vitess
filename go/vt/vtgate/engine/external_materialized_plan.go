package engine

import (
	"context"

	boostwatcher "vitess.io/vitess/go/boost/topo/watcher"
	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

var _ Primitive = (*externalPlanPrimitive)(nil)

type externalPlanPrimitive struct {
	view     *boostwatcher.View
	args     []*querypb.BindVariable
	keyspace string
}

var materializationEnabled = 1

func SetServeMaterializedViews(v int) {
	materializationEnabled = v
}

func GetServeMaterializedViews() int {
	return materializationEnabled
}

func (e *externalPlanPrimitive) RouteType() string {
	return "ExternalMaterialized"
}

func (e *externalPlanPrimitive) GetKeyspaceName() string {
	return e.keyspace
}

func (e *externalPlanPrimitive) GetTableName() string {
	return e.view.TableName()
}

func (e *externalPlanPrimitive) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{Fields: e.view.Fields()}, nil
}

func (e *externalPlanPrimitive) NeedsTransaction() bool {
	return false
}

func (e *externalPlanPrimitive) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return e.view.LookupByBindVar(ctx, e.args, true)
}

func (e *externalPlanPrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := e.view.LookupByBindVar(ctx, e.args, true)
	if err != nil {
		return err
	}
	return callback(res)
}

func (e *externalPlanPrimitive) Inputs() []Primitive {
	return nil
}

func (e *externalPlanPrimitive) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Boost",
	}
}

func CanMaterialize(query sqlparser.Statement) bool {
	switch query := query.(type) {
	case sqlparser.SelectStatement:
		comments := query.GetParsedComments()
		if comments.Directives().IsSet("NO_CACHE") {
			return false
		}
		return true
	default:
		// if it's not a SELECT or UNION boost has nothing to do with it
		return false
	}
}

func (mat *MaterializationClient) GetPlan(vcursor VCursor, query sqlparser.Statement, bvars map[string]*querypb.BindVariable) (*Plan, bool) {
	if mat == nil || materializationEnabled == 0 {
		return nil, false
	}

	keyspace := vcursor.GetKeyspace()
	cached, ok := mat.watcher.GetCachedQuery(keyspace, query, bvars)
	if !ok {
		return nil, false
	}

	plan := &Plan{
		Type:     sqlparser.StmtSelect,
		Original: cached.Normalized,
		Instructions: &externalPlanPrimitive{
			view:     cached.View,
			args:     cached.Args,
			keyspace: keyspace,
		},
		BindVarNeeds: &sqlparser.BindVarNeeds{},
	}

	go func() {
		mat.watcher.Warmup(cached, func(view *boostwatcher.View) {
			_, err := view.LookupByBindVar(context.Background(), cached.Args, false)
			if err != nil {
				log.Warningf("failed to execute warming on cluster: %v", err)
			}
		})
	}()

	return plan, true
}

type MaterializationClient struct {
	watcher *boostwatcher.Watcher
}

func NewMaterializationClient(watcher *boostwatcher.Watcher) *MaterializationClient {
	return &MaterializationClient{
		watcher: watcher,
	}
}
