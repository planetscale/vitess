package view

import (
	"context"
	"strconv"

	"vitess.io/vitess/go/boost/server/controller/boostplan/viewplan"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vthash"
)

type bindVarMap struct {
	Name string
	Pos  int
}

type Filter struct {
	bvars   []bindVarMap
	expr    []evalengine.Expr
	agg     []viewplan.Aggregation
	schema  []sql.Type
	groupBy []int
}

func (f *Filter) makeAggregationState() []aggregator {
	agg := make([]aggregator, 0, len(f.agg))
	for i, a := range f.agg {
		agg = append(agg, f.makeAggregator(a, f.schema[i].Collation))
	}
	return agg
}

func (f *Filter) makeAggregator(kind viewplan.Aggregation, collation collations.ID) aggregator {
	switch kind {
	case viewplan.Aggregation_FIRST:
		return &aggregationFirst{}
	case viewplan.Aggregation_COUNT:
		return &aggregationCount{}
	case viewplan.Aggregation_SUM:
		return &aggregationSum{}
	case viewplan.Aggregation_MIN:
		return &aggregationMin{collation: collation}
	case viewplan.Aggregation_MAX:
		return &aggregationMax{collation: collation}
	default:
		panic("unexpected aggregation kind")
	}
}

type aggregator interface {
	Add(col int, row sql.Row)
	Resolve(rb *sql.RowBuilder)
}

func newFilter(schema []sql.Type, desc *viewplan.Plan) *Filter {
	var posts int
	for _, p := range desc.Parameters {
		if p.Kind == viewplan.Param_POST {
			posts++
		}
	}
	if posts == 0 {
		return nil
	}

	filter := &Filter{
		agg:     desc.PostAggregation,
		schema:  schema,
		groupBy: desc.PostGroupBy,
	}

	for i, p := range desc.Parameters {
		if p.Kind == viewplan.Param_POST {
			expr := sql.EvalExprFromProto(p.PostFilter)
			filter.expr = append(filter.expr, expr.Eval)
			filter.bvars = append(filter.bvars, bindVarMap{Name: p.Name, Pos: i})
		}
	}

	return filter
}

func (f *Filter) extractBindVars(row sql.Row) map[string]*querypb.BindVariable {
	vars := make(map[string]*querypb.BindVariable, len(f.bvars))
	for _, bv := range f.bvars {
		vars[bv.Name] = row.ValueAt(bv.Pos).ToBindVarUnsafe()
	}
	return vars
}

func (f *Filter) Apply(ctx context.Context, key sql.Row, rows []sql.Row) []sql.Row {
	if f == nil {
		return rows
	}

	env := evalengine.NewExpressionEnv(ctx, f.extractBindVars(key), nil)
	if len(f.agg) == 0 {
		return f.apply(env, rows)
	}
	if len(f.groupBy) == 0 {
		return f.aggregate(env, rows)
	}
	return f.aggregateGroup(env, rows)
}

func (f *Filter) apply(env *evalengine.ExpressionEnv, rows []sql.Row) []sql.Row {
	filtered := rows[:0]

nextRow:
	for _, row := range rows {
		env.Row = row.ToVitess()
		for _, expr := range f.expr {
			v, err := env.Evaluate(expr)
			if err != nil {
				continue nextRow
			}
			if !v.ToBoolean() {
				continue nextRow
			}
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func (f *Filter) aggregate(env *evalengine.ExpressionEnv, rows []sql.Row) []sql.Row {
	agg := f.makeAggregationState()

nextRow:
	for _, row := range rows {
		env.Row = row.ToVitess()
		for _, expr := range f.expr {
			v, err := env.Evaluate(expr)
			if err != nil {
				continue nextRow
			}
			if !v.ToBoolean() {
				continue nextRow
			}
		}
		for col, a := range agg {
			a.Add(col, row)
		}
	}

	rb := sql.NewRowBuilder(len(agg))
	for _, a := range agg {
		a.Resolve(&rb)
	}
	return append(rows[:0], rb.Finish())
}

func (f *Filter) aggregateGroup(env *evalengine.ExpressionEnv, rows []sql.Row) []sql.Row {
	groupBy := f.groupBy
	schema := f.schema
	groupAgg := make(map[vthash.Hash][]aggregator)
	var hash vthash.Hasher

nextRow:
	for _, row := range rows {
		env.Row = row.ToVitess()
		for _, expr := range f.expr {
			v, err := env.Evaluate(expr)
			if err != nil {
				continue nextRow
			}
			if !v.ToBoolean() {
				continue nextRow
			}
		}

		hash.Reset()

		key := row.HashWithKey(&hash, groupBy, schema)
		agg, ok := groupAgg[key]
		if !ok {
			agg = f.makeAggregationState()
			groupAgg[key] = agg
		}

		for col, a := range agg {
			a.Add(col, row)
		}
	}

	filtered := rows[:0]
	for _, agg := range groupAgg {
		rb := sql.NewRowBuilder(len(agg))
		for _, a := range agg {
			a.Resolve(&rb)
		}
		filtered = append(filtered, rb.Finish())
	}
	return filtered
}

type aggregationFirst struct {
	v sql.Value
}

func (agg *aggregationFirst) Add(col int, row sql.Row) {
	if agg.v == "" {
		agg.v = row.ValueAt(col)
	}
}

func (agg *aggregationFirst) Resolve(rb *sql.RowBuilder) {
	rb.Add(agg.v)
}

type aggregationCount struct {
	count int64
}

func (agg *aggregationCount) Add(col int, row sql.Row) {
	c := row.ValueAt(col).ToVitessUnsafe()
	v, err := c.ToInt64()
	if err != nil {
		panic(err)
	}
	agg.count += v
}

func (agg *aggregationCount) Resolve(rb *sql.RowBuilder) {
	rb.AddRaw(sqltypes.Int64, strconv.AppendInt(nil, agg.count, 10))
}

type aggregationSum struct {
	sum sqltypes.Value
}

func (agg *aggregationSum) Add(col int, row sql.Row) {
	v := row.ValueAt(col).ToVitessUnsafe()
	if agg.sum.IsNull() {
		agg.sum = v
	} else {
		agg.sum, _ = evalengine.Add(agg.sum, v)
	}
}

func (agg *aggregationSum) Resolve(rb *sql.RowBuilder) {
	rb.AddVitess(agg.sum)
}

type aggregationMin struct {
	min       sqltypes.Value
	collation collations.ID
}

func (agg *aggregationMin) Add(col int, row sql.Row) {
	v := row.ValueAt(col).ToVitessUnsafe()
	if agg.min.IsNull() {
		agg.min = v
	} else {
		agg.min, _ = evalengine.Min(agg.min, v, agg.collation)
	}
}

func (agg *aggregationMin) Resolve(rb *sql.RowBuilder) {
	rb.AddVitess(agg.min)
}

type aggregationMax struct {
	max       sqltypes.Value
	collation collations.ID
}

func (agg *aggregationMax) Add(col int, row sql.Row) {
	v := row.ValueAt(col).ToVitessUnsafe()
	if agg.max.IsNull() {
		agg.max = v
	} else {
		agg.max, _ = evalengine.Max(agg.max, v, agg.collation)
	}
}

func (agg *aggregationMax) Resolve(rb *sql.RowBuilder) {
	rb.AddVitess(agg.max)
}
