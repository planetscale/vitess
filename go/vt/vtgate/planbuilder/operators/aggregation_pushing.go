/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func tryPushingDownAggregator(ctx *plancontext.PlanningContext, aggregator *Aggregator) (output ops.Operator, applyResult *rewrite.ApplyResult, err error) {
	if aggregator.Pushed {
		return aggregator, rewrite.SameTree, nil
	}
	switch src := aggregator.Source.(type) {
	case *Route:
		// if we have a single sharded route, we can push it down
		output, applyResult, err = pushDownAggregationThroughRoute(ctx, aggregator, src)
	case *ApplyJoin:
		if ctx.DelegateAggregation {
			output, applyResult, err = pushDownAggregationThroughJoin(ctx, aggregator, src)
		}
	case *Filter:
		if ctx.DelegateAggregation {
			output, applyResult, err = pushDownAggregationThroughFilter(ctx, aggregator, src)
		}

	case *SemiJoin:
		if ctx.DelegateAggregation {
			output, applyResult, err = pushDownAggregationThroughSemiJoin(aggregator, src)
		}
	default:
		return aggregator, rewrite.SameTree, nil
	}

	if err != nil {
		return nil, nil, err
	}

	if output == nil {
		return aggregator, rewrite.SameTree, nil
	}

	aggregator.Pushed = true

	return
}

func (a *Aggregator) aggregateTheAggregates() {
	for i := range a.Aggregations {
		aggregateTheAggregate(a, i)
	}
}

func aggregateTheAggregate(a *Aggregator, i int) {
	aggr := a.Aggregations[i]
	switch aggr.OpCode {
	case opcode.AggregateCount, opcode.AggregateCountStar, opcode.AggregateCountDistinct, opcode.AggregateSumDistinct:
		// All count variations turn into SUM above the Route. This is also applied for Sum distinct when it is pushed down.
		// Think of it as we are SUMming together a bunch of distributed COUNTs.
		aggr.OriginalOpCode, aggr.OpCode = aggr.OpCode, opcode.AggregateSum
		a.Aggregations[i] = aggr
	}
}

func pushDownAggregationThroughRoute(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	route *Route,
) (ops.Operator, *rewrite.ApplyResult, error) {
	// If the route is single-shard, or we are grouping by sharding keys, we can just push down the aggregation
	if route.IsSingleShard() || overlappingUniqueVindex(ctx, aggregator.Grouping) {
		return rewrite.Swap(aggregator, route, "push down aggregation under route - remove original")
	}

	if !ctx.DelegateAggregation {
		return nil, nil, nil
	}

	// Create a new aggregator to be placed below the route.
	aggrBelowRoute := aggregator.SplitAggregatorBelowRoute(route.Inputs())
	aggrBelowRoute.Aggregations = nil

	err := pushDownAggregations(ctx, aggregator, aggrBelowRoute)
	if err != nil {
		return nil, nil, err
	}

	// Set the source of the route to the new aggregator placed below the route.
	route.Source = aggrBelowRoute

	if !aggregator.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return aggregator.Source, rewrite.NewTree("push aggregation under route - remove original", aggregator), nil
	}

	return aggregator, rewrite.NewTree("push aggregation under route - keep original", aggregator), nil
}

// pushDownAggregations splits aggregations between the original aggregator and the one we are pushing down
func pushDownAggregations(ctx *plancontext.PlanningContext, aggregator *Aggregator, aggrBelowRoute *Aggregator) error {
	canPushDownDistinctAggr, distinctExpr, err := checkIfWeCanPushDown(ctx, aggregator)
	if err != nil {
		return err
	}

	distinctAggrGroupByAdded := false

	for i, aggr := range aggregator.Aggregations {
		if !aggr.Distinct || canPushDownDistinctAggr {
			aggrBelowRoute.Aggregations = append(aggrBelowRoute.Aggregations, aggr)
			aggregateTheAggregate(aggregator, i)
			continue
		}

		// We handle a distinct aggregation by turning it into a group by and
		// doing the aggregating on the vtgate level instead
		aeDistinctExpr := aeWrap(distinctExpr)
		aggrBelowRoute.Columns[aggr.ColOffset] = aeDistinctExpr

		// We handle a distinct aggregation by turning it into a group by and
		// doing the aggregating on the vtgate level instead
		// Adding to group by can be done only once even though there are multiple distinct aggregation with same expression.
		if !distinctAggrGroupByAdded {
			groupBy := NewGroupBy(distinctExpr, distinctExpr, aeDistinctExpr)
			groupBy.ColOffset = aggr.ColOffset
			aggrBelowRoute.Grouping = append(aggrBelowRoute.Grouping, groupBy)
			distinctAggrGroupByAdded = true
		}
	}

	if !canPushDownDistinctAggr {
		aggregator.DistinctExpr = distinctExpr
	}

	return nil
}

func checkIfWeCanPushDown(ctx *plancontext.PlanningContext, aggregator *Aggregator) (bool, sqlparser.Expr, error) {
	canPushDown := true
	var distinctExpr sqlparser.Expr
	var differentExpr *sqlparser.AliasedExpr

	for _, aggr := range aggregator.Aggregations {
		if !aggr.Distinct {
			continue
		}

		innerExpr := aggr.Func.GetArg()
		if !exprHasUniqueVindex(ctx, innerExpr) {
			canPushDown = false
		}
		if distinctExpr == nil {
			distinctExpr = innerExpr
		}
		if !ctx.SemTable.EqualsExpr(distinctExpr, innerExpr) {
			differentExpr = aggr.Original
		}
	}

	if !canPushDown && differentExpr != nil {
		return false, nil, vterrors.VT12001(fmt.Sprintf("only one DISTINCT aggregation is allowed in a SELECT: %s", sqlparser.String(differentExpr)))
	}

	return canPushDown, distinctExpr, nil
}

func pushDownAggregationThroughFilter(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	filter *Filter,
) (ops.Operator, *rewrite.ApplyResult, error) {

	columnsNeeded := collectColNamesNeeded(ctx, filter)

	// Create a new aggregator to be placed below the route.
	pushedAggr := aggregator.Clone([]ops.Operator{filter.Source}).(*Aggregator)
	pushedAggr.Pushed = false
	pushedAggr.Original = false

withNextColumn:
	for _, col := range columnsNeeded {
		for _, gb := range pushedAggr.Grouping {
			if ctx.SemTable.EqualsExpr(col, gb.SimplifiedExpr) {
				continue withNextColumn
			}
		}
		pushedAggr.addColumnWithoutPushing(aeWrap(col), true)
	}

	// Set the source of the filter to the new aggregator placed below the route.
	filter.Source = pushedAggr

	if !aggregator.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return aggregator.Source, rewrite.NewTree("push aggregation under filter - remove original", aggregator), nil
	}
	aggregator.aggregateTheAggregates()
	return aggregator, rewrite.NewTree("push aggregation under filter - keep original", aggregator), nil
}

// pushDownAggregationThroughSemiJoin is similar to pushDownAggregationThroughJoin, but it's simpler,
// because we don't get any inputs from the RHS, so there are no aggregations or groupings that have
// to be sent to the RHS
//
// We do however need to add the columns used in the subquery coming from the LHS to the grouping.
// That way we get the aggregation grouped by the column we need to use to decide if the row should
// be included in the result set or not.
func pushDownAggregationThroughSemiJoin(rootAggr *Aggregator, join *SemiJoin) (ops.Operator, *rewrite.ApplyResult, error) {
	columnsNeeded := slice.Map(join.LHSColumns, func(colName *sqlparser.ColName) GroupBy {
		return GroupBy{
			Inner:          colName,
			SimplifiedExpr: colName,
			ColOffset:      -1,
			WSOffset:       -1,
		}
	})

	cols := append(columnsNeeded, rootAggr.Grouping...)
	join.LHS = &Aggregator{
		Source:       join.LHS,
		QP:           rootAggr.QP,
		Grouping:     cols,
		Aggregations: slices.Clone(rootAggr.Aggregations),
		Columns:      slices.Clone(rootAggr.Columns),
	}

	rootAggr.aggregateTheAggregates()
	return rootAggr, rewrite.NewTree("push Aggregation under semiJoin", rootAggr), nil
}

func collectColNamesNeeded(ctx *plancontext.PlanningContext, f *Filter) (columnsNeeded []*sqlparser.ColName) {
	for _, p := range f.Predicates {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			col, ok := node.(*sqlparser.ColName)
			if !ok {
				return true, nil
			}
			for _, existing := range columnsNeeded {
				if ctx.SemTable.EqualsExpr(col, existing) {
					return true, nil
				}
			}
			columnsNeeded = append(columnsNeeded, col)
			return true, nil
		}, p)
	}
	return
}

func overlappingUniqueVindex(ctx *plancontext.PlanningContext, groupByExprs []GroupBy) bool {
	for _, groupByExpr := range groupByExprs {
		if exprHasUniqueVindex(ctx, groupByExpr.SimplifiedExpr) {
			return true
		}
	}
	return false
}

func exprHasUniqueVindex(ctx *plancontext.PlanningContext, expr sqlparser.Expr) bool {
	return exprHasVindex(ctx, expr, true)
}

func exprHasVindex(ctx *plancontext.PlanningContext, expr sqlparser.Expr, hasToBeUnique bool) bool {
	col, isCol := expr.(*sqlparser.ColName)
	if !isCol {
		return false
	}
	ts := ctx.SemTable.RecursiveDeps(expr)
	tableInfo, err := ctx.SemTable.TableInfoFor(ts)
	if err != nil {
		return false
	}
	vschemaTable := tableInfo.GetVindexTable()
	for _, vindex := range vschemaTable.ColumnVindexes {
		// TODO: Support composite vindexes (multicol, etc).
		if len(vindex.Columns) > 1 || hasToBeUnique && !vindex.IsUnique() {
			return false
		}
		if col.Name.Equal(vindex.Columns[0]) {
			return true
		}
	}
	return false
}

func extractExpr(expr *sqlparser.AliasedExpr) sqlparser.Expr { return expr.Expr }
