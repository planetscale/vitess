/*
Copyright 2021 The Vitess Authors.

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
	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// While building the initial operator tree, we use the LogicalSubQuery operator for all
	// types of sub queries. Later, during the operator rewriting phase, we convert the
	// LogicalSubQuery to the appropriate type of subquery based on the type of the subquery -
	// SemiJoin, UncorrelatedSubQuery, or as CorrelatedSubQuery
	LogicalSubQuery struct {
		Outer ops.Operator
		Inner []*SubQueryInner

		noColumns
		noPredicates
	}

	// CorrelatedSubQuery is a subquery (inner/RHS) with a correlation with the outer query (LHS)
	// This means that the subquery needs to be executed once per row of the outer query
	CorrelatedSubQuery struct {
		LHS, RHS ops.Operator

		noPredicates
	}

	// SubQueryInner stores the subquery information for a select statement
	SubQueryInner struct {
		// Inner is the Operator inside the parenthesis of the subquery.
		// i.e: select (select 1 union select 1), the Inner here would be
		// of type Concatenate since we have a Union.
		Inner ops.Operator

		// ExtractedSubquery contains all information we need about this subquery
		ExtractedSubquery *sqlparser.ExtractedSubquery

		noColumns
		noPredicates
	}

	// UncorrelatedSubQuery is a subquery that does not have any correlation with the outer query
	// This means that the subquery can be executed once for the entire outer query, instead of once per row
	UncorrelatedSubQuery struct {
		LHS, RHS  ops.Operator
		Extracted *sqlparser.ExtractedSubquery

		noPredicates
	}

	SubQuery interface {
		ops.Operator
		SetOuter(outer ops.Operator)
	}
)

func (l *LogicalSubQuery) Clone(inputs []ops.Operator) ops.Operator {
	inners, err := slice.MapWithError(inputs[1:], func(i ops.Operator) (*SubQueryInner, error) {
		sqi, ok := i.(*SubQueryInner)
		if !ok {
			return nil, vterrors.VT13001(fmt.Sprintf("unexpected operator type %T", i))
		}
		return sqi, nil
	})
	if err != nil {
		panic(err.Error())
	}

	return &LogicalSubQuery{
		Outer: inputs[0],
		Inner: inners,
	}
}

func (l *LogicalSubQuery) Inputs() []ops.Operator {
	operators := []ops.Operator{l.Outer}
	for _, inner := range l.Inner {
		operators = append(operators, inner)
	}
	return operators
}

func (l *LogicalSubQuery) SetInputs(operators []ops.Operator) {
	//TODO implement me
	panic("implement me")
}

func (l *LogicalSubQuery) ShortDescription() string {
	return ""
}

func (l *LogicalSubQuery) GetOrdering() ([]ops.OrderBy, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LogicalSubQuery) SetOuter(outer ops.Operator) {
	l.Outer = outer
}

func (s *UncorrelatedSubQuery) SetOuter(outer ops.Operator) {
	s.LHS = outer
}

func (s *CorrelatedSubQuery) SetOuter(outer ops.Operator) {
	s.LHS = outer
}

var _ ops.Operator = (*CorrelatedSubQuery)(nil)
var _ ops.Operator = (*LogicalSubQuery)(nil)
var _ ops.Operator = (*SubQueryInner)(nil)

// Clone implements the Operator interface
func (s *SubQueryInner) Clone(inputs []ops.Operator) ops.Operator {
	return &SubQueryInner{
		Inner:             inputs[0],
		ExtractedSubquery: s.ExtractedSubquery,
	}
}

func (s *SubQueryInner) GetOrdering() ([]ops.OrderBy, error) {
	return s.Inner.GetOrdering()
}

// Inputs implements the Operator interface
func (s *SubQueryInner) Inputs() []ops.Operator {
	return []ops.Operator{s.Inner}
}

// SetInputs implements the Operator interface
func (s *SubQueryInner) SetInputs(ops []ops.Operator) {
	s.Inner = ops[0]
}

// Clone implements the Operator interface
func (s *CorrelatedSubQuery) Clone(inputs []ops.Operator) ops.Operator {
	result := &CorrelatedSubQuery{
		LHS: inputs[0],
		RHS: inputs[1],
	}
	return result
}

func (s *CorrelatedSubQuery) GetOrdering() ([]ops.OrderBy, error) {
	return s.LHS.GetOrdering()
}

// Inputs implements the Operator interface
func (s *CorrelatedSubQuery) Inputs() []ops.Operator {
	return []ops.Operator{s.LHS, s.RHS}
}

// SetInputs implements the Operator interface
func (s *CorrelatedSubQuery) SetInputs(ops []ops.Operator) {
	s.LHS = ops[0]
}

func createSubqueryFromStatement(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (SubQuery, error) {
	if len(ctx.SemTable.SubqueryMap[stmt]) == 0 {
		return nil, nil
	}
	subq := &LogicalSubQuery{}
	for _, sq := range ctx.SemTable.SubqueryMap[stmt] {
		opInner, err := translateQueryToOp(ctx, sq.Subquery.Select)
		if err != nil {
			return nil, err
		}

		subq.Inner = append(subq.Inner, &SubQueryInner{
			ExtractedSubquery: sq,
			Inner:             opInner,
		})
	}
	return subq, nil
}

func (s *CorrelatedSubQuery) ShortDescription() string {
	return ""
}

func (s *SubQueryInner) ShortDescription() string {
	return ""
}

// Clone implements the Operator interface
func (s *UncorrelatedSubQuery) Clone(inputs []ops.Operator) ops.Operator {
	result := &UncorrelatedSubQuery{
		LHS:       inputs[0],
		RHS:       inputs[1],
		Extracted: s.Extracted,
	}
	return result
}

func (s *UncorrelatedSubQuery) GetOrdering() ([]ops.OrderBy, error) {
	return s.LHS.GetOrdering()
}

// Inputs implements the Operator interface
func (s *UncorrelatedSubQuery) Inputs() []ops.Operator {
	return []ops.Operator{s.LHS, s.RHS}
}

// SetInputs implements the Operator interface
func (s *UncorrelatedSubQuery) SetInputs(ops []ops.Operator) {
	s.LHS, s.RHS = ops[0], ops[1]
}

func (s *UncorrelatedSubQuery) ShortDescription() string {
	return s.Extracted.OpCode.String()
}

func (s *UncorrelatedSubQuery) AddColumns(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	return s.LHS.AddColumns(ctx, reuseExisting, addToGroupBy, exprs)
}

func (s *UncorrelatedSubQuery) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return s.LHS.FindCol(ctx, expr, underRoute)
}

func (s *UncorrelatedSubQuery) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return s.LHS.GetColumns(ctx)
}

func (s *UncorrelatedSubQuery) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return s.LHS.GetSelectExprs(ctx)
}

func (s *CorrelatedSubQuery) AddColumns(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	//TODO implement me
	panic("implement me")
}

func (s *CorrelatedSubQuery) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (s *CorrelatedSubQuery) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	//TODO implement me
	panic("implement me")
}

func (s *CorrelatedSubQuery) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	//TODO implement me
	panic("implement me")
}
