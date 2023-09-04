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
	"maps"
	"slices"
	"strings"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// ApplyJoin is a nested loop join - for each row on the LHS,
// we'll execute the plan on the RHS, feeding data from left to right
type ApplyJoin struct {
	LHS, RHS ops.Operator

	// LeftJoin will be true in the case of an outer join
	LeftJoin bool

	// JoinCols are the columns from the LHS used for the join.
	// These are the same columns pushed on the LHS that are now used in the Vars field
	LHSColumns []*sqlparser.ColName

	// Before offset planning
	Predicate sqlparser.Expr

	// JoinColumns keeps track of what AST expression is represented in the Columns array
	JoinColumns []JoinColumn

	// JoinPredicates are join predicates that have been broken up into left hand side and right hand side parts.
	JoinPredicates []JoinColumn

	// After offset planning

	// Columns stores the column indexes of the columns coming from the left and right side
	// negative value comes from LHS and positive from RHS
	Columns []int

	// Vars are the arguments that need to be copied from the LHS to the RHS
	Vars map[string]int
}

// JoinColumn is where we store information about columns passing through the join operator
// It can be in one of three possible configurations:
//   - Pure left
//     We are projecting a column that comes from the left. The RHSExpr will be nil for these
//   - Pure right
//     We are projecting a column that comes from the right. The LHSExprs will be empty for these
//   - Mix of data from left and right
//     Here we need to transmit columns from the LHS to the RHS,
//     so they can be used for the result of this expression that is using data from both sides.
//     All fields will be used for these
type JoinColumn struct {
	Original *sqlparser.AliasedExpr // this is the original expression being passed through
	BvNames  []string               // the BvNames and LHSCols line up
	LHSExprs []sqlparser.Expr
	RHSExpr  sqlparser.Expr
	GroupBy  bool // if this is true, we need to push this down to our inputs with addToGroupBy set to true
}

func NewApplyJoin(lhs, rhs ops.Operator, predicate sqlparser.Expr, leftOuterJoin bool) *ApplyJoin {
	return &ApplyJoin{
		LHS:       lhs,
		RHS:       rhs,
		Vars:      map[string]int{},
		Predicate: predicate,
		LeftJoin:  leftOuterJoin,
	}
}

// Clone implements the Operator interface
func (aj *ApplyJoin) Clone(inputs []ops.Operator) ops.Operator {
	return &ApplyJoin{
		LHS:            inputs[0],
		RHS:            inputs[1],
		Columns:        slices.Clone(aj.Columns),
		JoinColumns:    slices.Clone(aj.JoinColumns),
		JoinPredicates: slices.Clone(aj.JoinPredicates),
		Vars:           maps.Clone(aj.Vars),
		LeftJoin:       aj.LeftJoin,
		Predicate:      sqlparser.CloneExpr(aj.Predicate),
		LHSColumns:     slices.Clone(aj.LHSColumns),
	}
}

func (aj *ApplyJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	return AddPredicate(ctx, aj, expr, false, newFilter)
}

// Inputs implements the Operator interface
func (aj *ApplyJoin) Inputs() []ops.Operator {
	return []ops.Operator{aj.LHS, aj.RHS}
}

// SetInputs implements the Operator interface
func (aj *ApplyJoin) SetInputs(inputs []ops.Operator) {
	aj.LHS, aj.RHS = inputs[0], inputs[1]
}

var _ JoinOp = (*ApplyJoin)(nil)

func (aj *ApplyJoin) GetLHS() ops.Operator {
	return aj.LHS
}

func (aj *ApplyJoin) GetRHS() ops.Operator {
	return aj.RHS
}

func (aj *ApplyJoin) SetLHS(operator ops.Operator) {
	aj.LHS = operator
}

func (aj *ApplyJoin) SetRHS(operator ops.Operator) {
	aj.RHS = operator
}

func (aj *ApplyJoin) MakeInner() {
	aj.LeftJoin = false
}

func (aj *ApplyJoin) IsInner() bool {
	return !aj.LeftJoin
}

func (aj *ApplyJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error {
	aj.Predicate = ctx.SemTable.AndExpressions(expr, aj.Predicate)

	col, err := BreakExpressionInLHSandRHS(ctx, expr, TableID(aj.LHS))
	if err != nil {
		return err
	}
	aj.JoinPredicates = append(aj.JoinPredicates, col)
	rhs, err := aj.RHS.AddPredicate(ctx, col.RHSExpr)
	if err != nil {
		return err
	}
	aj.RHS = rhs

	return nil
}

func (aj *ApplyJoin) pushColLeft(ctx *plancontext.PlanningContext, e *sqlparser.AliasedExpr, addToGroupBy bool) (int, error) {
	offset, err := aj.LHS.AddColumn(ctx, true, addToGroupBy, e)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (aj *ApplyJoin) pushColRight(ctx *plancontext.PlanningContext, e *sqlparser.AliasedExpr, addToGroupBy bool) (int, error) {
	offset, err := aj.RHS.AddColumn(ctx, true, addToGroupBy, e)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (aj *ApplyJoin) GetColumns(*plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return slice.Map(aj.JoinColumns, joinColumnToAliasedExpr), nil
}

func (aj *ApplyJoin) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return transformColumnsToSelectExprs(ctx, aj)
}

func (aj *ApplyJoin) GetOrdering() ([]ops.OrderBy, error) {
	return aj.LHS.GetOrdering()
}

func joinColumnToAliasedExpr(c JoinColumn) *sqlparser.AliasedExpr {
	return c.Original
}

func joinColumnToExpr(column JoinColumn) sqlparser.Expr {
	return column.Original.Expr
}

func (aj *ApplyJoin) getJoinColumnFor(ctx *plancontext.PlanningContext, e *sqlparser.AliasedExpr, addToGroupBy bool) (col JoinColumn, err error) {
	defer func() {
		col.Original = e
	}()
	lhs := TableID(aj.LHS)
	rhs := TableID(aj.RHS)
	both := lhs.Merge(rhs)
	expr := e.Expr
	deps := ctx.SemTable.RecursiveDeps(expr)
	col.GroupBy = addToGroupBy

	switch {
	case deps.IsSolvedBy(lhs):
		col.LHSExprs = []sqlparser.Expr{expr}
	case deps.IsSolvedBy(rhs):
		col.RHSExpr = expr
	case deps.IsSolvedBy(both):
		col, err = BreakExpressionInLHSandRHS(ctx, expr, TableID(aj.LHS))
		if err != nil {
			return JoinColumn{}, err
		}
	default:
		return JoinColumn{}, vterrors.VT13002(sqlparser.String(e))
	}

	return
}

func (aj *ApplyJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, _ bool) (int, error) {
	offset, found := canReuseColumn(ctx, aj.JoinColumns, expr, joinColumnToExpr)
	if !found {
		return -1, nil
	}
	return offset, nil
}

func (aj *ApplyJoin) AddColumn(
	ctx *plancontext.PlanningContext,
	reuse bool,
	groupBy bool,
	expr *sqlparser.AliasedExpr,
) (int, error) {
	if reuse {
		offset, err := aj.FindCol(ctx, expr.Expr, false)
		if err != nil {
			return 0, err
		}
		if offset != -1 {
			return offset, nil
		}
	}
	col, err := aj.getJoinColumnFor(ctx, expr, groupBy)
	if err != nil {
		return 0, err
	}
	offset := len(aj.JoinColumns)
	aj.JoinColumns = append(aj.JoinColumns, col)
	return offset, nil
}

func (aj *ApplyJoin) planOffsets(ctx *plancontext.PlanningContext) (err error) {
	for _, col := range aj.JoinColumns {
		// Read the type description for JoinColumn to understand the following code
		for i, lhsExpr := range col.LHSExprs {
			offset, err := aj.pushColLeft(ctx, aeWrap(lhsExpr), col.GroupBy)
			if err != nil {
				return err
			}
			if col.RHSExpr == nil {
				// if we don't have an RHS expr, it means that this is a pure LHS expression
				aj.addOffset(-offset - 1)
			} else {
				aj.Vars[col.BvNames[i]] = offset
			}
		}
		if col.RHSExpr != nil {
			offset, err := aj.pushColRight(ctx, aeWrap(col.RHSExpr), col.GroupBy)
			if err != nil {
				return err
			}
			aj.addOffset(offset + 1)
		}
	}

	for _, col := range aj.JoinPredicates {
		for i, lhsExpr := range col.LHSExprs {
			offset, err := aj.pushColLeft(ctx, aeWrap(lhsExpr), false)
			if err != nil {
				return err
			}
			aj.Vars[col.BvNames[i]] = offset
		}
		lhsColumns := slice.Map(col.LHSExprs, func(from sqlparser.Expr) *sqlparser.ColName {
			col, ok := from.(*sqlparser.ColName)
			if !ok {
				// todo: there is no good reason to keep this limitation around
				err = vterrors.VT13001("joins can only compare columns: %s", sqlparser.String(from))
			}
			return col
		})
		if err != nil {
			return err
		}
		aj.LHSColumns = append(aj.LHSColumns, lhsColumns...)
	}
	return nil
}

func (aj *ApplyJoin) addOffset(offset int) {
	aj.Columns = append(aj.Columns, offset)
}

func (aj *ApplyJoin) ShortDescription() string {
	pred := sqlparser.String(aj.Predicate)
	columns := slice.Map(aj.JoinColumns, func(from JoinColumn) string {
		return sqlparser.String(from.Original)
	})
	return fmt.Sprintf("on %s columns: %s", pred, strings.Join(columns, ", "))
}

func (jc JoinColumn) IsPureLeft() bool {
	return jc.RHSExpr == nil
}

func (jc JoinColumn) IsPureRight() bool {
	return len(jc.LHSExprs) == 0
}

func (jc JoinColumn) IsMixedLeftAndRight() bool {
	return len(jc.LHSExprs) > 0 && jc.RHSExpr != nil
}

// splitProjection creates JoinPredicates for all projections,
// and pushes down columns as needed between the LHS and RHS of a join
func (aj *ApplyJoin) splitProjection(
	ctx *plancontext.PlanningContext,
	lhs, rhs *projector,
	in ProjExpr,
	colName *sqlparser.AliasedExpr,
) error {
	expr := in.GetExpr()

	// Check if the current expression can reuse an existing column in the ApplyJoin.
	if _, found := canReuseColumn(ctx, aj.JoinColumns, expr, joinColumnToExpr); found {
		return nil
	}

	// Get a JoinColumn for the current expression.
	col, err := aj.getJoinColumnFor(ctx, colName, false)
	if err != nil {
		return err
	}

	// Update the left and right child columns and names based on the JoinColumn type.
	switch {
	case col.IsPureLeft():
		lhs.add(in, colName)
	case col.IsPureRight():
		rhs.add(in, colName)
	case col.IsMixedLeftAndRight():
		for _, lhsExpr := range col.LHSExprs {
			lhs.add(&UnexploredExpression{E: lhsExpr}, aeWrap(lhsExpr))
		}
		rhs.add(&UnexploredExpression{E: col.RHSExpr}, &sqlparser.AliasedExpr{Expr: col.RHSExpr, As: colName.As})
	}

	// Add the new JoinColumn to the ApplyJoin's JoinPredicates.
	aj.JoinColumns = append(aj.JoinColumns, col)
	return nil
}
