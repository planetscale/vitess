/*
Copyright 2022 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// SemiJoin is a special operator that is used to represent a semi-join. A semi-join is a join that is used to
	// filter the LHS based on the RHS. The RHS is a subquery that is correlated to the LHS. The RHS is executed
	// for each row in the LHS. If at least one row is returned from the RHS, the LHS row is passed through
	// otherwise it is filtered out.
	SemiJoin struct {
		LHS, RHS  ops.Operator
		Extracted *sqlparser.ExtractedSubquery

		// JoinCols are the columns from the LHS used for the join.
		// These are the same columns pushed on the LHS that are now used in the Vars field
		LHSColumns []*sqlparser.ColName

		// arguments that need to be copied from the outer to inner
		Vars map[string]int

		noPredicates
	}

	SubQueryOp struct {
		Outer, Inner ops.Operator
		Extracted    *sqlparser.ExtractedSubquery

		noColumns
		noPredicates
	}
)

// Clone implements the Operator interface
func (s *SubQueryOp) Clone(inputs []ops.Operator) ops.Operator {
	result := &SubQueryOp{
		Outer:     inputs[0],
		Inner:     inputs[1],
		Extracted: s.Extracted,
	}
	return result
}

func (s *SubQueryOp) GetOrdering() ([]ops.OrderBy, error) {
	return s.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (s *SubQueryOp) Inputs() []ops.Operator {
	return []ops.Operator{s.Outer, s.Inner}
}

// SetInputs implements the Operator interface
func (s *SubQueryOp) SetInputs(ops []ops.Operator) {
	s.Outer, s.Inner = ops[0], ops[1]
}

func (s *SubQueryOp) ShortDescription() string {
	return ""
}

// Clone implements the Operator interface
func (c *SemiJoin) Clone(inputs []ops.Operator) ops.Operator {
	columns := make([]*sqlparser.ColName, len(c.LHSColumns))
	copy(columns, c.LHSColumns)
	vars := make(map[string]int, len(c.Vars))
	for k, v := range c.Vars {
		vars[k] = v
	}

	result := &SemiJoin{
		LHS:        inputs[0],
		RHS:        inputs[1],
		Extracted:  c.Extracted,
		LHSColumns: columns,
		Vars:       vars,
	}
	return result
}

func (c *SemiJoin) GetOrdering() ([]ops.OrderBy, error) {
	return c.LHS.GetOrdering()
}

// Inputs implements the Operator interface
func (c *SemiJoin) Inputs() []ops.Operator {
	return []ops.Operator{c.LHS, c.RHS}
}

// SetInputs implements the Operator interface
func (c *SemiJoin) SetInputs(ops []ops.Operator) {
	c.LHS, c.RHS = ops[0], ops[1]
}

func (c *SemiJoin) ShortDescription() string {
	return ""
}

func (c *SemiJoin) AddColumns(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	return c.LHS.AddColumns(ctx, reuseExisting, addToGroupBy, exprs)
}

func (c *SemiJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return c.LHS.FindCol(ctx, expr, underRoute)
}

func (c *SemiJoin) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return c.LHS.GetColumns(ctx)
}

func (c *SemiJoin) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return c.LHS.GetSelectExprs(ctx)
}
