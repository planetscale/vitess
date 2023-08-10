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
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// SemiJoin is an operator that is used to represent a semi-join. A semi-join is a join that is used to
// filter the LHS based on the RHS. The RHS is a subquery that is correlated to the LHS. The RHS is executed
// for each row in the LHS. If at least one row is returned from the RHS, the LHS row is passed through
// otherwise it is filtered out.
type SemiJoin struct {
	LHS, RHS ops.Operator

	// these are the bindvars we will need, before offset planning
	bindVars map[string]*sqlparser.ColName

	// these are the same vars as the bindVars, but after offset planning
	Vars map[string]int

	noPredicates
}

// Clone implements the Operator interface
func (c *SemiJoin) Clone(inputs []ops.Operator) ops.Operator {
	return &SemiJoin{
		LHS:      inputs[0],
		RHS:      inputs[1],
		Vars:     maps.Clone(c.Vars),
		bindVars: maps.Clone(c.bindVars),
	}
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

func (c *SemiJoin) planOffsets(ctx *plancontext.PlanningContext) error {
	c.Vars = make(map[string]int, len(c.bindVars))
	for bvname, col := range c.bindVars {
		coloffset, err := c.LHS.AddColumns(ctx, true, []bool{false}, []*sqlparser.AliasedExpr{aeWrap(col)})
		if err != nil {
			return err
		}
		if len(coloffset) != 1 {
			return vterrors.VT13001("should have only one column offset")
		}
		c.Vars[bvname] = coloffset[0]
	}
	return nil
}
