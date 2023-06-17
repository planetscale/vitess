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
	CorrelatedSubQueryOp struct {
		Outer, Inner ops.Operator
		Extracted    *sqlparser.ExtractedSubquery

		// JoinCols are the columns from the LHS used for the join.
		// These are the same columns pushed on the LHS that are now used in the Vars field
		LHSColumns []*sqlparser.ColName

		// arguments that need to be copied from the outer to inner
		Vars map[string]int
	}

	SubQueryOp struct {
		Outer, Inner ops.Operator
		Extracted    *sqlparser.ExtractedSubquery
	}
)

func (c *CorrelatedSubQueryOp) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return c.Outer.GetColumns()
}

func (c *CorrelatedSubQueryOp) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := c.Outer.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	c.Outer = newSrc
	return c, nil
}

func (c *CorrelatedSubQueryOp) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, reuseExisting, addToGroupBy bool) (ops.Operator, int, error) {
	newSrc, offset, err := c.Outer.AddColumn(ctx, expr, reuseExisting, addToGroupBy)
	if err != nil {
		return nil, 0, err
	}
	c.Outer = newSrc
	return c, offset, nil
}
func (s *SubQueryOp) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return s.Outer.GetColumns()
}

func (s *SubQueryOp) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := s.Outer.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	s.Outer = newSrc
	return s, nil
}

func (s *SubQueryOp) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, reuseExisting, addToGroupBy bool) (ops.Operator, int, error) {
	newSrc, offset, err := s.Outer.AddColumn(ctx, expr, reuseExisting, addToGroupBy)
	if err != nil {
		return nil, 0, err
	}
	s.Outer = newSrc
	return s, offset, nil
}

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
func (c *CorrelatedSubQueryOp) Clone(inputs []ops.Operator) ops.Operator {
	columns := make([]*sqlparser.ColName, len(c.LHSColumns))
	copy(columns, c.LHSColumns)
	vars := make(map[string]int, len(c.Vars))
	for k, v := range c.Vars {
		vars[k] = v
	}

	result := &CorrelatedSubQueryOp{
		Outer:      inputs[0],
		Inner:      inputs[1],
		Extracted:  c.Extracted,
		LHSColumns: columns,
		Vars:       vars,
	}
	return result
}

func (c *CorrelatedSubQueryOp) GetOrdering() ([]ops.OrderBy, error) {
	return c.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (c *CorrelatedSubQueryOp) Inputs() []ops.Operator {
	return []ops.Operator{c.Outer, c.Inner}
}

// SetInputs implements the Operator interface
func (c *CorrelatedSubQueryOp) SetInputs(ops []ops.Operator) {
	c.Outer, c.Inner = ops[0], ops[1]
}

func (c *CorrelatedSubQueryOp) ShortDescription() string {
	return ""
}
