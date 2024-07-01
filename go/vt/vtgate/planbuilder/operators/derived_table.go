/*
Copyright 2024 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// DerivedTable represents a derived table operator.
// It's used during the planning phase to represent a derived table for AST column lookups.
// Once we have done offset planning, DerivedTable is only used for
// the part of the plan that can be pushed down to the underlying MySQL
// When pushing down expressions, they have to be rewritten to refer to the
// underlying table expressions and not the exposed derived table column names.
type DerivedTableOp struct {
	Source  Operator
	TableID semantics.TableSet  // TableID is the table set that this derived table introduces.
	TblInfo semantics.TableInfo // TblInfo is the table info for the derived table.
	Alias   string              // Alias is the alias of the derived table.
	Columns sqlparser.Columns   // Columns is the list of columns in the derived table.
}

var _ Operator = (*DerivedTableOp)(nil)

func (dt *DerivedTableOp) Clone(inputs []Operator) Operator {
	klone := *dt
	klone.Source = inputs[0]
	return &klone
}

func (dt *DerivedTableOp) Inputs() []Operator {
	return []Operator{dt.Source}
}

func (dt *DerivedTableOp) SetInputs(operators []Operator) {
	dt.Source = operators[0]
}

func (dt *DerivedTableOp) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	src := dt.Source.AddPredicate(ctx, semantics.RewriteDerivedTableExpression(expr, dt.TblInfo))
	dt.Source = src
	return dt
}

func (dt *DerivedTableOp) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	// TODO implement me
	panic("implement me")
}

func (dt *DerivedTableOp) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	// TODO implement me
	panic("implement me")
}

func (dt *DerivedTableOp) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	// TODO implement me
	panic("implement me")
}

func (dt *DerivedTableOp) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return slice.Map(dt.Columns, func(col sqlparser.IdentifierCI) *sqlparser.AliasedExpr {
		return aeWrap(&sqlparser.ColName{
			Name:      col,
			Qualifier: sqlparser.NewTableName(dt.Alias),
		})
	})
}

func (dt *DerivedTableOp) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	// TODO implement me
	panic("implement me")
}

func (dt *DerivedTableOp) ShortDescription() string {
	// TODO implement me
	panic("implement me")
}

func (dt *DerivedTableOp) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	// TODO implement me
	panic("implement me")
}

func (dt *DerivedTableOp) String() string {
	return fmt.Sprintf("DERIVED %s(%s)", dt.Alias, sqlparser.String(dt.Columns))
}

func (dt *DerivedTableOp) RewriteExpression(ctx *plancontext.PlanningContext, expr sqlparser.Expr) sqlparser.Expr {
	if dt == nil {
		return expr
	}
	tableInfo, err := ctx.SemTable.TableInfoFor(dt.TableID)
	if err != nil {
		panic(err)
	}
	return semantics.RewriteDerivedTableExpression(expr, tableInfo)
}

func (dt *DerivedTableOp) introducesTableID() semantics.TableSet {
	if dt == nil {
		return semantics.EmptyTableSet()
	}
	return dt.TableID
}
