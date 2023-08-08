package operators

import (
	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type DerivedTable struct {
	Source ops.Operator

	TableID semantics.TableSet
	Alias   string
}

func (d *DerivedTable) Clone(inputs []ops.Operator) ops.Operator {
	klone := *d
	klone.Source = inputs[0]
	return &klone
}

func (d *DerivedTable) Inputs() []ops.Operator {
	return []ops.Operator{d.Source}
}

func (d *DerivedTable) SetInputs(operators []ops.Operator) {
	d.Source = operators[0]
}

func (d *DerivedTable) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	vt, err := ctx.SemTable.TableInfoFor(d.TableID)
	if err != nil {
		return nil, err
	}
	expr = semantics.RewriteDerivedTableExpression(expr, vt)

	newSrc, err := d.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	d.Source = newSrc
	return d, nil
}

func (d *DerivedTable) AddColumns(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	vt, err := ctx.SemTable.TableInfoFor(d.TableID)
	if err != nil {
		return nil, err
	}

	rewritten := slice.Map(exprs, func(expr *sqlparser.AliasedExpr) *sqlparser.AliasedExpr {
		return &sqlparser.AliasedExpr{
			As:   expr.As,
			Expr: semantics.RewriteDerivedTableExpression(expr.Expr, vt),
		}
	})

	return d.Source.AddColumns(ctx, reuseExisting, addToGroupBy, rewritten)
}

func (d *DerivedTable) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	vt, err := ctx.SemTable.TableInfoFor(d.TableID)
	if err != nil {
		return 0, err
	}
	return d.Source.FindCol(ctx, semantics.RewriteDerivedTableExpression(expr, vt), underRoute)
}

func (d *DerivedTable) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	//TODO implement me
	panic("implement me")
}

func (d *DerivedTable) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	//TODO implement me
	panic("implement me")
}

func (d *DerivedTable) ShortDescription() string {
	return "DerivedTable"
}

func (d *DerivedTable) GetOrdering() ([]ops.OrderBy, error) {
	//TODO implement me
	panic("implement me")
}
