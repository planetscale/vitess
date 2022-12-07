package sqlparserx

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

func SelectOutputExprs(stmt sqlparser.SelectStatement) ([]sqlparser.Expr, error) {
	sel := sqlparser.GetFirstSelect(stmt)
	var exprs []sqlparser.Expr
	for _, expr := range sel.SelectExprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("unexpected type in Select statement: %T", expr)
		}
		exprs = append(exprs, ae.Expr)
	}
	return exprs, nil
}

func AddSelectPredicate(stmt sqlparser.SelectStatement, expr sqlparser.Expr) {
	sel := stmt.(*sqlparser.Select)
	if sel.Where == nil {
		sel.AddWhere(expr)
		return
	}
	for _, exp := range sqlparser.SplitAndExpression(nil, expr) {
		sel.AddWhere(exp)
	}
}
