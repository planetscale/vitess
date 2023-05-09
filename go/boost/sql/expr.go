package sql

import (
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type EvalExpr struct {
	Eval evalengine.Expr
	Expr sqlparser.Expr
}

func (expr *EvalExpr) ToProto() *Expr {
	return &Expr{Expr: sqlparser.String(expr.Expr)}
}

func EvalExprFromProto(f *Expr) EvalExpr {
	expr, err := sqlparser.ParseExpr(f.Expr)
	if err != nil {
		panic(fmt.Errorf("should not fail to deserialize SQL expression %s (%v)", f.Expr, err))
	}
	e := EvalExpr{Expr: expr}
	if err = e.Compile(nil); err != nil {
		panic(err)
	}
	return e
}

func (expr *EvalExpr) Compile(types evalengine.TypeResolver) (err error) {
	expr.Eval, err = evalengine.Translate(expr.Expr, &evalengine.Config{
		ResolveColumn: func(_ *sqlparser.ColName) (int, error) {
			panic("column names should have been resolved to offsets")
		},
		ResolveType: types,
		Collation:   collations.CollationUtf8mb4ID,
	})
	return
}

func EvalExprsToProto(filters []EvalExpr) (pb []*Expr) {
	for _, f := range filters {
		pb = append(pb, f.ToProto())
	}
	return
}

func EvalExprsFromProto(pb []*Expr) (exprs []EvalExpr) {
	for _, f := range pb {
		exprs = append(exprs, EvalExprFromProto(f))
	}
	return
}
