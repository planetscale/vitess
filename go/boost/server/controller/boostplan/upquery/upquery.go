package upquery

import (
	"fmt"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/server/controller/boostplan/sqlparserx"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Upquery struct {
	sel    sqlparser.SelectStatement
	output []sqlparser.Expr
	cols   []int
	single string
}

func (up *Upquery) String() string {
	return up.single
}

func (up *Upquery) For(keys []boostpb.Row) (string, map[string]*querypb.BindVariable) {
	switch len(keys) {
	case 0:
		return up.single, nil
	case 1:
		bvars := make(map[string]*querypb.BindVariable, len(up.cols))
		key := keys[0]
		for i := 0; i < len(up.cols); i++ {
			bvars[sqlparser.DefaultBindVar(i)] = sqltypes.ValueBindVariable(key.ValueAt(i).ToVitessUnsafe())
		}
		return up.single, bvars
	default:
		return up.buildForMany(keys)
	}
}

func New(stmt sqlparser.SelectStatement, cols []int, mode boostpb.UpqueryMode) (*Upquery, error) {
	upquery := &Upquery{sel: stmt, cols: cols}
	return upquery, upquery.cacheSingle(mode)
}

func Parse(stmt string, cols []int, mode boostpb.UpqueryMode) (*Upquery, error) {
	sel, err := sqlparser.Parse(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Upquery: %w", err)
	}

	switch sel := sel.(type) {
	case sqlparser.SelectStatement:
		return New(sel, cols, mode)
	default:
		return nil, fmt.Errorf("unexpected upquery type: %T", err)
	}
}

func (up *Upquery) cacheSingle(mode boostpb.UpqueryMode) error {
	var err error
	up.output, err = sqlparserx.SelectOutputExprs(up.sel)
	if err != nil {
		return err
	}

	if mode == boostpb.UpqueryMode_SELECT_GTID {
		gtid := &sqlparser.AliasedExpr{
			Expr: &sqlparser.Variable{
				Scope: sqlparser.GlobalScope,
				Name:  sqlparser.NewIdentifierCI("gtid_executed"),
			},
		}

		// TODO: add the select GTID to all sides of the union
		sel := sqlparser.GetFirstSelect(up.sel)
		sel.SelectExprs = append([]sqlparser.SelectExpr{gtid}, sel.SelectExprs...)
	}

	sel := sqlparser.CloneSelectStatement(up.sel)
	for i, key := range up.cols {
		sqlparserx.AddSelectPredicate(sel, &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualOp,
			Left:     up.output[key],
			Right:    sqlparser.Argument(sqlparser.DefaultBindVar(i)),
		})
	}

	up.single = sqlparser.String(sel)
	return nil
}

func (up *Upquery) buildForMany(keys []boostpb.Row) (string, map[string]*querypb.BindVariable) {
	filter := &sqlparser.OrExpr{}
	bvars := make(map[string]*querypb.BindVariable, len(up.cols)*len(keys))
	for _, key := range keys {
		for i, col := range up.cols {
			bv := sqlparser.DefaultBindVar(len(bvars))
			bvars[bv] = sqltypes.ValueBindVariable(key.ValueAt(i).ToVitessUnsafe())

			expr := &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualOp,
				Left:     up.output[col],
				Right:    sqlparser.Argument(bv),
			}

			switch {
			case filter.Left == nil:
				filter.Left = expr
			case filter.Right == nil:
				filter.Right = expr
			default:
				filter = &sqlparser.OrExpr{Left: filter, Right: expr}
			}
		}
	}

	sel := sqlparser.CloneSelectStatement(up.sel)
	sqlparserx.AddSelectPredicate(sel, filter)
	return sqlparser.String(sel), bvars
}
