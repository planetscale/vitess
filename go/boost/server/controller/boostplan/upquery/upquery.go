package upquery

import (
	"fmt"

	"vitess.io/vitess/go/boost/server/controller/boostplan/sqlparserx"
	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Upquery struct {
	sel          sqlparser.SelectStatement
	output       []sqlparser.Expr
	cols         []int
	single       string
	intermediate bool
}

func (up *Upquery) String() string {
	return up.single
}

func (up *Upquery) Key() []int {
	return up.cols
}

func (up *Upquery) IsIntermediate() bool {
	if up == nil {
		return false
	}
	return up.intermediate
}

func (up *Upquery) For(keys map[sql.Row]bool) (string, map[string]*querypb.BindVariable) {
	switch len(keys) {
	case 0:
		return up.single, nil
	case 1:
		bvars := make(map[string]*querypb.BindVariable, len(up.cols))
		for key := range keys {
			for i := 0; i < len(up.cols); i++ {
				bvars[sqlparser.DefaultBindVar(i)] = sqltypes.ValueBindVariable(key.ValueAt(i).ToVitessUnsafe())
			}
		}
		return up.single, bvars
	default:
		return up.buildForMany(keys)
	}
}

func New(stmt sqlparser.SelectStatement, cols []int, intermediate bool, mode config.UpqueryMode) (*Upquery, error) {
	upquery := &Upquery{sel: stmt, cols: cols, intermediate: intermediate}
	return upquery, upquery.cacheSingle(mode)
}

func Parse(stmt string, cols []int, intermediate bool, mode config.UpqueryMode) (*Upquery, error) {
	sel, err := sqlparser.Parse(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Upquery: %w", err)
	}

	switch sel := sel.(type) {
	case sqlparser.SelectStatement:
		return New(sel, cols, intermediate, mode)
	default:
		return nil, fmt.Errorf("unexpected upquery type: %T", err)
	}
}

var selectGtidExpr = &sqlparser.AliasedExpr{
	Expr: &sqlparser.Variable{
		Scope: sqlparser.GlobalScope,
		Name:  sqlparser.NewIdentifierCI("gtid_executed"),
	},
}

func addSelectGTID(stmt sqlparser.SelectStatement) error {
	switch sel := stmt.(type) {
	case *sqlparser.Union:
		err := addSelectGTID(sel.Left)
		if err != nil {
			return err
		}
		err = addSelectGTID(sel.Right)
		if err != nil {
			return err
		}
	case *sqlparser.Select:
		sel.SelectExprs = append([]sqlparser.SelectExpr{selectGtidExpr}, sel.SelectExprs...)
	default:
		return fmt.Errorf("unexpected upquery type: %T", stmt)
	}
	return nil
}

func (up *Upquery) cacheSingle(mode config.UpqueryMode) error {
	var err error
	up.output, err = sqlparserx.SelectOutputExprs(up.sel)
	if err != nil {
		return err
	}

	if mode == config.UpqueryMode_SELECT_GTID {
		err := addSelectGTID(up.sel)
		if err != nil {
			return err
		}
	}

	sel := sqlparser.CloneSelectStatement(up.sel)
	for i, key := range up.cols {
		sqlparserx.AddSelectPredicate(sel, &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualOp,
			Left:     up.output[key],
			Right:    &sqlparser.Argument{Name: sqlparser.DefaultBindVar(i)},
		})
	}

	up.single = sqlparser.String(sel)
	return nil
}

func (up *Upquery) buildForMany(keys map[sql.Row]bool) (string, map[string]*querypb.BindVariable) {
	// it's safe to build an OR expression here immediately since we
	// know we have more than 1 key, so we need at least one OR.
	filter := &sqlparser.OrExpr{}
	bvars := make(map[string]*querypb.BindVariable, len(up.cols)*len(keys))
	for key := range keys {
		var exprs []sqlparser.Expr
		for i, col := range up.cols {
			bv := sqlparser.DefaultBindVar(len(bvars))
			bvars[bv] = sqltypes.ValueBindVariable(key.ValueAt(i).ToVitessUnsafe())

			exprs = append(exprs, &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualOp,
				Left:     up.output[col],
				Right:    &sqlparser.Argument{Name: bv},
			})
		}

		colFilter := sqlparser.AndExpressions(exprs...)

		switch {
		case filter.Left == nil:
			filter.Left = colFilter
		case filter.Right == nil:
			filter.Right = colFilter
		default:
			filter = &sqlparser.OrExpr{Left: filter, Right: colFilter}
		}
	}

	sel := sqlparser.CloneSelectStatement(up.sel)
	sqlparserx.AddSelectPredicate(sel, filter)
	return sqlparser.String(sel), bvars
}
