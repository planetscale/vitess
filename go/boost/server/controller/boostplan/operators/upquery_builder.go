package operators

import (
	"fmt"
	"sort"

	"vitess.io/vitess/go/boost/server/controller/boostplan/sqlparserx"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type queryBuilder struct {
	ctx        *PlanContext
	sel        sqlparser.SelectStatement
	tableNames map[string]int
	aliasMap   map[semantics.TableSet]string
}

func newQueryBuilder(ctx *PlanContext) *queryBuilder {
	return &queryBuilder{
		ctx:        ctx,
		tableNames: map[string]int{},
		aliasMap:   map[semantics.TableSet]string{},
	}
}

func (qb *queryBuilder) sortTables() {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		sel, isSel := node.(*sqlparser.Select)
		if !isSel {
			return true, nil
		}
		ts := &tableSorter{
			sel: sel,
			tbl: qb.ctx.SemTable,
		}
		sort.Sort(ts)
		return true, nil
	}, qb.sel)

}

func (qb *queryBuilder) addTable(db, tableName, alias string, tableID semantics.TableSet, hints sqlparser.IndexHints) {
	tableExpr := sqlparser.TableName{
		Name:      sqlparser.NewIdentifierCS(tableName),
		Qualifier: sqlparser.NewIdentifierCS(db),
	}
	qb.addTableExpr(alias, tableID, tableExpr, hints)
}

func (qb *queryBuilder) addTableExpr(
	alias string,
	tableID semantics.TableSet,
	tblExpr sqlparser.SimpleTableExpr,
	hints sqlparser.IndexHints,
) {
	if qb.sel == nil {
		qb.sel = &sqlparser.Select{}
	}
	sel := qb.sel.(*sqlparser.Select)
	elems := &sqlparser.AliasedTableExpr{
		Expr:       tblExpr,
		Partitions: nil,
		As:         sqlparser.NewIdentifierCS(alias),
		Hints:      hints,
		Columns:    nil,
	}
	err := qb.ctx.SemTable.ReplaceTableSetFor(tableID, elems)
	if err != nil {
		log.Warningf("error in replacing table expression in semtable: %v", err)
	}
	sel.From = append(sel.From, elems)
	qb.sel = sel
}

func (qb *queryBuilder) rewriteColNames(expr sqlparser.Expr) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			id := qb.ctx.SemTable.DirectDeps(node)
			alias, ok := qb.aliasMap[id]
			if !ok {
				return false, NewBug("should be in the alias map")
			}
			node.Qualifier.Name = sqlparser.NewIdentifierCS(alias)
			node.Qualifier.Qualifier = sqlparser.NewIdentifierCS("")
		}
		return true, nil
	}, expr)
}

func (qb *queryBuilder) addProjection(projection sqlparser.Expr) error {
	err := qb.rewriteColNames(projection)
	if err != nil {
		return err
	}
	sel := qb.sel.(*sqlparser.Select)
	sel.SelectExprs = append(sel.SelectExprs, &sqlparser.AliasedExpr{Expr: projection})
	return nil
}

func (qb *queryBuilder) joinInnerWith(other *queryBuilder, onCondition sqlparser.Expr) {
	sel := qb.sel.(*sqlparser.Select)
	otherSel := other.sel.(*sqlparser.Select)
	sel.From = append(sel.From, otherSel.From...)
	sel.SelectExprs = append(sel.SelectExprs, otherSel.SelectExprs...)

	var predicate sqlparser.Expr
	if sel.Where != nil {
		predicate = sel.Where.Expr
	}
	if otherSel.Where != nil {
		predExprs := sqlparser.SplitAndExpression(nil, predicate)
		otherExprs := sqlparser.SplitAndExpression(nil, otherSel.Where.Expr)
		predicate = sqlparser.AndExpressions(append(predExprs, otherExprs...)...)
	}
	if predicate != nil {
		sel.Where = &sqlparser.Where{Type: sqlparser.WhereClause, Expr: predicate}
	}

	sqlparserx.AddSelectPredicate(qb.sel, onCondition)
}

func (qb *queryBuilder) joinOuterWith(other *queryBuilder, onCondition sqlparser.Expr) {
	sel := qb.sel.(*sqlparser.Select)
	otherSel := other.sel.(*sqlparser.Select)
	var lhs sqlparser.TableExpr
	if len(sel.From) == 1 {
		lhs = sel.From[0]
	} else {
		lhs = &sqlparser.ParenTableExpr{Exprs: sel.From}
	}
	var rhs sqlparser.TableExpr
	if len(otherSel.From) == 1 {
		rhs = otherSel.From[0]
	} else {
		rhs = &sqlparser.ParenTableExpr{Exprs: otherSel.From}
	}
	sel.From = []sqlparser.TableExpr{&sqlparser.JoinTableExpr{
		LeftExpr:  lhs,
		RightExpr: rhs,
		Join:      sqlparser.LeftJoinType,
		Condition: &sqlparser.JoinCondition{
			On: onCondition,
		},
	}}

	sel.SelectExprs = append(sel.SelectExprs, otherSel.SelectExprs...)
	var predicate sqlparser.Expr
	if sel.Where != nil {
		predicate = sel.Where.Expr
	}
	if otherSel.Where != nil {
		predicate = sqlparser.AndExpressions(predicate, otherSel.Where.Expr)
	}
	if predicate != nil {
		sel.Where = &sqlparser.Where{Type: sqlparser.WhereClause, Expr: predicate}
	}
}

func (qb *queryBuilder) clearOutput() error {
	sel, ok := qb.sel.(*sqlparser.Select)
	if !ok {
		// this probably means we have a UNION inside a derived table. not supported yet
		return ErrUpqueryNotSupported
	}
	sel.SelectExprs = nil
	return nil
}

func (qb *queryBuilder) replaceProjections(projections []Projection) error {
	oldSel, err := sqlparserx.SelectOutputExprs(qb.sel)
	if err != nil {
		return err
	}

	err = qb.clearOutput()
	if err != nil {
		return err
	}

	for _, proj := range projections {
		switch proj.Kind {
		case ProjectionColumn:
			expr := oldSel[proj.Column]
			err = qb.addProjection(expr)
		case ProjectionLiteral, ProjectionEval:
			err = qb.addProjection(proj.AST)
		default:
			err = NewBug(fmt.Sprintf("unexpected projection on generated query %T", proj))
		}
		if err != nil {
			return err
		}
	}
	return nil
}
