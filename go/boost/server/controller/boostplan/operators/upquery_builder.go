package operators

import (
	"fmt"
	"sort"

	"vitess.io/vitess/go/boost/common/dbg"
	"vitess.io/vitess/go/boost/server/controller/boostplan/sqlparserx"

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
	qb.ctx.SemTable.ReplaceTableSetFor(tableID, elems)
	sel.From = append(sel.From, elems)
	qb.sel = sel
}

func (qb *queryBuilder) rewriteColNames(expr sqlparser.Expr) (sqlparser.Expr, error) {
	newExpr := sqlparser.Rewrite(sqlparser.CloneExpr(expr), func(cursor *sqlparser.Cursor) bool {
		col, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return true
		}

		id := qb.ctx.SemTable.DirectDeps(col)
		tblAlias, ok := qb.aliasMap[id]
		if !ok {
			return true
		}
		dbg.Assert(ok, "id %v should be in the alias map", id)

		newCol := &sqlparser.ColName{
			Name: col.Name,
			Qualifier: sqlparser.TableName{
				Name:      sqlparser.NewIdentifierCS(tblAlias),
				Qualifier: sqlparser.NewIdentifierCS(""),
			},
		}

		cursor.Replace(newCol)
		qb.ctx.SemTable.Direct[newCol] = id
		qb.ctx.SemTable.Recursive[newCol] = id

		return true
	}, nil).(sqlparser.Expr)
	return newExpr, nil
}

func (qb *queryBuilder) addProjection(projection sqlparser.Expr) error {
	projection, err := qb.rewriteColNames(projection)
	if err != nil {
		return err
	}
	sel := sqlparser.GetFirstSelect(qb.sel)
	ae := &sqlparser.AliasedExpr{Expr: projection}
	if lit, ok := projection.(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
		ae.As = sqlparser.NewIdentifierCI(getColNameForLiteral(lit))
	}

	sel.SelectExprs = append(sel.SelectExprs, ae)
	return nil
}

func (qb *queryBuilder) addGrouping(grouping sqlparser.Expr) error {
	if lit, ok := grouping.(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
		// GROUP BY 1 is referring to the first column returned and not the literal value 1
		grouping = sqlparser.NewColName(getColNameForLiteral(lit))
	} else {
		var err error
		grouping, err = qb.rewriteColNames(grouping)
		if err != nil {
			return err
		}
	}

	sel := qb.sel.(*sqlparser.Select)
	sel.GroupBy = append(sel.GroupBy, grouping)
	return nil
}

func getColNameForLiteral(lit *sqlparser.Literal) string {
	return "literal-" + sqlparser.String(lit)
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

func (qb *queryBuilder) unionWith(other *queryBuilder) {
	qb.sel = &sqlparser.Union{
		Left:  qb.sel,
		Right: other.sel,
	}
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

func (qb *queryBuilder) turnIntoDerived(name string, id semantics.TableSet) error {
	// here we'll take the query that has been accumulated so far and
	// use that as SELECT we stick into the derived table.
	// We also need to return all the columns on the outside
	//
	// [SELECT tbl.a, tbl.b, 12 FROM tbl] would get turned into:
	// [SELECT dt_0.a, dt_0.b, dt_0.`12` FROM (SELECT tbl.a, tbl.b, 12 FROM tbl) as dt_0]
	alias := qb.getTableAlias(name)
	qb.aliasMap[id] = alias

	dt := &sqlparser.AliasedTableExpr{
		Expr: &sqlparser.DerivedTable{Select: qb.sel},
		As:   sqlparser.NewIdentifierCS(alias),
	}

	var selectExprs sqlparser.SelectExprs
	for _, expr := range sqlparser.GetFirstSelect(qb.sel).SelectExprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			// it could be a `StarExpression`, and that means that we have a query that has not been expanded
			// not expanded is already problematic for other reasons
			return ErrUpqueryNotSupported
		}
		colName := &sqlparser.ColName{
			Name:      sqlparser.NewIdentifierCI(ae.ColumnName()),
			Qualifier: sqlparser.TableName{Name: sqlparser.NewIdentifierCS(alias)},
		}
		qb.ctx.SemTable.Direct[colName] = id
		qb.ctx.SemTable.Recursive[colName] = id
		selectExprs = append(selectExprs, &sqlparser.AliasedExpr{Expr: colName})
	}
	qb.sel = &sqlparser.Select{
		SelectExprs: selectExprs,
		From:        []sqlparser.TableExpr{dt},
	}
	return nil
}

func (qb *queryBuilder) clearOutput() error {
	sel := sqlparser.GetFirstSelect(qb.sel)
	if sel == nil {
		// this probably means we have a UNION inside a derived table. not supported yet
		return ErrUpqueryNotSupported
	}
	sel.SelectExprs = nil
	return nil
}

func (qb *queryBuilder) getTableAlias(name string) string {
	i := qb.tableNames[name]
	qb.tableNames[name]++
	return fmt.Sprintf("%s_%d", name, i)
}

func (qb *queryBuilder) replaceProjections(projections []Projection) error {
	var selects []*sqlparser.Select
	var oldExpressions [][]sqlparser.Expr
	queue := []sqlparser.SelectStatement{qb.sel}
	for len(queue) > 0 {
		this := queue[0]
		queue = queue[1:]
		switch stmt := this.(type) {
		case *sqlparser.Select:
			oldSel, err := sqlparserx.SelectOutputExprs(stmt)
			if err != nil {
				return err
			}
			oldExpressions = append(oldExpressions, oldSel)
			selects = append(selects, stmt)
			stmt.SelectExprs = nil
		case *sqlparser.Union:
			queue = append(queue, stmt.Left, stmt.Right)
		}
	}

	addColumn := func(col int) error {
		for idx, sel := range selects {
			oldExpr := oldExpressions[idx][col]
			expr, err := qb.rewriteColNames(oldExpr)
			if err != nil {
				return err
			}
			sel.SelectExprs = append(sel.SelectExprs, &sqlparser.AliasedExpr{Expr: expr})
		}
		return nil
	}
	addExpression := func(in sqlparser.Expr) error {
		expr, err := qb.rewriteColNames(in)
		if err != nil {
			return err
		}

		for _, sel := range selects {
			sel.SelectExprs = append(sel.SelectExprs, &sqlparser.AliasedExpr{Expr: expr})
		}
		return nil
	}

	for _, proj := range projections {
		switch proj.Kind {
		case ProjectionColumn:
			if err := addColumn(proj.Column); err != nil {
				return err
			}
		case ProjectionLiteral, ProjectionEval:
			if err := addExpression(proj.Original); err != nil {
				return err
			}
		default:
			dbg.Bug("unexpected projection on generated query %T", proj)
		}
	}
	return nil
}
