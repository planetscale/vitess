package operators

import (
	"errors"
	"strconv"

	"vitess.io/vitess/go/boost/common/dbg"
	"vitess.io/vitess/go/boost/server/controller/boostplan/sqlparserx"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var ErrUpqueryNotSupported = errors.New("can't build upquery")

func (n *Node) generateUpqueries(ctx *PlanContext) error {
	for _, ancestor := range n.Ancestors {
		if err := ancestor.generateUpqueries(ctx); err != nil {
			return err
		}
	}

	q := newQueryBuilder(ctx)
	if err := n.addToQueryBuilder(q); err != nil {
		if errors.Is(err, ErrUpqueryNotSupported) {
			return nil
		}
		return err
	}
	q.sortTables()
	n.Upquery = q.sel

	if tref, ok := n.Op.(*NodeTableRef); ok {
		tref.Node.Upquery = q.sel
	}

	return nil
}

func (n *Node) addToQueryBuilder(qb *queryBuilder) error {
	type UpqueryOperator interface {
		Operator
		addToQueryBuilder(qb *queryBuilder, this *Node) error
	}

	type UpqueryOperator2 interface {
		Operator
		addToQueryBuilder2(qbLeft, qbRight *queryBuilder, this *Node) error
	}

	switch op := n.Op.(type) {
	case UpqueryOperator:
		switch len(n.Ancestors) {
		case 0:
		case 1:
			err := n.Ancestors[0].addToQueryBuilder(qb)
			if err != nil {
				return err
			}
		default:
			dbg.Bug("UpqueryOperator with >1 ancestors")
		}
		return op.addToQueryBuilder(qb, n)

	case UpqueryOperator2:
		dbg.Assert(len(n.Ancestors) == 2, "UpqueryOperator2 without 2 ancestors")

		err := n.Ancestors[0].addToQueryBuilder(qb)
		if err != nil {
			return err
		}
		qbR := newQueryBuilder(qb.ctx)
		qbR.tableNames = qb.tableNames
		qbR.aliasMap = qb.aliasMap

		err = n.Ancestors[1].addToQueryBuilder(qbR)
		if err != nil {
			return err
		}
		return op.addToQueryBuilder2(qb, qbR, n)

	default:
		dbg.Bug("missing UpqueryOperator")
		return nil
	}
}

type tableSorter struct {
	sel *sqlparser.Select
	tbl *semantics.SemTable
}

// Len implements the Sort interface
func (ts *tableSorter) Len() int {
	return len(ts.sel.From)
}

// Less implements the Sort interface
func (ts *tableSorter) Less(i, j int) bool {
	lhs := ts.sel.From[i]
	rhs := ts.sel.From[j]
	left, ok := lhs.(*sqlparser.AliasedTableExpr)
	if !ok {
		return i < j
	}
	right, ok := rhs.(*sqlparser.AliasedTableExpr)
	if !ok {
		return i < j
	}

	tbl := ts.tbl
	lft := tbl.TableSetFor(left)
	rgt := tbl.TableSetFor(right)
	return lft.TableOffset() < rgt.TableOffset()
}

// Swap implements the Sort interface
func (ts *tableSorter) Swap(i, j int) {
	ts.sel.From[i], ts.sel.From[j] = ts.sel.From[j], ts.sel.From[i]
}

func (t *Table) addToQueryBuilder(*queryBuilder, *Node) error {
	// empty by design
	return nil
}

func (j *Join) addToQueryBuilder2(qb, qbR *queryBuilder, _ *Node) error {
	pred, err := qb.rewriteColNames(j.Predicates)
	if err != nil {
		return err
	}
	if j.Inner {
		qb.joinInnerWith(qbR, pred)
	} else {
		qb.joinOuterWith(qbR, pred)
	}
	err = qb.clearOutput()
	if err != nil {
		return err
	}
	for _, col := range j.EmitColumns {
		err := qb.addProjection(col.AST[0])
		if err != nil {
			return err
		}
	}
	return nil
}

func (u *Union) addToQueryBuilder2(qb1, qb2 *queryBuilder, _ *Node) error {
	qb1.unionWith(qb2)
	return nil
}

func (f *Filter) addToQueryBuilder(qb *queryBuilder, _ *Node) error {
	pred, err := qb.rewriteColNames(f.Predicates)
	if err != nil {
		return err
	}
	sqlparserx.AddSelectPredicate(qb.sel, pred)
	return nil
}

func (n *NullFilter) addToQueryBuilder(qb *queryBuilder, this *Node) error {
	sel, ok := qb.sel.(*sqlparser.Select)
	dbg.Assert(ok, "null filter should always come after a join")

	jt, ok := sel.From[len(sel.From)-1].(*sqlparser.JoinTableExpr)
	dbg.Assert(ok, "expected to find a join as the last table in the from clause")

	pred, err := qb.rewriteColNames(n.Predicates)
	if err != nil {
		return err
	}

	jt.Condition.On = sqlparser.AndExpressions(jt.Condition.On, pred)

	err = qb.clearOutput()
	if err != nil {
		return err
	}

	for _, col := range this.Columns {
		err := qb.addProjection(col.AST[0])
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *GroupBy) addToQueryBuilder(qb *queryBuilder, n *Node) error {
	if err := qb.clearOutput(); err != nil {
		return err
	}
	for _, col := range n.Columns {
		err := qb.addProjection(col.AST[0])
		if err != nil {
			return err
		}
	}
	for _, col := range g.Grouping {
		err := qb.addGrouping(col.AST[0])
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Project) addToQueryBuilder(qb *queryBuilder, _ *Node) error {
	err := qb.replaceProjections(p)
	if err != nil {
		return err
	}
	if p.isDerivedTable() {
		return qb.turnIntoDerived(p.Alias, *p.TableID)
	}
	return nil
}

func (v *View) addToQueryBuilder(*queryBuilder, *Node) error {
	// empty by design
	return nil
}

func (n *NodeTableRef) addToQueryBuilder(qb *queryBuilder, _ *Node) error {
	tbl, isTbl := n.Node.Op.(*Table)
	dbg.Assert(isTbl, "should be a table")

	alias := qb.getTableAlias(tbl.Keyspace + "_" + tbl.TableName)
	qb.aliasMap[n.TableID] = alias

	qb.addTable(tbl.Keyspace, tbl.TableName, alias, n.TableID, n.Hints)
	for _, name := range n.Columns {
		ast, err := name.SingleAST()
		if err != nil {
			return err
		}
		col, isCol := ast.(*sqlparser.ColName)
		dbg.Assert(isCol, "should be a column")

		col.Qualifier.Name = sqlparser.NewIdentifierCS(alias)
		col.Qualifier.Qualifier = sqlparser.NewIdentifierCS("")

		err = qb.addProjection(ast)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TopK) addToQueryBuilder(qb *queryBuilder, _ *Node) (err error) {
	for _, order := range t.Order {
		order.Expr, err = qb.rewriteColNames(order.Expr)
		if err != nil {
			return err
		}
	}
	qb.sel.SetOrderBy(t.Order)
	if t.K < 0 {
		return nil
	}
	limit := &sqlparser.Limit{
		Rowcount: sqlparser.NewIntLiteral(strconv.FormatInt(int64(t.K), 10)),
	}
	qb.sel.SetLimit(limit)
	return
}

func (d *Distinct) addToQueryBuilder(qb *queryBuilder, _ *Node) error {
	switch stmt := qb.sel.(type) {
	case *sqlparser.Select:
		stmt.Distinct = true
	case *sqlparser.Union:
		stmt.Distinct = true
	default:
		dbg.Bug("expected select or union")
	}
	return nil
}
