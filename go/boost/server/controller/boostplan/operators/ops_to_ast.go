package operators

import (
	"errors"
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/sqlparser"
)

func ToSQL(ctx *PlanContext, node *Node, keys []int) (sqlparser.SelectStatement, error) {
	q := newQueryBuilder(ctx)
	err := buildQuery(node, q)
	if err != nil {
		return nil, err
	}
	q.sortTables()
	err = q.addKeys(keys)
	if err != nil {
		return nil, err
	}

	return q.sel, nil
}

var ErrUpqueryNotSupported = errors.New("can't build upquery")

func buildQuery(node *Node, qb *queryBuilder) error {
	switch len(node.Ancestors) {
	case 0:
		return node.Op.AddToQueryBuilder([]*queryBuilder{qb}, node)
	case 1:
		err := buildQuery(node.Ancestors[0], qb)
		if err != nil {
			return err
		}
		return node.Op.AddToQueryBuilder([]*queryBuilder{qb}, node)
	case 2:
		switch op := node.Op.(type) {
		case *Union:
			return ErrUpqueryNotSupported
		case *Join:
			err := buildQuery(node.Ancestors[0], qb)
			if err != nil {
				return err
			}
			qbR := newQueryBuilder(qb.ctx)
			qbR.tableNames = qb.tableNames
			qbR.aliasMap = qb.aliasMap
			err = buildQuery(node.Ancestors[1], qbR)
			if err != nil {
				return err
			}
			return op.AddToQueryBuilder([]*queryBuilder{qb, qbR}, node)
		}
	}

	return nil
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

	return ts.tbl.TableSetFor(left).TableOffset() < ts.tbl.TableSetFor(right).TableOffset()
}

// Swap implements the Sort interface
func (ts *tableSorter) Swap(i, j int) {
	ts.sel.From[i], ts.sel.From[j] = ts.sel.From[j], ts.sel.From[i]
}

func (t *Table) AddToQueryBuilder([]*queryBuilder, *Node) error {
	// empty by design
	return nil
}

func (j *Join) AddToQueryBuilder(builders []*queryBuilder, _ *Node) error {
	qb := builders[0]
	err := qb.rewriteColNames(j.Predicates)
	if err != nil {
		return err
	}
	qbR := builders[1]
	if j.Inner {
		qb.joinInnerWith(qbR, j.Predicates)
	} else {
		qb.joinOuterWith(qbR, j.Predicates)
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

func (f *Filter) AddToQueryBuilder(builders []*queryBuilder, _ *Node) error {
	qb := builders[0]
	err := qb.rewriteColNames(f.Predicates)
	if err != nil {
		return err
	}
	qb.addPredicate(f.Predicates)
	return nil
}

func (n *NullFilter) AddToQueryBuilder(builders []*queryBuilder, this *Node) error {
	qb := builders[0]
	sel, ok := qb.sel.(*sqlparser.Select)
	if !ok {
		return NewBug("null filter should always come after a join")
	}
	jt, ok := sel.From[len(sel.From)-1].(*sqlparser.JoinTableExpr)
	if !ok {
		return NewBug("expected to find a join as the last table in the from clause")
	}

	jt.Condition.On = sqlparser.AndExpressions(jt.Condition.On, n.Predicates)

	err := qb.clearOutput()
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

func (g *GroupBy) AddToQueryBuilder(builders []*queryBuilder, n *Node) error {
	qb := builders[0]
	if err := qb.clearOutput(); err != nil {
		return err
	}
	for _, col := range n.Columns {
		err := qb.addProjection(col.AST[0])
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Project) AddToQueryBuilder(builders []*queryBuilder, _ *Node) error {
	if p.isDerivedTable() {
		return ErrUpqueryNotSupported
	}
	qb := builders[0]
	return qb.replaceProjections(p.Projections)
}

func (v *View) AddToQueryBuilder([]*queryBuilder, *Node) error {
	// empty by design
	return nil
}

func (n *NodeTableRef) AddToQueryBuilder(builders []*queryBuilder, _ *Node) error {
	tbl, isTbl := n.Node.Op.(*Table)
	if !isTbl {
		return NewBug("should be a table")
	}

	aliasPrefix := tbl.Keyspace + "_" + tbl.TableName
	qb := builders[0]
	i := qb.tableNames[aliasPrefix]
	qb.tableNames[aliasPrefix]++
	alias := fmt.Sprintf("%s_%d", aliasPrefix, i)
	qb.aliasMap[n.TableID] = alias

	qb.addTable(tbl.Keyspace, tbl.TableName, alias, n.TableID, n.Hints)
	for _, name := range n.Columns {
		ast, err := name.SingleAST()
		if err != nil {
			return err
		}
		col, isCol := ast.(*sqlparser.ColName)
		if !isCol {
			return NewBug("should be a column")
		}
		col.Qualifier.Name = sqlparser.NewIdentifierCS(alias)
		col.Qualifier.Qualifier = sqlparser.NewIdentifierCS("")

		if err != nil {
			return err
		}
		err = qb.addProjection(ast)
		if err != nil {
			return err
		}
	}
	return nil
}

func (u *Union) AddToQueryBuilder([]*queryBuilder, *Node) error {
	return ErrUpqueryNotSupported
}

func (t *TopK) AddToQueryBuilder(builders []*queryBuilder, _ *Node) error {
	qb := builders[0]
	qb.sel.SetOrderBy(t.Order)
	limit := &sqlparser.Limit{
		Offset:   nil,
		Rowcount: sqlparser.NewIntLiteral(strconv.FormatInt(int64(t.K), 10)),
	}
	qb.sel.SetLimit(limit)
	return nil
}

func (d *Distinct) AddToQueryBuilder([]*queryBuilder, *Node) error {
	return ErrUpqueryNotSupported
}
