package operators

import (
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// bindOffsets finds the offsets between operators, and creates the evalengine expressions needed
// We don't do this when the operators are created because they will be rewritten and changed,
// and the offsets are not stable during the rewriting (for example when a filter can be moved under a join)
func bindOffsets(node *Node, semTable *semantics.SemTable) error {
	if len(node.Ancestors) == 0 {
		// if we are not fetching data from an incoming operator, no need to do column offset resolution
		return nil
	}

	// first we visit the children, so we can do this bottom up
	for _, ancestor := range node.Ancestors {
		err := bindOffsets(ancestor, semTable)
		if err != nil {
			return err
		}
	}

	return node.Op.PlanOffsets(node, semTable)
}

func (v *View) PlanOffsets(node *Node, st *semantics.SemTable) error {
	ancestor := node.Ancestors[0]
	for _, param := range v.Parameters {
		paramExpr, err := param.key.SingleAST()
		if err != nil {
			return err
		}
		offset, err := ancestor.ExprLookup(st, paramExpr)
		if err != nil {
			return err
		}
		v.ParametersIdx = append(v.ParametersIdx, offset)
	}
	return nil
}

func (g *GroupBy) PlanOffsets(node *Node, semTable *semantics.SemTable) error {
	input := node.Ancestors[0]

	for _, grouping := range g.Grouping {
		ast, err := grouping.SingleAST()
		if err != nil {
			return err
		}
		offset, err := input.ExprLookup(semTable, ast)
		if err != nil {
			return err
		}
		g.GroupingIdx = append(g.GroupingIdx, offset)
	}

	for _, aggr := range g.Aggregations {
		var offset int
		var err error
		_, isCountStar := aggr.(*sqlparser.CountStar)
		if !isCountStar {
			offset, err = input.ExprLookup(semTable, aggr.GetArg())
			if err != nil {
				return err
			}
		}
		g.AggregationsIdx = append(g.AggregationsIdx, offset)
		g.AggregationsTypes = append(g.AggregationsTypes, aggrFuncToGroupKind(aggr))
	}
	return nil
}

func aggrFuncToGroupKind(aggr sqlparser.AggrFunc) flownode.AggregationKind {
	switch aggr.(type) {
	case *sqlparser.Sum:
		return flownode.AggregationSum
	case *sqlparser.Count:
		return flownode.AggregationCount
	case *sqlparser.CountStar:
		return flownode.AggregationCountStar
	case *sqlparser.Min:
		return flownode.ExtremumMin
	case *sqlparser.Max:
		return flownode.ExtremumMax
	}
	panic("should not happen")
}

func (j *Join) PlanOffsets(node *Node, semTable *semantics.SemTable) error {
	lhsNode := node.Ancestors[0]
	rhsNode := node.Ancestors[1]
	lhsID := lhsNode.Covers()
	rhsID := rhsNode.Covers()

	for _, col := range j.EmitColumns {
		if len(col.AST) == 2 {
			err := handleJoinColumn(col, semTable, lhsID, rhsID, j, lhsNode, rhsNode)
			if err != nil {
				return err
			}
			continue
		}

		expr, err := col.SingleAST()
		if err != nil {
			return err
		}
		deps := semTable.DirectDeps(expr)
		switch {
		case deps.IsSolvedBy(lhsID):
			offset, err := lhsNode.ExprLookup(semTable, expr)
			if err != nil {
				return err
			}
			j.Emit = append(j.Emit, flownode.JoinSourceLeft(offset))
		case deps.IsSolvedBy(rhsID):
			offset, err := rhsNode.ExprLookup(semTable, expr)
			if err != nil {
				return err
			}
			j.Emit = append(j.Emit, flownode.JoinSourceRight(offset))
		default:
			panic("could not find where this should go!")
		}
	}

	return nil
}

func handleJoinColumn(
	col *Column,
	semTable *semantics.SemTable,
	lhsID, rhsID semantics.TableSet,
	op *Join,
	lhsNode, rhsNode *Node,
) error {
	lftCol, lftOK := col.AST[0].(*sqlparser.ColName)
	rgtCol, rgtOK := col.AST[1].(*sqlparser.ColName)
	if !lftOK || !rgtOK {
		return NewBug("wrong data in join predicate, should be an equal op comparison")
	}
	switch {
	// lhs = rhs
	case semTable.DirectDeps(lftCol).IsSolvedBy(lhsID) &&
		semTable.DirectDeps(rgtCol).IsSolvedBy(rhsID):
		err := setColNameOffsets(semTable, op, lhsNode, rhsNode, lftCol, rgtCol)
		if err != nil {
			return err
		}
	case semTable.DirectDeps(rgtCol).IsSolvedBy(lhsID) &&
		semTable.DirectDeps(lftCol).IsSolvedBy(rhsID):
		err := setColNameOffsets(semTable, op, lhsNode, rhsNode, rgtCol, lftCol)
		if err != nil {
			return err
		}
	default:
		return NewBug("wrong data in join predicate, should be an equal op comparison")
	}
	return nil
}

func setColNameOffsets(semTable *semantics.SemTable, op *Join, lhsNode, rhsNode *Node, lftCol, rgtCol *sqlparser.ColName) error {
	lft, err := lhsNode.ExprLookup(semTable, lftCol)
	if err != nil {
		return err
	}

	rgt, err := rhsNode.ExprLookup(semTable, rgtCol)
	if err != nil {
		return err
	}
	op.Emit = append(op.Emit, flownode.JoinSourceBoth(lft, rgt))
	return nil
}

// getComparisons splits the incoming expression into a list of ANDed comparisons.
// If the predicate is not a list of ANDed predicated, this function will fail
func getComparisons(expr sqlparser.Expr) (predicates []*sqlparser.ComparisonExpr, err error) {
	if expr == nil {
		return nil, nil
	}
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		switch comp := e.(type) {
		case *sqlparser.ComparisonExpr:
			if comp.Operator != sqlparser.EqualOp {
				return nil, NewBug("wrong data in join predicate, should be an equal op comparison")
			}
			predicates = append(predicates, comp)
		default:
			return nil, NewBug("wrong data in join predicate, should be an equal op comparison")
		}
	}
	return
}

func (p *Project) PlanOffsets(node *Node, semTable *semantics.SemTable) error {
	input := node.Ancestors[0]
	lu := &lookup{
		node:     input,
		semTable: semTable,
	}

	for _, col := range p.Columns {
		ast, err := col.SingleAST()
		if err != nil {
			return err
		}
		switch expr := ast.(type) {
		case *sqlparser.ColName, sqlparser.AggrFunc:
			offset, err := input.ExprLookup(semTable, expr)
			if err != nil {
				return err
			}
			p.Projections = append(p.Projections, flownode.ProjectedCol(offset))
		case *sqlparser.Literal:
			v, err := flownode.ProjectedLiteralFromAST(expr)
			if err != nil {
				return err
			}
			p.Projections = append(p.Projections, v)
		default:
			newExpr, err := rewriteColNamesToOffsets(semTable, node.Ancestors[0], expr)
			if err != nil {
				return err
			}
			eexpr, err := evalengine.Translate(newExpr, lu)
			if err != nil {
				return &UnsupportedError{AST: expr, Type: EvalEngineNotSupported}
			}
			p.Projections = append(p.Projections, &flownode.ProjectedExpr{
				AST:      newExpr,
				EvalExpr: eexpr,
			})
		}
	}
	return nil
}

func rewriteColNamesToOffsets(semTable *semantics.SemTable, node *Node, expr sqlparser.Expr) (sqlparser.Expr, error) {
	var breakingError error
	result := sqlparser.Rewrite(sqlparser.CloneExpr(expr), func(cursor *sqlparser.Cursor) bool {
		switch expr := cursor.Node().(type) {
		case *sqlparser.ColName:
			offset, err := node.ExprLookup(semTable, expr)
			if err != nil {
				breakingError = err
				return false
			}
			cursor.Replace(&sqlparser.Offset{V: offset})
		}
		return breakingError == nil
	}, nil)
	return result.(sqlparser.Expr), breakingError
}

func (f *Filter) PlanOffsets(node *Node, semTable *semantics.SemTable) error {
	input := node.Ancestors[0]
	var err error
	newPredicate := sqlparser.Rewrite(sqlparser.CloneExpr(f.Predicates), func(cursor *sqlparser.Cursor) bool {
		switch col := cursor.Node().(type) {
		case *sqlparser.ColName, sqlparser.AggrFunc:
			expr := col.(sqlparser.Expr) // this is always valid, but golang doesn't realise it for us
			offset, err := input.ExprLookup(semTable, expr)
			if err != nil {
				return false
			}
			cursor.Replace(&sqlparser.Offset{
				V:        offset,
				Original: sqlparser.String(col),
			})
		}
		return true
	}, nil)
	if err != nil {
		return err
	}
	for _, expr := range sqlparser.SplitAndExpression(nil, newPredicate.(sqlparser.Expr)) {
		evalEngineExpr, err := evalengine.Translate(expr, nil)
		if err != nil {
			return err
		}
		f.EvalExpr = append(f.EvalExpr, evalEngineExpr)
		s := sqlparser.String(expr)
		f.ExprStr = append(f.ExprStr, s)
	}
	return nil
}

func (u *Union) PlanOffsets(node *Node, st *semantics.SemTable) error {
	var err error
	u.ColumnsIdx[0], err = bindUnionSideOffset(u.InputColumns[0], node.Ancestors[0], st)
	if err != nil {
		return err
	}
	u.ColumnsIdx[1], err = bindUnionSideOffset(u.InputColumns[1], node.Ancestors[1], st)
	if err != nil {
		return err
	}
	return nil
}

func (n *NodeTableRef) PlanOffsets(*Node, *semantics.SemTable) error {
	// do nothing
	return nil
}

func (t *Table) PlanOffsets(*Node, *semantics.SemTable) error {
	// do nothing
	return nil
}

func (t *TopK) PlanOffsets(node *Node, table *semantics.SemTable) error {
	ancestor := node.Ancestors[0]
	for _, param := range t.Order {
		offset, err := ancestor.ExprLookup(table, param.Expr)
		if err != nil {
			return err
		}
		t.OrderOffsets = append(t.OrderOffsets, offset)
	}

	for _, p := range t.Parameters {
		col, err := p.SingleAST()
		if err != nil {
			return err
		}
		offset, err := ancestor.ExprLookup(table, col)
		if err != nil {
			return err
		}
		t.ParamOffsets = append(t.ParamOffsets, offset)
	}
	return nil
}

func (d *Distinct) PlanOffsets(*Node, *semantics.SemTable) error {
	// nothing to do
	return nil
}

func bindUnionSideOffset(columns Columns, node *Node, st *semantics.SemTable) ([]int, error) {
	var offsets []int
	for _, column := range columns {
		ast, err := column.SingleAST()
		if err != nil {
			return nil, err
		}
		idx, err := node.ExprLookup(st, ast)
		if err != nil {
			return nil, err
		}
		offsets = append(offsets, idx)
	}
	return offsets, nil
}

type lookup struct {
	node     *Node
	semTable *semantics.SemTable
}

func (lu *lookup) ColumnLookup(col *sqlparser.ColName) (int, error) {
	for i, column := range lu.node.Columns {
		if column.EqualsAST(lu.semTable, col, true) {
			return i, nil
		}
	}
	return -1, NewBug("column not found during lookup")
}

func (lu *lookup) CollationForExpr(expr sqlparser.Expr) collations.ID {
	return lu.semTable.CollationForExpr(expr)
}

func (lu *lookup) DefaultCollation() collations.ID {
	return collations.Default()
}
