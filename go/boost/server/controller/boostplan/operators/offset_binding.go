package operators

import (
	"golang.org/x/exp/slices"

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
		ast, err := aggr.SingleAST()
		if err != nil {
			return err
		}
		var offset int
		switch aggr := ast.(type) {
		case *sqlparser.CountStar:
		case sqlparser.AggrFunc:
			offset, err = input.ExprLookup(semTable, aggr.GetArg())
			if err != nil {
				return err
			}
		default:
			offset, err = input.ExprLookup(semTable, aggr)
			if err != nil {
				return err
			}
		}

		g.AggregationsIdx = append(g.AggregationsIdx, offset)
	}
	return nil
}

func (j *Join) PlanOffsets(node *Node, semTable *semantics.SemTable) error {
	lhsNode := node.Ancestors[0]
	rhsNode := node.Ancestors[1]
	lhsID := lhsNode.Covers()
	rhsID := rhsNode.Covers()

	for _, col := range j.EmitColumns {
		isJoinCondition := slices.Index(j.JoinColumns, col) >= 0

		if isJoinCondition && j.Inner {
			err := j.handleJoinColumn(col, semTable, lhsID, rhsID, lhsNode, rhsNode)
			if err != nil {
				return err
			}
			continue
		}

		for _, expr := range col.AST {
			deps := semTable.DirectDeps(expr)
			switch {
			case deps.IsSolvedBy(lhsID):
				offset, err := lhsNode.ExprLookup(semTable, expr)
				if err != nil {
					return err
				}
				j.Emit = append(j.Emit, [2]int{offset, -1})
				if isJoinCondition {
					j.On[0] = offset
				}
			case deps.IsSolvedBy(rhsID):
				offset, err := rhsNode.ExprLookup(semTable, expr)
				if err != nil {
					return err
				}
				j.Emit = append(j.Emit, [2]int{-1, offset})
				if isJoinCondition {
					j.On[1] = offset
				}
			default:
				panic("could not find where this should go!")
			}
		}
	}

	return nil
}

func (j *Join) handleJoinColumn(
	col *Column,
	semTable *semantics.SemTable,
	lhsID, rhsID semantics.TableSet,
	lhsNode, rhsNode *Node,
) error {
	lftCol, lftOK := col.AST[0].(*sqlparser.ColName)
	rgtCol, rgtOK := col.AST[1].(*sqlparser.ColName)
	if !lftOK {
		return &UnsupportedError{
			AST:  col.AST[0],
			Type: JoinPredicates,
		}
	}
	if !rgtOK {
		return &UnsupportedError{
			AST:  col.AST[1],
			Type: JoinPredicates,
		}
	}
	switch {
	// lhs = rhs
	case semTable.DirectDeps(lftCol).IsSolvedBy(lhsID) &&
		semTable.DirectDeps(rgtCol).IsSolvedBy(rhsID):
		err := j.setColNameOffsets(semTable, lhsNode, rhsNode, lftCol, rgtCol)
		if err != nil {
			return err
		}
	case semTable.DirectDeps(rgtCol).IsSolvedBy(lhsID) &&
		semTable.DirectDeps(lftCol).IsSolvedBy(rhsID):
		err := j.setColNameOffsets(semTable, lhsNode, rhsNode, rgtCol, lftCol)
		if err != nil {
			return err
		}
	default:
		return &UnsupportedError{
			AST: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualOp,
				Left:     lftCol,
				Right:    rgtCol,
			},
			Type: JoinPredicates,
		}
	}
	return nil
}

func (j *Join) setColNameOffsets(semTable *semantics.SemTable, lhsNode, rhsNode *Node, lftCol, rgtCol *sqlparser.ColName) error {
	lft, err := lhsNode.ExprLookup(semTable, lftCol)
	if err != nil {
		return err
	}

	rgt, err := rhsNode.ExprLookup(semTable, rgtCol)
	if err != nil {
		return err
	}

	source := [2]int{lft, rgt}
	j.Emit = append(j.Emit, source)
	j.On = source
	return nil
}

// getComparisons splits the incoming expression into a list of ANDed comparisons.
// If the predicate is not a list of ANDed predicated, this function will fail
func getComparisons(expr sqlparser.Expr) (predicates []*sqlparser.ComparisonExpr, err error) {
	if expr == nil {
		return nil, nil
	}
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		comp, ok := e.(*sqlparser.ComparisonExpr)
		if ok {
			predicates = append(predicates, comp)
			continue
		}
		return nil, &UnsupportedError{
			AST:  e,
			Type: JoinPredicates,
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
			p.Projections = append(p.Projections, Projection{Kind: ProjectionColumn, Column: offset})
		case *sqlparser.Literal:
			p.Projections = append(p.Projections, Projection{Kind: ProjectionLiteral, AST: expr})
		default:
			newExpr, err := rewriteColNamesToOffsets(semTable, node.Ancestors[0], expr)
			if err != nil {
				return err
			}
			eexpr, err := evalengine.Translate(newExpr, lu)
			if err != nil {
				return &UnsupportedError{AST: expr, Type: EvalEngineNotSupported}
			}
			p.Projections = append(p.Projections, Projection{Kind: ProjectionEval, AST: newExpr, Eval: eexpr})
		}
	}
	return nil
}

func (n *NullFilter) PlanOffsets(node *Node, st *semantics.SemTable) error {
	input := node.Ancestors[0]

	newPredicate := sqlparser.Rewrite(sqlparser.CloneExpr(n.Predicates), findOffsets(st, input), nil).(sqlparser.Expr)

	for _, col := range node.Columns {
		ast, err := col.SingleAST()
		if err != nil {
			return err
		}

		col, ok := ast.(*sqlparser.ColName)
		deps := st.DirectDeps(col)

		if ok && !deps.Equals(n.OuterSide) {
			// if we have a bare column and it comes from the inner side of the join, we can just pass it through
			offset, err := input.ExprLookup(st, col)
			if err != nil {
				return err
			}
			n.Projections = append(n.Projections, Projection{Kind: ProjectionColumn, Column: offset})
			continue
		}

		// for everything else, we need to go over the expression and null out values coming from the outer side
		ast = sqlparser.CloneExpr(ast)
		ast = sqlparser.Rewrite(ast, func(cursor *sqlparser.Cursor) bool {
			col, ok := cursor.Node().(*sqlparser.ColName)
			if !ok {
				return err != nil
			}

			offset, thisErr := input.ExprLookup(st, col)
			if thisErr != nil {
				err = thisErr
				return false
			}

			offsetExpr := sqlparser.NewOffset(offset, col)
			if !st.DirectDeps(col).Equals(n.OuterSide) {
				// columns from the inner side can just pass through
				cursor.Replace(offsetExpr)
				return false
			}

			caseExpr := &sqlparser.CaseExpr{
				Whens: []*sqlparser.When{{
					Cond: newPredicate,
					Val:  offsetExpr,
				}},
				Else: &sqlparser.NullVal{},
			}

			cursor.Replace(caseExpr)
			return false
		}, nil).(sqlparser.Expr)
		if err != nil {
			return err
		}

		eeExpr, err := evalengine.Translate(ast, nil)
		if err != nil {
			return err
		}
		n.Projections = append(n.Projections, Projection{Kind: ProjectionEval, AST: ast, Eval: eeExpr})
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

func findOffsets(semTable *semantics.SemTable, input *Node) func(cursor *sqlparser.Cursor) bool {
	return func(cursor *sqlparser.Cursor) bool {
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
	}
}

func (f *Filter) PlanOffsets(node *Node, semTable *semantics.SemTable) error {
	input := node.Ancestors[0]
	var err error
	newPredicate := sqlparser.Rewrite(sqlparser.CloneExpr(f.Predicates), findOffsets(semTable, input), nil)
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
