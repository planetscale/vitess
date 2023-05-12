package operators

import (
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/common/dbg"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// bindOffsets finds the offsets between operators, and creates the evalengine expressions needed
// We don't do this when the operators are created because they will be rewritten and changed,
// and the offsets are not stable during the rewriting (for example when a filter can be moved under a join)
func (conv *Converter) bindOffsets(node *Node, semTable *semantics.SemTable) error {
	if len(node.Ancestors) == 0 {
		// if we are not fetching data from an incoming operator, no need to do column offset resolution
		return nil
	}

	// first we visit the children, so we can do this bottom up
	for _, ancestor := range node.Ancestors {
		err := conv.bindOffsets(ancestor, semTable)
		if err != nil {
			return err
		}
	}

	return node.Op.PlanOffsets(node, semTable, conv)
}

func (v *View) PlanOffsets(node *Node, st *semantics.SemTable, _ *Converter) error {
	ancestor := node.Ancestors[0]
	for _, param := range v.Dependencies {
		var err error
		dbg.Assert(len(param.Column.AST) == 1, "view with multiple ASTs for a parameter")

		param.ColumnOffset, err = ancestor.ExprLookup(st, param.Column.AST[0])
		if err != nil {
			return err
		}

		param.Column.AST[0] = sqlparser.Rewrite(param.Column.AST[0], func(cursor *sqlparser.Cursor) bool {
			switch col := cursor.Node().(type) {
			case *sqlparser.ColName:
				cursor.Replace(sqlparser.NewOffset(param.ColumnOffset, col))
				return false
			case *sqlparser.Offset:
				return false
			}
			return true
		}, nil).(sqlparser.Expr)
	}
	return nil
}

func (g *GroupBy) PlanOffsets(node *Node, st *semantics.SemTable, _ *Converter) error {
	input := node.Ancestors[0]

	for _, grouping := range g.Grouping {
		ast, err := grouping.SingleAST()
		if err != nil {
			return err
		}
		offset, err := input.ExprLookup(st, ast)
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
			offset, err = input.ExprLookup(st, aggr.GetArg())
			if err != nil {
				return err
			}
		default:
			offset, err = input.ExprLookup(st, aggr)
			if err != nil {
				return err
			}
		}

		g.AggregationsIdx = append(g.AggregationsIdx, offset)
	}
	return nil
}

func (j *Join) PlanOffsets(node *Node, st *semantics.SemTable, _ *Converter) error {
	lhsNode := node.Ancestors[0]
	rhsNode := node.Ancestors[1]
	lhsID := lhsNode.Covers()
	rhsID := rhsNode.Covers()

	for _, col := range j.EmitColumns {
		isJoinCondition := slices.Index(j.JoinColumns, col) >= 0

		if isJoinCondition && j.Inner {
			err := j.handleJoinColumn(col, st, lhsID, rhsID, lhsNode, rhsNode)
			if err != nil {
				return err
			}
			continue
		}

		for _, expr := range col.AST {
			deps := st.DirectDeps(expr)
			switch {
			case deps.IsSolvedBy(lhsID):
				offset, err := lhsNode.ExprLookup(st, expr)
				if err != nil {
					return err
				}
				j.Emit = append(j.Emit, [2]int{offset, -1})
				if isJoinCondition {
					j.On[0] = offset
				}
			case deps.IsSolvedBy(rhsID):
				offset, err := rhsNode.ExprLookup(st, expr)
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

func (p *Project) PlanOffsets(node *Node, st *semantics.SemTable, _ *Converter) error {
	input := node.Ancestors[0]

	for _, col := range p.Columns {
		ast, err := col.SingleAST()
		if err != nil {
			return err
		}
		var proj Projection
		switch expr := ast.(type) {
		case *sqlparser.ColName, sqlparser.AggrFunc:
			offset, err := input.ExprLookup(st, expr)
			if err != nil {
				return err
			}
			proj = Projection{Kind: ProjectionColumn, Column: offset}
		case *sqlparser.Literal:
			proj = Projection{Kind: ProjectionLiteral, AST: expr}
		case *sqlparser.Offset:
			proj = Projection{Kind: ProjectionColumn, Column: expr.V}
		default:
			newExpr, err := rewriteColNamesToOffsets(st, node.Ancestors[0], expr)
			if err != nil {
				return err
			}
			eexpr, err := evalengine.Translate(newExpr, &evalengine.Config{
				ResolveColumn: columnLookup(input, st),
				ResolveType:   st.TypeForExpr,
				Collation:     st.Collation,
			})
			if err != nil {
				return &UnsupportedError{AST: expr, Type: EvalEngineNotSupported}
			}
			proj = Projection{Kind: ProjectionEval, AST: newExpr, Eval: eexpr}
		}
		proj.Original = ast
		p.Projections = append(p.Projections, proj)
	}
	return nil
}

func (n *NullFilter) PlanOffsets(node *Node, st *semantics.SemTable, _ *Converter) error {
	input := node.Ancestors[0]

	newPredicate := sqlparser.Rewrite(sqlparser.CloneExpr(n.Predicates), findOffsets(st, input), nil).(sqlparser.Expr)

	for _, col := range node.Columns {
		ast, err := col.SingleAST()
		if err != nil {
			return err
		}

		col, ok := ast.(*sqlparser.ColName)
		deps := st.DirectDeps(col)

		if ok && deps != n.OuterSide {
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
			if st.DirectDeps(col) != n.OuterSide {
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
				Original: expr,
			})
			return false
		default:
			return true
		}
	}
}

func (f *Filter) PlanOffsets(node *Node, st *semantics.SemTable, _ *Converter) error {
	input := node.Ancestors[0]
	var err error
	newPredicate := sqlparser.Rewrite(sqlparser.CloneExpr(f.Predicates), findOffsets(st, input), nil)
	if err != nil {
		return err
	}
	for _, expr := range sqlparser.SplitAndExpression(nil, newPredicate.(sqlparser.Expr)) {
		if _, err := evalengine.Translate(expr, nil); err != nil {
			return err
		}
		f.EvalExpr = append(f.EvalExpr, expr)
	}
	return nil
}

func (u *Union) PlanOffsets(node *Node, st *semantics.SemTable, conv *Converter) error {
	var err error
	node.Ancestors[0], u.ColumnsIdx[0], err = conv.bindUnionSideOffset(u.InputColumns[0], node.Ancestors[0], st)
	if err != nil {
		return err
	}
	node.Ancestors[1], u.ColumnsIdx[1], err = conv.bindUnionSideOffset(u.InputColumns[1], node.Ancestors[1], st)
	if err != nil {
		return err
	}
	return nil
}

func (n *NodeTableRef) PlanOffsets(*Node, *semantics.SemTable, *Converter) error {
	// do nothing
	return nil
}

func (t *Table) PlanOffsets(*Node, *semantics.SemTable, *Converter) error {
	// do nothing
	return nil
}

func (t *TopK) PlanOffsets(node *Node, st *semantics.SemTable, _ *Converter) error {
	ancestor := node.Ancestors[0]
	for _, param := range t.Order {
		offset, err := ancestor.ExprLookup(st, param.Expr)
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
		offset, err := ancestor.ExprLookup(st, col)
		if err != nil {
			return err
		}
		t.ParamOffsets = append(t.ParamOffsets, offset)
	}
	return nil
}

func (d *Distinct) PlanOffsets(*Node, *semantics.SemTable, *Converter) error {
	// nothing to do
	return nil
}

func (conv *Converter) bindUnionSideOffset(columns Columns, node *Node, st *semantics.SemTable) (*Node, []int, error) {
	var offsets []int
	for _, column := range columns {
		ast, err := column.SingleAST()
		if err != nil {
			return nil, nil, err
		}
		idx, err := node.ExprLookup(st, ast)
		if err != nil {
			return nil, nil, err
		}
		offsets = append(offsets, idx)
	}

	if slices.IsSorted(offsets) {
		return node, offsets, nil
	}

	// Union requires input columns to be sorted. If they aren't already, we stick a projection in front
	// that will do nothing except rearrange the columns
	proj := &Project{}

	projNode := conv.NewNode("union_align_project", proj, []*Node{node})

	type tuple struct {
		oldIdx, offset int
	}

	newColumns := slices2.MapIdx(offsets, func(idx, from int) tuple {
		t := tuple{
			oldIdx: idx,
			offset: from,
		}
		return t
	})

	slices.SortFunc(newColumns, func(a, b tuple) bool {
		return a.offset < b.offset
	})

	for idx, col := range newColumns {
		column := node.Columns[col.oldIdx]
		proj.Columns = append(proj.Columns, column)
		projNode.Columns = append(projNode.Columns, column)
		proj.Projections = append(proj.Projections, Projection{
			Kind:     ProjectionColumn,
			Original: column.AST[0],
			Column:   col.oldIdx,
		})
		offsets[idx] = idx
	}

	return projNode, offsets, nil
}

func columnLookup(node *Node, semTable *semantics.SemTable) evalengine.ColumnResolver {
	return func(expr *sqlparser.ColName) (int, error) {
		for i, column := range node.Columns {
			if column.EqualsAST(semTable, expr, true) {
				return i, nil
			}
		}
		dbg.Bug("column not found during lookup")
		return -1, nil
	}
}
