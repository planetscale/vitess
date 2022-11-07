package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func (conv *Converter) pushDownPredicate(ctx *PlanContext) RewriteOpFunc {
	return func(node *Node) (*Node, error) {
		switch op := node.Op.(type) {
		case *Filter:
			filter := op
			inputNode := node.Ancestors[0]
			switch inputOp := inputNode.Op.(type) {
			case *Filter:
				// If we have two filters on top of each other, merge them into a single one
				op.Predicates = sqlparser.AndExpressions(op.Predicates, inputOp.Predicates)
				node.Ancestors = inputNode.Ancestors
				return node, nil
			case *Join:
				return conv.pushDownPredicateThroughJoin(ctx, node, inputNode, filter, inputOp)
			default:
				return conv.pushDownToSingleAncestor(node, inputNode, op, ctx, filter), nil

			}
		}
		return node, nil
	}
}

func (conv *Converter) pushDownToSingleAncestor(node *Node, inputNode *Node, op *Filter, ctx *PlanContext, filter *Filter) *Node {
	if len(inputNode.Ancestors) != 1 {
		// if we are dealing with nodes with less or more than a single source, we can't push here
		return node
	}
	inputInput := inputNode.Ancestors[0]
	// first, let's figure out which predicates we can push and which we can't
	predicates := sqlparser.SplitAndExpression(nil, op.Predicates)
	var canPush, canNotPush []sqlparser.Expr
	for _, predicate := range predicates {
		deps := ctx.SemTable.DirectDeps(predicate)
		if deps.IsSolvedBy(inputInput.Covers()) {
			canPush = append(canPush, predicate)
		} else {
			canNotPush = append(canNotPush, predicate)
		}
	}

	// now we have two sets - some that we can, and some that we can't push. handle all combos
	pushThese := sqlparser.AndExpressions(canPush...)
	switch {
	case len(canPush) == 0:
		return node
	case len(canNotPush) == 0:
		// we can remove the Filter we started with, and create a new one under the ancestor of the original filter
		filterAncestor := inputNode.Ancestors[0]
		inputNode.Ancestors[0] = conv.NewNode("filter", &Filter{Predicates: pushThese}, []*Node{filterAncestor})
		return inputNode
	default:
		// we can push down some, but not all predicate. keep this filter _and_ create a new one under the ancestor
		filterAncestor := inputNode.Ancestors[0]
		inputNode.Ancestors[0] = conv.NewNode("filter", &Filter{Predicates: pushThese}, []*Node{filterAncestor})
		filter.Predicates = sqlparser.AndExpressions(canNotPush...)
		return node
	}
}

// check if we can push down predicates either to one of the sides of the join,
// or into the join as join predicates
func (conv *Converter) pushDownPredicateThroughJoin(ctx *PlanContext, filterNode, joinNode *Node, filterOp *Filter, join *Join) (*Node, error) {
	predicates := sqlparser.SplitAndExpression(nil, filterOp.Predicates)
	var notPushedPredicates []sqlparser.Expr
	switchedOuterToInner := false
	lhs := joinNode.Ancestors[0]
	rhs := joinNode.Ancestors[1]
	for _, predicate := range predicates {
		deps := ctx.SemTable.DirectDeps(predicate)
		canGoLeft := deps.IsSolvedBy(lhs.Covers())
		canGoRight := deps.IsSolvedBy(rhs.Covers())
		switch {
		case canGoLeft:
			joinNode.Ancestors[0] = conv.NewNode("filter", &Filter{Predicates: predicate}, []*Node{lhs})
			lhs = joinNode.Ancestors[0]
		case canGoRight:
			if !join.Inner {
				// if this is an outer join, let's check if this predicate turns it into an inner join.
				join.tryConvertToInnerJoin(predicate, ctx.SemTable, rhs)
				switchedOuterToInner = join.Inner
			}

			// for inner joins, it's safe to push down the predicate to the RHS
			// if we are still an outer join, we are done here. can't push on the rhs of an outer join
			if join.Inner {
				joinNode.Ancestors[1] = conv.NewNode("filter", &Filter{Predicates: predicate}, []*Node{rhs})
				rhs = joinNode.Ancestors[1]
			} else {
				notPushedPredicates = append(notPushedPredicates, predicate)
			}
		case deps.IsSolvedBy(lhs.Covers().Merge(rhs.Covers())):
			if !join.Inner {
				// if this is an outer join, let's check if this predicate turns it into an inner join.
				join.tryConvertToInnerJoin(predicate, ctx.SemTable, rhs)
				switchedOuterToInner = join.Inner
			}

			// we need both sides of the join to evaluate this expression.
			// If it's the right kind of comparison, we can evaluate it as a join predicate,
			// but only if it is an inner join - outer joins must still be
			if join.Inner && canBeUsedForEquiJoin(predicate) {
				join.Predicates = sqlparser.AndExpressions(join.Predicates, predicate)
			} else {
				notPushedPredicates = append(notPushedPredicates, predicate)
			}
		default:
			return nil, NewBug("weird dependencies for a predicate")
		}
	}

	if switchedOuterToInner {
		// if we were able to turn an outer join into an inner join, we go over the notPushedPredicates again.
		// the switch could have happened on the very last predicate, and the ones we couldn't push to the
		// rhs because this is an outer join, can now safely be pushed down as well
		tryAgain := notPushedPredicates
		notPushedPredicates = nil

		for _, predicate := range tryAgain {
			deps := ctx.SemTable.DirectDeps(predicate)
			canGoRight := deps.IsSolvedBy(rhs.Covers())
			if canGoRight {
				joinNode.Ancestors[1] = conv.NewNode("filter", &Filter{Predicates: predicate}, []*Node{rhs})
				continue
			}
			notPushedPredicates = append(notPushedPredicates, predicate)
		}
	}

	if len(notPushedPredicates) == 0 {
		return joinNode, nil
	}

	filterOp.Predicates = sqlparser.AndExpressions(notPushedPredicates...)
	return filterNode, nil
}

func canBeUsedForEquiJoin(e sqlparser.Expr) bool {
	cmp, ok := e.(*sqlparser.ComparisonExpr)
	if !ok {
		return false
	}

	if cmp.Operator != sqlparser.EqualOp {
		return false
	}
	_, lft := cmp.Left.(*sqlparser.ColName)
	_, rgt := cmp.Right.(*sqlparser.ColName)
	return lft && rgt
}

// When a predicate uses information from an outer table, we can convert from an outer join to an inner join
// if the predicate is "null-intolerant".
//
// Null-intolerant in this context means that the predicate will not be true if the table columns are null.
//
// Since an outer join is an inner join with the addition of all the rows from the left-hand side that
// matched no rows on the right-hand, if we are later going to remove all the rows where the right-hand
// side did not match, we might as well turn the join into an inner join.
//
// This is based on the paper "Canonical Abstraction for Outerjoin Optimization" by J Rao et al
func (j *Join) tryConvertToInnerJoin(expr sqlparser.Expr, semTable *semantics.SemTable, rhsNode *Node) {
	depsFor := semTable.RecursiveDeps
	if j.Inner ||
		// we should only do this if there really is a dependency on the RHS,
		// and not just a predicate without any table dependencies at all
		depsFor(expr).NumberOfTables() == 0 {
		return
	}

	isColName := sqlparser.IsColName

	rhs := rhsNode.Covers()
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if expr.Operator == sqlparser.NullSafeEqualOp {
			break
		}

		// the expression has to be a bare column - if the dependencies to the RHS is through,
		// for example a function, the function could make the value non-null,
		// and then we are not sure if this is a null-intolerant predicate or not
		if isColName(expr.Left) && depsFor(expr.Left).IsSolvedBy(rhs) ||
			isColName(expr.Right) && depsFor(expr.Right).IsSolvedBy(rhs) {
			j.Inner = true
		}

	case *sqlparser.IsExpr:
		// Except for IS NOT NULL, all other IS expressions - NULL, TRUE, NOT TRUE, FALSE and NOT FALSE
		// will return false for NULL values, and so are null-intolerant
		if expr.Right != sqlparser.IsNotNullOp {
			break
		}

		if isColName(expr.Left) && depsFor(expr.Left).IsSolvedBy(rhs) {
			j.Inner = true
		}
	}
}
