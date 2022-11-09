package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

func cleanUpTree(ctx *PlanContext, node *Node, cleanHere bool) (*Node, error) {
	for i, ancestor := range node.Ancestors {
		next, err := cleanUpTree(ctx, ancestor, shouldCleanUpHere(node.Op))
		if err != nil {
			return nil, err
		}
		node.Ancestors[i] = next
	}

	if !cleanHere {
		return node, nil
	}

	switch op := node.Op.(type) {
	case *Project:
		// When the table id of a projection is not nil, it means we project a derived table,
		// in this case we don't want to remove it.
		if op.TableID != nil {
			return node, nil
		}

		required := false
		for _, column := range op.Columns {
			expr, err := column.SingleAST() // Projection nodes should not have columns that represent multiple AST expressions
			if err != nil {
				return nil, err
			}

			if _, isCol := expr.(*sqlparser.ColName); !isCol {
				required = true
				break
			}
		}

		if !required {
			return node.Ancestors[0], nil
		}
	case *Join:
		if op.Predicates == nil {
			return nil, &UnsupportedError{
				Type: JoinWithoutPredicates,
			}
		}
	}

	return node, nil
}

// shouldCleanUpHere returns true if the op we are cleaning under is one that needs a
// projection to clean up the incoming columns
func shouldCleanUpHere(op Operator) bool {
	switch op.(type) {
	case *View, *TopK, *Distinct:
		return false
	}
	return true
}
