package operators

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestOuterToInner(t *testing.T) {
	tests := []struct {
		pred            string
		inner, pushable bool
	}{
		{pred: "rgt.col = 1", inner: true, pushable: true},
		{pred: "lft.col = 1", inner: false, pushable: true},
		{pred: "lft.col = rgt.col", inner: true, pushable: true},
		{pred: "func(lft.col) = func(rgt.col)", inner: false, pushable: false},
		{pred: "func(rgt.col) = 1", inner: false, pushable: false},
	}

	for _, tc := range tests {
		t.Run(tc.pred, func(t *testing.T) {
			predicate, err := sqlparser.ParseExpr(tc.pred)
			require.NoError(t, err)
			lftID := semantics.SingleTableSet(1)
			rgtID := semantics.SingleTableSet(2)
			lft := &Node{
				Name: "lft",
				Op:   &fakeOp{id: lftID},
			}
			rgt := &Node{
				Name: "rgt",
				Op:   &fakeOp{id: rgtID},
			}
			joinOp := &Join{Inner: false}
			join := &Node{
				Name:      "join",
				Op:        joinOp,
				Ancestors: []*Node{lft, rgt},
			}
			filter := &Node{
				Name: "filter",
				Op: &Filter{
					Predicates:             predicate,
					EvalExpr:               nil,
					ExprStr:                nil,
					doesNotIntroduceColumn: doesNotIntroduceColumn{},
					keepsAncestorColumns:   keepsAncestorColumns{},
				},
				Ancestors: []*Node{join},
			}

			conv := NewConverter()
			st := semantics.EmptySemTable()
			ctx := &PlanContext{
				SemTable: st,
			}
			_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				col, ok := node.(*sqlparser.ColName)
				if ok {
					switch col.Qualifier.Name.String() {
					case "lft":
						st.Recursive[col] = lftID
						st.Direct[col] = lftID
					case "rgt":
						st.Recursive[col] = rgtID
						st.Direct[col] = rgtID
					}
				}
				return true, nil
			}, predicate)
			rewriter := conv.pushDownPredicate(ctx)
			result, err := rewrite(filter, rewriter)
			require.NoError(t, err)
			if tc.inner {
				require.True(t, joinOp.Inner)
			} else {
				require.False(t, joinOp.Inner)
			}
			if tc.pushable {
				require.Same(t, result, join)
			} else {
				require.Same(t, result, filter)
			}
		})
	}
}
