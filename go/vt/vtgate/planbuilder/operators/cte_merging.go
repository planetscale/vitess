package operators

import (
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func tryMergeRecurse(ctx *plancontext.PlanningContext, in *Recurse) (Operator, *ApplyResult) {
	op := tryMergeCTE(ctx, in.Init, in.Tail, in)
	if op == nil {
		return in, NoRewrite
	}

	return op, Rewrote("Merged CTE")
}

func tryMergeCTE(ctx *plancontext.PlanningContext, init, tail Operator, in *Recurse) *Route {
	initRoute, tailRoute, _, routingB, a, b, sameKeyspace := prepareInputRoutes(init, tail)
	if initRoute == nil || !sameKeyspace {
		return nil
	}

	switch {
	case a == dual:
		return mergeCTE(ctx, initRoute, tailRoute, routingB, in)
	case a == sharded && b == sharded:
		return tryMergeCTESharded(ctx, initRoute, tailRoute, in)
	default:
		return nil
	}
}

func tryMergeCTESharded(ctx *plancontext.PlanningContext, init, tail *Route, in *Recurse) *Route {
	tblA := init.Routing.(*ShardedRouting)
	tblB := tail.Routing.(*ShardedRouting)
	switch tblA.RouteOpCode {
	case engine.EqualUnique:
		// If the two routes fully match, they can be merged together.
		if tblB.RouteOpCode == engine.EqualUnique {
			aVdx := tblA.SelectedVindex()
			bVdx := tblB.SelectedVindex()
			aExpr := tblA.VindexExpressions()
			bExpr := tblB.VindexExpressions()
			if aVdx == bVdx && gen4ValuesEqual(ctx, aExpr, bExpr) {
				return mergeCTE(ctx, init, tail, tblA, in)
			}
		}
	}

	return nil
}

func mergeCTE(ctx *plancontext.PlanningContext, init, tail *Route, r Routing, in *Recurse) *Route {
	return &Route{
		Routing: r,
		Source: &Recurse{
			Name:        in.Name,
			ColumnNames: in.ColumnNames,
			Init:        init.Source,
			Tail:        tail.Source,
		},
		MergedWith: []*Route{tail},
	}
}
