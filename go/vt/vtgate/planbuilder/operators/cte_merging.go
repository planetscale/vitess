package operators

import (
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
	initRoute, tailRoute, _, routingB, a, _, _ := prepareInputRoutes(init, tail)
	if initRoute == nil {
		return nil
	}

	switch {
	case a == dual:
		return mergeCTE(ctx, initRoute, tailRoute, routingB, in)
	default:
		return nil
	}
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
