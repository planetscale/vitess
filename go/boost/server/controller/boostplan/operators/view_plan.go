package operators

import (
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/server/controller/boostplan/viewplan"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
)

func paramKind(op sqlparser.ComparisonExprOperator) viewplan.Param_Kind {
	switch op {
	case sqlparser.EqualOp:
		return viewplan.Param_EQUALITY
	case sqlparser.LessThanOp, sqlparser.GreaterThanOp, sqlparser.LessEqualOp, sqlparser.GreaterEqualOp:
		return viewplan.Param_RANGE
	case sqlparser.NotEqualOp, sqlparser.NullSafeEqualOp:
		return viewplan.Param_POST
	case sqlparser.InOp, sqlparser.NotInOp, sqlparser.LikeOp, sqlparser.NotLikeOp:
		return viewplan.Param_POST
	default:
		return viewplan.Param_UNSUPPORTED
	}
}

type viewParameter struct {
	Pos      int
	Original *Dependency
	Param    *viewplan.Param
}

func (p viewParameter) op() sqlparser.ComparisonExprOperator {
	return *p.Original.Op
}

func (p viewParameter) rank() int {
	switch p.op() {
	case sqlparser.InOp:
		return 0
	case sqlparser.EqualOp:
		return 1
	case sqlparser.GreaterEqualOp, sqlparser.GreaterThanOp:
		return 2
	case sqlparser.LessEqualOp, sqlparser.LessThanOp:
		return 3
	default:
		return 4
	}
}

func (p viewParameter) sameDirection(p2 viewParameter) bool {
	switch p2.op() {
	case sqlparser.GreaterEqualOp, sqlparser.GreaterThanOp:
		return p.op() == sqlparser.GreaterEqualOp || p.op() == sqlparser.GreaterThanOp
	case sqlparser.LessThanOp, sqlparser.LessEqualOp:
		return p.op() == sqlparser.LessThanOp || p.op() == sqlparser.LessEqualOp
	default:
		return false
	}
}

type viewPlan []viewParameter

func rangeError(typ UnsupportedRangeType, params ...viewParameter) error {
	cols := make([]string, 0, len(params))
	for _, p := range params {
		cols = append(cols, p.Original.Column.Name)
	}
	return &UnsupportedRangeError{Columns: cols, Type: typ}
}

func diffColumns(ctx *PlanContext, from, to []*Column) []*Column {
	var diff []*Column
	for _, a := range from {
		contained := slices.ContainsFunc(to, func(c *Column) bool {
			return c.Equals(ctx.SemTable, a, true)
		})
		if !contained {
			diff = append(diff, a)
		}
	}
	return diff
}

func (vp viewPlan) PlanMultiLookups() {
	// IN-planning: we can implement IN operators as POST-functions or multi-lookups.
	// We assume that any function that contains an IN + only equalities is most performantly
	// served as a multi-lookup. More complex functions (e.g. INs with other non-equality operators or
	// post-operators) cannot be served via Multi-lookup.
	if vp[0].op() == sqlparser.InOp && slices2.All(vp[1:], func(p viewParameter) bool { return p.Param.Kind == viewplan.Param_EQUALITY }) {
		vp[0].Param.Kind = viewplan.Param_MULTI_EQUALITY
	}
}

func (vp viewPlan) CheckRanges() error {
	for i, p := range vp {
		switch p.Param.Kind {
		case viewplan.Param_EQUALITY:
		case viewplan.Param_RANGE:
			for _, p2 := range vp[:i] {
				if p2.Param.Col == p.Param.Col {
					if p.sameDirection(p2) {
						return rangeError(RangeSameDirection, p2, p)
					}
					if *p2.Original.Op == sqlparser.EqualOp {
						return rangeError(RangeEqualityOperator, p2, p)
					}
				}
			}
		}
	}
	return nil
}

func (vp viewPlan) PlanPostFilters() {
	for i, p := range vp {
		if p.Param.Kind == viewplan.Param_RANGE {
			for _, p2 := range vp[:i] {
				if p2.Param.Col == p.Param.Col {
					continue
				}
				if p2.Param.Kind == viewplan.Param_RANGE {
					p.Param.Kind = viewplan.Param_POST
					break
				}
			}
		}
		if p.Param.Kind == viewplan.Param_POST {
			expr := &sqlparser.ComparisonExpr{
				Operator: p.op(),
				Left:     sqlparser.NewOffset(p.Original.ColumnOffset, p.Original.Column.AST[0]),
			}
			switch expr.Operator {
			case sqlparser.InOp, sqlparser.NotInOp:
				expr.Right = sqlparser.NewListArg(p.Original.Name)
			default:
				expr.Right = sqlparser.NewArgument(p.Original.Name)
			}
			p.Param.PostFilter = &sql.Expr{Expr: sqlparser.String(expr)}
		}
	}
}

func (vp viewPlan) ExternalKey() (externalKey []int) {
	for _, p := range vp {
		if p.Param.Kind == viewplan.Param_POST {
			continue
		}
		if slices.Contains(externalKey, p.Param.Col) {
			continue
		}
		externalKey = append(externalKey, p.Param.Col)
	}
	return
}

func (vp viewPlan) InternalKey() (internalKey []int) {
	if vp.HasEquality() && vp.HasRange() {
		for _, p := range vp {
			if p.Param.Kind != viewplan.Param_EQUALITY {
				continue
			}
			if slices.Contains(internalKey, p.Param.Col) {
				continue
			}
			internalKey = append(internalKey, p.Param.Col)
		}
		return
	}
	return vp.ExternalKey()
}

func (vp viewPlan) HasEquality() bool {
	return slices.ContainsFunc(vp, func(p viewParameter) bool { return p.Param.Kind == viewplan.Param_EQUALITY })
}

func (vp viewPlan) HasRange() bool {
	return slices.ContainsFunc(vp, func(p viewParameter) bool { return p.Param.Kind == viewplan.Param_RANGE })
}

func (vp viewPlan) PlanAggregations(ctx *PlanContext, node *Node) (agg []viewplan.Aggregation, aggGroupBy []int, err error) {
	groupBy := findGroupBy(node)
	if groupBy == nil {
		return
	}

	var implicits int

	for _, param := range vp {
		impl := slices.IndexFunc(groupBy.ImplicitGrouping, func(col *Column) bool {
			return param.Original.Column.Equals(ctx.SemTable, col, true)
		})
		if impl < 0 {
			continue
		}

		switch param.Param.Kind {
		case viewplan.Param_EQUALITY, viewplan.Param_MULTI_EQUALITY:
			// OK, the results are scalars and can be returned as-is
		case viewplan.Param_RANGE:
			return nil, nil, &UnsupportedError{Type: RangeAndAggregation, AST: param.Original.Column.AST[0]}
		case viewplan.Param_POST:
			// OK, but need to plan a post-processing aggregation
			implicits++
		}
	}

	if implicits > 0 {
		var err error
		agg, err = planViewPostAggregation(node.Columns)
		if err != nil {
			return nil, nil, err
		}

		ancestor := node.Ancestors[0]
		for _, col := range diffColumns(ctx, groupBy.Grouping, groupBy.ImplicitGrouping) {
			offset, err := ancestor.ExprLookup(ctx.SemTable, col.AST[0])
			if err != nil {
				return nil, nil, err
			}
			aggGroupBy = append(aggGroupBy, offset)
		}
	}

	return
}

func (vp viewPlan) TreeKey() *viewplan.Plan_TreeKey {
	if !vp.HasRange() {
		return nil
	}

	tk := &viewplan.Plan_TreeKey{}
	for _, p := range vp {
		if p.Param.Kind == viewplan.Param_POST {
			continue
		}

		switch p.op() {
		case sqlparser.EqualOp:
			tk.Lower = append(tk.Lower, p.Pos)
			tk.Upper = append(tk.Upper, p.Pos)

		case sqlparser.GreaterEqualOp:
			tk.LowerInclusive = true
			fallthrough

		case sqlparser.GreaterThanOp:
			tk.Lower = append(tk.Lower, p.Pos)

		case sqlparser.LessEqualOp:
			tk.UpperInclusive = true
			fallthrough

		case sqlparser.LessThanOp:
			tk.Upper = append(tk.Upper, p.Pos)

		case sqlparser.InOp:
		default:
			panic("should never find an unsupported operator in the plan")
		}
	}
	return tk
}

func (vp viewPlan) ParameterKey() (parameterKey []int) {
	for _, p := range vp {
		switch p.Param.Kind {
		case viewplan.Param_EQUALITY, viewplan.Param_MULTI_EQUALITY:
			parameterKey = append(parameterKey, p.Pos)
		}
	}
	return
}

func (vp viewPlan) Sort() {
	slices.SortStableFunc(vp, func(a, b viewParameter) int {
		// TODO: switch to cmp.Compare for Go 1.21+.
		//
		// https://pkg.go.dev/cmp@master#Compare.
		switch {
		case a.rank() < b.rank():
			return -1
		case a.rank() > b.rank():
			return 1
		default:
			return 0
		}
	})
}

func (view *View) plan(ctx *PlanContext, node *Node) error {
	var queryOrder []*viewplan.Param
	var planOrder viewPlan

	for i, param := range view.Dependencies {
		if param.Op == nil {
			continue
		}
		if param.ColumnOffset < 0 {
			panic("did not resolve column for View parameter")
		}

		p := &viewplan.Param{
			Name: param.Name,
			Kind: paramKind(*param.Op),
			Col:  param.ColumnOffset,
		}

		if p.Kind == viewplan.Param_UNSUPPORTED {
			return fmt.Errorf("unsupported operator %v", param.Op)
		}

		queryOrder = append(queryOrder, p)
		planOrder = append(planOrder, viewParameter{Pos: i, Original: param, Param: p})
	}

	planOrder.Sort()
	planOrder.PlanMultiLookups()

	if err := planOrder.CheckRanges(); err != nil {
		return err
	}

	planOrder.PlanPostFilters()

	agg, aggGroupBy, err := planOrder.PlanAggregations(ctx, node)
	if err != nil {
		return err
	}

	externalKey := planOrder.ExternalKey()
	if len(externalKey) == 0 {
		return &UnsupportedError{Type: NoIndexableColumn}
	}

	internalKey := planOrder.InternalKey()
	if len(internalKey) == 0 {
		panic("did not properly compute internal key")
	}

	treeKey := planOrder.TreeKey()
	parameterKey := planOrder.ParameterKey()

	view.Plan = &viewplan.Plan{
		AllowPartialMaterialization: planOrder.HasEquality(),
		Parameters:                  queryOrder,
		ParameterKey:                parameterKey,
		TreeKey:                     treeKey,
		ColumnsForUser:              len(view.Columns),
		InternalStateKey:            internalKey,
		ExternalStateKey:            externalKey,
		PostAggregation:             agg,
		PostGroupBy:                 aggGroupBy,
	}
	return nil
}

func planViewPostAggregation(rowColumns []*Column) (aggregations []viewplan.Aggregation, err error) {
	for _, col := range rowColumns {
		kind := viewplan.Aggregation_FIRST
		if agg, ok := col.AST[0].(sqlparser.AggrFunc); ok {
			switch agg := agg.(type) {
			case *sqlparser.CountStar, *sqlparser.Count:
				kind = viewplan.Aggregation_COUNT
			case *sqlparser.Sum:
				kind = viewplan.Aggregation_SUM
			case *sqlparser.Min:
				kind = viewplan.Aggregation_MIN
			case *sqlparser.Max:
				kind = viewplan.Aggregation_MAX
			default:
				return nil, fmt.Errorf("unsupported post-aggregation: %s", sqlparser.CanonicalString(agg))
			}
		}
		aggregations = append(aggregations, kind)
	}
	return
}

func findGroupBy(node *Node) *GroupBy {
	switch op := node.Op.(type) {
	case *GroupBy:
		return op
	default:
		for _, p := range node.Ancestors {
			if op := findGroupBy(p); op != nil {
				return op
			}
		}
	}
	return nil
}
