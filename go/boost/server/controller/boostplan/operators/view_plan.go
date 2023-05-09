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
		return viewplan.Param_SINGLE
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

type planParameter struct {
	Pos      int
	Original *Dependency
	Param    *viewplan.Param
}

func (p *planParameter) op() sqlparser.ComparisonExprOperator {
	return *p.Original.Op
}

func (p *planParameter) rank() int {
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

func (p *planParameter) sameDirection(p2 *planParameter) bool {
	switch p2.op() {
	case sqlparser.GreaterEqualOp, sqlparser.GreaterThanOp:
		return p.op() == sqlparser.GreaterEqualOp || p.op() == sqlparser.GreaterThanOp
	case sqlparser.LessThanOp, sqlparser.LessEqualOp:
		return p.op() == sqlparser.LessThanOp || p.op() == sqlparser.LessEqualOp
	default:
		return false
	}
}

func rangeError(typ UnsupportedRangeType, params ...*planParameter) error {
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

func (view *View) plan(ctx *PlanContext, node *Node) error {
	deps := view.Dependencies

	queryOrder := make([]*viewplan.Param, 0, len(deps))
	planOrder := make([]planParameter, 0, len(deps))

	for i, param := range deps {
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
		planOrder = append(planOrder, planParameter{Pos: i, Original: param, Param: p})
	}

	slices.SortStableFunc(planOrder, func(a, b planParameter) bool {
		return a.rank() < b.rank()
	})

	// IN-planning: we can implement IN operators as POST-functions or multi-lookups.
	// We assume that any function that contains an IN + only equalities is most performantly
	// served as a multi-lookup. More complex functions (e.g. INs with other non-equality operators or
	// post-operators) cannot be served via Multi-lookup.
	if planOrder[0].op() == sqlparser.InOp && slices2.All(planOrder[1:], func(p planParameter) bool { return p.Param.Kind == viewplan.Param_SINGLE }) {
		planOrder[0].Param.Kind = viewplan.Param_MULTI
	}

	for i, p := range planOrder {
		switch p.Param.Kind {
		case viewplan.Param_SINGLE:
		case viewplan.Param_RANGE:
			for _, p2 := range planOrder[:i] {
				if p2.Param.Col == p.Param.Col {
					if p.sameDirection(&p2) {
						return rangeError(RangeSameDirection, &p2, &p)
					}
					if *p2.Original.Op == sqlparser.EqualOp {
						return rangeError(RangeEqualityOperator, &p2, &p)
					}
					continue
				}
				if p2.Param.Kind == viewplan.Param_RANGE {
					p.Param.Kind = viewplan.Param_POST
				}
			}
		}
	}

	var key []int
	var ranged bool

	for _, p := range planOrder {
		if p.Param.Kind == viewplan.Param_POST {
			continue
		}
		if p.Param.Kind == viewplan.Param_RANGE {
			ranged = true
		}
		if slices.Contains(key, p.Param.Col) {
			continue
		}
		key = append(key, p.Param.Col)
	}

	if len(key) == 0 {
		return &UnsupportedError{Type: NoIndexableColumn}
	}

	view.Plan = &viewplan.Plan{
		ColumnsForUser: len(node.Columns),
	}

	var agg []viewplan.Aggregation
	var aggGroupBy []int

	if groupBy := findGroupBy(node); groupBy != nil {
		var implicits int

		for _, param := range planOrder {
			impl := slices.IndexFunc(groupBy.ImplicitGrouping, func(col *Column) bool {
				return param.Original.Column.Equals(ctx.SemTable, col, true)
			})
			if impl < 0 {
				continue
			}

			switch param.Param.Kind {
			case viewplan.Param_SINGLE, viewplan.Param_MULTI:
				// OK, the results are scalars and can be returned as-is
			case viewplan.Param_RANGE:
				return &UnsupportedError{Type: RangeAndAggregation, AST: param.Original.Column.AST[0]}
			case viewplan.Param_POST:
				// OK, but need to plan a post-processing aggregation
				implicits++
			}
		}

		if implicits > 0 {
			var err error
			agg, err = planViewPostAggregation(node.Columns)
			if err != nil {
				return err
			}

			ancestor := node.Ancestors[0]
			for _, col := range diffColumns(ctx, groupBy.Grouping, groupBy.ImplicitGrouping) {
				offset, err := ancestor.ExprLookup(ctx.SemTable, col.AST[0])
				if err != nil {
					return err
				}
				aggGroupBy = append(aggGroupBy, offset)
			}
		}
	}

	var tk *viewplan.Plan_TreeKey
	var mk []int

	if ranged {
		tk = &viewplan.Plan_TreeKey{}
		for _, p := range planOrder {
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
	} else {
		for _, p := range planOrder {
			if p.Param.Kind == viewplan.Param_POST {
				continue
			}
			mk = append(mk, p.Pos)
		}
	}

	for _, p := range planOrder {
		if p.Param.Kind != viewplan.Param_POST {
			continue
		}
		expr := &sqlparser.ComparisonExpr{
			Operator: p.op(),
			Left:     sqlparser.NewOffset(p.Original.ColumnOffset, p.Original.Column.AST[0]),
		}
		switch p.op() {
		case sqlparser.InOp, sqlparser.NotInOp:
			expr.Right = sqlparser.NewListArg(p.Original.Name)
		default:
			expr.Right = sqlparser.NewArgument(p.Original.Name)
		}
		p.Param.PostFilter = &sql.Expr{Expr: sqlparser.String(expr)}
	}

	view.Plan = &viewplan.Plan{
		Parameters:      queryOrder,
		MapKey:          mk,
		TreeKey:         tk,
		ColumnsForUser:  len(view.Columns),
		TriggerKey:      key,
		PostAggregation: agg,
		PostGroupBy:     aggGroupBy,
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
