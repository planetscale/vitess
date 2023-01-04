package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type vindexRouting struct {
	// here we store the possible vindexes we can use so that when we add predicates to the plan,
	// we can quickly check if the new predicates enables any new vindex Options
	VindexPreds []*VindexPlusPredicates

	// the best option available is stored here
	Selected *VindexOption

	RouteOpCode engine.Opcode

	// SeenPredicates contains all the predicates that have had a chance to influence routing.
	// If we need to replan routing, we'll use this list
	SeenPredicates []sqlparser.Expr
}

func (r *vindexRouting) OpCode() engine.Opcode {
	return r.RouteOpCode
}

func (r *vindexRouting) Clone() routing {
	kopy := *r
	kopy.VindexPreds = make([]*VindexPlusPredicates, len(r.VindexPreds))
	for i, pred := range r.VindexPreds {
		//we do this to create a copy of the struct
		p := *pred
		kopy.VindexPreds[i] = &p
	}
	return &kopy
}

func (r *vindexRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error {
	if r.RouteOpCode != engine.None {
		newVindexFound, err := r.searchForNewVindexes(ctx, expr)
		if err != nil {
			return err
		}

		// if we didn't open up any new vindex Options, no need to enter here
		if newVindexFound {
			r.PickBestAvailableVindex()
		}
	}
	return nil
}

func (r *vindexRouting) AddQueryTablePredicates(ctx *plancontext.PlanningContext, qt *QueryTable) error {
	for _, predicate := range qt.Predicates {
		err := r.UpdateRoutingLogic(ctx, predicate)
		if err != nil {
			return err
		}
	}

	if r.RouteOpCode == engine.Scatter && len(qt.Predicates) > 0 {
		// If we have a scatter query, it's worth spending a little extra time seeing if we can't improve it
		oldPredicates := qt.Predicates
		qt.Predicates = nil
		plan.SeenPredicates = nil
		for _, pred := range oldPredicates {
			rewritten := sqlparser.RewritePredicate(pred)
			predicates := sqlparser.SplitAndExpression(nil, rewritten.(sqlparser.Expr))
			for _, predicate := range predicates {
				qt.Predicates = append(qt.Predicates, predicate)
				err := r.UpdateRoutingLogic(ctx, predicate)
				if err != nil {
					return nil, err
				}
			}
		}

		if plan.RouteOpCode == engine.Scatter {
			// if we _still_ haven't found a better route, we can run this additional rewrite on any ORs we have
			for _, expr := range queryTable.Predicates {
				or, ok := expr.(*sqlparser.OrExpr)
				if !ok {
					continue
				}
				for _, predicate := range sqlparser.ExtractINFromOR(or) {
					err := plan.UpdateRoutingLogic(ctx, predicate)
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}

}

// PickBestAvailableVindex goes over the available vindexes for this route and picks the best one available.
func (r *vindexRouting) PickBestAvailableVindex() engine.Opcode {
	for _, v := range r.VindexPreds {
		option := v.bestOption()
		if option != nil && (r.Selected == nil || less(option.Cost, r.Selected.Cost)) {
			r.Selected = option
			return option.OpCode
		}
	}
	return engine.Scatter
}

func (r *vindexRouting) searchForNewVindexes(ctx *plancontext.PlanningContext, predicate sqlparser.Expr) (bool, error) {
	newVindexFound := false
	switch node := predicate.(type) {
	case *sqlparser.ExtractedSubquery:
		originalCmp, ok := node.Original.(*sqlparser.ComparisonExpr)
		if !ok {
			break
		}

		// using the node.subquery which is the rewritten version of our subquery
		cmp := &sqlparser.ComparisonExpr{
			Left:     node.OtherSide,
			Right:    &sqlparser.Subquery{Select: node.Subquery.Select},
			Operator: originalCmp.Operator,
		}
		found, exitEarly, err := r.planComparison(ctx, cmp)
		if err != nil || exitEarly {
			return false, err
		}
		newVindexFound = newVindexFound || found

	case *sqlparser.ComparisonExpr:
		found, exitEarly, err := r.planComparison(ctx, node)
		if err != nil || exitEarly {
			return false, err
		}
		newVindexFound = newVindexFound || found
	case *sqlparser.IsExpr:
		found := r.planIsExpr(ctx, node)
		newVindexFound = newVindexFound || found
	}
	return newVindexFound, nil
}

func (r *vindexRouting) planComparison(ctx *plancontext.PlanningContext, cmp *sqlparser.ComparisonExpr) (found bool, exitEarly bool, err error) {
	if cmp.Operator != sqlparser.NullSafeEqualOp && (sqlparser.IsNull(cmp.Left) || sqlparser.IsNull(cmp.Right)) {
		// we are looking at ANDed predicates in the WHERE clause.
		// since we know that nothing returns true when compared to NULL,
		// so we can safely bail out here
		r.setSelectNoneOpcode()
		return false, true, nil
	}

	switch cmp.Operator {
	case sqlparser.EqualOp:
		found := r.planEqualOp(ctx, cmp)
		return found, false, nil
	case sqlparser.InOp:
		if r.isImpossibleIN(cmp) {
			return false, true, nil
		}
		found := r.planInOp(ctx, cmp)
		return found, false, nil
	case sqlparser.NotInOp:
		// NOT IN is always a scatter, except when we can be sure it would return nothing
		if r.isImpossibleNotIN(cmp) {
			return false, true, nil
		}
	case sqlparser.LikeOp:
		found := r.planLikeOp(ctx, cmp)
		return found, false, nil

	}
	return false, false, nil
}

func (r *vindexRouting) setSelectNoneOpcode() {
	r.RouteOpCode = engine.None
	// clear any chosen vindex as this query does not need to be sent down.
	r.Selected = nil
}

func (r *vindexRouting) haveMatchingVindex(
	ctx *plancontext.PlanningContext,
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value evalengine.Expr,
	opcode func(*vindexes.ColumnVindex) engine.Opcode,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) bool {
	newVindexFound := false
	for _, v := range r.VindexPreds {
		// check that the
		if !ctx.SemTable.DirectDeps(column).IsSolvedBy(v.TableID) {
			continue
		}
		switch v.ColVindex.Vindex.(type) {
		case vindexes.SingleColumn:
			col := v.ColVindex.Columns[0]
			if column.Name.Equal(col) {
				// single column vindex - just add the option
				routeOpcode := opcode(v.ColVindex)
				vindex := vfunc(v.ColVindex)
				if vindex == nil || routeOpcode == engine.Scatter {
					continue
				}
				v.Options = append(v.Options, &VindexOption{
					Values:      []evalengine.Expr{value},
					ValueExprs:  []sqlparser.Expr{valueExpr},
					Predicates:  []sqlparser.Expr{node},
					OpCode:      routeOpcode,
					FoundVindex: vindex,
					Cost:        costFor(v.ColVindex, routeOpcode),
					Ready:       true,
				})
				newVindexFound = true
			}
		case vindexes.MultiColumn:
			colLoweredName := ""
			indexOfCol := -1
			for idx, col := range v.ColVindex.Columns {
				if column.Name.Equal(col) {
					colLoweredName = column.Name.Lowered()
					indexOfCol = idx
					break
				}
			}
			if colLoweredName == "" {
				break
			}

			var newOption []*VindexOption
			for _, op := range v.Options {
				if op.Ready {
					continue
				}
				_, isPresent := op.ColsSeen[colLoweredName]
				if isPresent {
					continue
				}
				option := copyOption(op)
				optionReady := option.updateWithNewColumn(colLoweredName, valueExpr, indexOfCol, value, node, v.ColVindex, opcode)
				if optionReady {
					newVindexFound = true
				}
				newOption = append(newOption, option)
			}
			v.Options = append(v.Options, newOption...)

			// multi column vindex - just always add as new option
			option := createOption(v.ColVindex, vfunc)
			optionReady := option.updateWithNewColumn(colLoweredName, valueExpr, indexOfCol, value, node, v.ColVindex, opcode)
			if optionReady {
				newVindexFound = true
			}
			v.Options = append(v.Options, option)
		}
	}
	return newVindexFound
}

func createOption(
	colVindex *vindexes.ColumnVindex,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) *VindexOption {
	values := make([]evalengine.Expr, len(colVindex.Columns))
	predicates := make([]sqlparser.Expr, len(colVindex.Columns))
	vindex := vfunc(colVindex)

	return &VindexOption{
		Values:      values,
		Predicates:  predicates,
		ColsSeen:    map[string]any{},
		FoundVindex: vindex,
	}
}

func (r *vindexRouting) hasVindex(column *sqlparser.ColName) bool {
	for _, v := range r.VindexPreds {
		for _, col := range v.ColVindex.Columns {
			if column.Name.Equal(col) {
				return true
			}
		}
	}
	return false
}

func (r *vindexRouting) planIsExpr(ctx *plancontext.PlanningContext, node *sqlparser.IsExpr) bool {
	// we only handle IS NULL correct. IsExpr can contain other expressions as well
	if node.Right != sqlparser.IsNullOp {
		return false
	}
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false
	}
	vdValue := &sqlparser.NullVal{}
	val := r.makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}
	opcodeF := func(vindex *vindexes.ColumnVindex) engine.Opcode {
		if _, ok := vindex.Vindex.(vindexes.Lookup); ok {
			return engine.Scatter
		}
		return equalOrEqualUnique(vindex)
	}

	return r.haveMatchingVindex(ctx, node, vdValue, column, val, opcodeF, justTheVindex)
}

func (r *vindexRouting) SelectedVindex() vindexes.Vindex {
	if r.Selected == nil {
		return nil
	}
	return r.Selected.FoundVindex
}

func copyOption(orig *VindexOption) *VindexOption {
	colsSeen := make(map[string]any, len(orig.ColsSeen))
	valueExprs := make([]sqlparser.Expr, len(orig.ValueExprs))
	values := make([]evalengine.Expr, len(orig.Values))
	predicates := make([]sqlparser.Expr, len(orig.Predicates))

	copy(values, orig.Values)
	copy(valueExprs, orig.ValueExprs)
	copy(predicates, orig.Predicates)
	for k, v := range orig.ColsSeen {
		colsSeen[k] = v
	}
	vo := &VindexOption{
		Values:      values,
		ColsSeen:    colsSeen,
		ValueExprs:  valueExprs,
		Predicates:  predicates,
		OpCode:      orig.OpCode,
		FoundVindex: orig.FoundVindex,
		Cost:        orig.Cost,
	}
	return vo
}

func (option *VindexOption) updateWithNewColumn(
	colLoweredName string,
	valueExpr sqlparser.Expr,
	indexOfCol int,
	value evalengine.Expr,
	node sqlparser.Expr,
	colVindex *vindexes.ColumnVindex,
	opcode func(*vindexes.ColumnVindex) engine.Opcode,
) bool {
	option.ColsSeen[colLoweredName] = true
	option.ValueExprs = append(option.ValueExprs, valueExpr)
	option.Values[indexOfCol] = value
	option.Predicates[indexOfCol] = node
	option.Ready = len(option.ColsSeen) == len(colVindex.Columns)
	routeOpcode := opcode(colVindex)
	if option.OpCode < routeOpcode {
		option.OpCode = routeOpcode
		option.Cost = costFor(colVindex, routeOpcode)
	}
	return option.Ready
}
