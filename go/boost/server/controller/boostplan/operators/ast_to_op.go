package operators

import (
	"fmt"
	"reflect"

	"go.uber.org/multierr"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func (conv *Converter) toOperator(ctx *PlanContext, stmt sqlparser.SelectStatement, publicID string) (*Node, error) {
	node, params, columns, err := conv.selectStmtToOperator(ctx, stmt)
	if err != nil {
		return nil, err
	}

	if len(params) == 0 {
		params = []Parameter{{
			Name: "bogokey",
			key:  newBogoKeyColumn(),
		}}
	}

	pushDownParameter(ctx.SemTable, node, params)

	view := &View{
		PublicID:   publicID,
		Parameters: params,
		Columns:    columns,
	}

	viewNode := conv.NewNode("view", view, []*Node{node})

	return viewNode, nil
}

func (conv *Converter) selectStmtToOperator(ctx *PlanContext, stmt sqlparser.SelectStatement) (
	node *Node,
	params []Parameter,
	columns Columns,
	err error,
) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return conv.selectToOperator(ctx, stmt)
	case *sqlparser.Union:
		return conv.unionToOperator(ctx, stmt)
	default:
		err = &UnsupportedError{AST: stmt, Type: QueryType}
		return
	}
}

func (conv *Converter) addFilterIfNeeded(node *Node, where *sqlparser.Where, params []Parameter) (*Node, []Parameter, error) {
	if where == nil {
		return node, params, nil
	}

	// rewrite all OR chains into IN for further optimization
	// where.Expr = physical.TryRewriteOrToIn(where.Expr)

	if where.Type == sqlparser.WhereClause && sqlparser.ContainsAggregation(where) {
		return nil, nil, &UnsupportedError{
			AST:  where,
			Type: AggregationInWhere,
		}
	}
	predicates, moreParams := splitPredicates(where.Expr)
	if predicates != nil {
		node = conv.buildFilterOp(node, predicates)
	}
	return node, append(params, moreParams...), nil
}

func (conv *Converter) selectToOperator(ctx *PlanContext, sel *sqlparser.Select) (
	node *Node,
	params []Parameter,
	columns Columns,
	err error,
) {
	err = checkForUnsupported(sel)
	if err != nil {
		return nil, nil, nil, err
	}

	node, params, _, err = conv.buildFromOp(ctx, sel.From)
	if err != nil {
		return nil, nil, Columns{}, err
	}
	node, params, err = conv.addFilterIfNeeded(node, sel.Where, params)
	if err != nil {
		return nil, nil, nil, err
	}

	if sqlparser.ContainsAggregation(sel.SelectExprs) || sel.GroupBy != nil {
		node, columns, err = conv.buildAggregation(ctx, sel, node)
	} else {
		columns, err = conv.planProjectionWithoutAggr(ctx, sel)
	}
	if err != nil {
		return nil, nil, nil, err
	}

	node, params, err = conv.addFilterIfNeeded(node, sel.Having, params)
	if err != nil {
		return nil, nil, nil, err
	}

	projection := &Project{
		Columns: columns,
	}
	node = conv.NewNode("projection", projection, []*Node{node})

	if sel.Distinct {
		node = conv.NewNode("distinct", &Distinct{}, []*Node{node})
	}

	if sel.OrderBy != nil {
		k := -1
		env := evalengine.EmptyExpressionEnv()

		if sel.Limit != nil {
			expr, err := evalengine.Translate(sel.Limit.Rowcount, nil)
			if err != nil {
				return nil, nil, nil, err
			}
			value, err := env.Evaluate(expr)
			if err != nil {
				return nil, nil, nil, err
			}

			l, err := value.Value().ToUint64()
			if err != nil {
				return nil, nil, nil, err
			}
			k = int(l)
		}
		node = conv.NewNode("topK", &TopK{
			Order: sel.OrderBy,
			K:     k,
		}, []*Node{node})
	}

	return
}

func checkForUnsupported(sel *sqlparser.Select) error {
	var errors []error
	ordered := len(sel.OrderBy) != 0
	limited := sel.Limit != nil

	if limited && !ordered {
		errors = append(errors, &UnsupportedError{
			AST:  sel.Limit,
			Type: LimitNoOrderBy,
		})
	}

	if limited && sel.Limit.Offset != nil {
		errors = append(errors, &UnsupportedError{
			AST:  sel.Limit.Offset,
			Type: Offset,
		})
	}

	if sel.Into != nil && !reflect.ValueOf(sel.Into).IsNil() {
		errors = append(errors, &UnsupportedError{
			AST:  sel.Into,
			Type: SelectInto,
		})
	}

	if sel.With != nil && !reflect.ValueOf(sel.With).IsNil() {
		errors = append(errors, &UnsupportedError{
			AST:  sel.With,
			Type: SelectWith,
		})
	}

	if sel.Windows != nil && !reflect.ValueOf(sel.Windows).IsNil() {
		errors = append(errors, &UnsupportedError{
			AST:  sel.Windows,
			Type: SelectWindows,
		})
	}

	if sel.Lock != sqlparser.NoLock {
		errors = append(errors, &UnsupportedError{
			AST:  sel,
			Type: Lock,
		})
	}

	var hasListArg bool

	sqlparser.Rewrite(sel, func(cursor *sqlparser.Cursor) bool {
		switch cursor.Node().(type) {
		case sqlparser.Argument:
			parent, ok := cursor.Parent().(*sqlparser.ComparisonExpr)
			if !ok {
				errors = append(errors, &UnsupportedError{
					AST:  cursor.Parent(),
					Type: ParameterLocationCompare,
				})
				break
			}
			if parent.Operator != sqlparser.EqualOp {
				errors = append(errors, &UnsupportedError{
					AST:  cursor.Parent(),
					Type: ParameterNotEqual,
				})
			}
		case sqlparser.ListArg:
			// because of the way `sqlparser` works, a ListArg can only appear on the right-hand side
			// of an `IN` expression, so we don't need to check for anything specific here
			if hasListArg {
				errors = append(errors, &UnsupportedError{
					AST:  cursor.Parent(),
					Type: MultipleIn,
				})
			}
			hasListArg = true
		case *sqlparser.Subquery:
			errors = append(errors, &UnsupportedError{
				AST:  cursor.Parent(),
				Type: SubQuery,
			})
		}
		return true
	}, nil)

	return multierr.Combine(errors...)
}

// markOther returns a sqlparser rewriter that adds aggregation expressions to the semtable
func markOther(st *semantics.SemTable, col sqlparser.Expr, tableID semantics.TableSet) func(cursor *sqlparser.Cursor) bool {
	return func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.Subquery:
			return false
		case sqlparser.Expr:
			if st.EqualsExpr(col, node) {
				st.Direct[node] = tableID
			}
		}
		return true
	}
}

type aggrCol int

const (
	grouping aggrCol = iota
	aggregation
	unknown
)

func (conv *Converter) buildAggregation(ctx *PlanContext, sel *sqlparser.Select, input *Node) (*Node, Columns, error) {
	groupBy := &GroupBy{
		Grouping: Columns{},
		TableID:  ctx.SemTable.GetNextTableSet(),
	}

	columns := Columns{} // these are the columns that we want the whole SELECT query to return

	var ownCols []*Column // these are the columns that the aggregation works with
	var outputTypes []aggrCol

	colMap := map[string]sqlparser.Expr{}

	// step 1. we go over all expressions and mark them as either unknown or aggregation expressions
	for _, field := range sel.SelectExprs {
		switch field := field.(type) {
		case *sqlparser.StarExpr:
			return nil, nil, &UnsupportedError{AST: field, Type: ColumnsNotExpanded}
		case *sqlparser.AliasedExpr:
			col := &Column{
				AST:  []sqlparser.Expr{field.Expr},
				Name: field.ColumnName(),
			}
			ownCols = append(ownCols, col)
			columns = columns.Add(ctx, col)

			switch col := field.Expr.(type) {
			case sqlparser.AggrFunc:
				outputTypes = append(outputTypes, aggregation)
			default:
				if sqlparser.ContainsAggregation(col) {
					return nil, nil, &UnsupportedError{AST: col, Type: AggregationInComplexExpression}
				}
				outputTypes = append(outputTypes, unknown)
				if !field.As.IsEmpty() {
					colMap[field.As.String()] = field.Expr
				} else if col, isCol := field.Expr.(*sqlparser.ColName); isCol {
					colMap[col.Name.String()] = col
				}
			}
		}
	}

	// step 2. now go over the grouping clause. if we can find the expression in the list of outputs,
	// we mark it as a grouping column. if we can't find the column, we add a new one
	for _, expr := range sel.GroupBy {
		if col, isCol := expr.(*sqlparser.ColName); isCol && col.Qualifier.IsEmpty() {
			e, found := colMap[col.Name.String()]
			if found {
				expr = e
			}
		}
		for i, col := range ownCols {
			if col.EqualsAST(ctx.SemTable, expr, true) {
				outputTypes[i] = grouping
			}
		}
		ownCols = append(ownCols, ColumnFromAST(expr))
		outputTypes = append(outputTypes, grouping)
	}

	// step 3. finally, any output columns still marked as unknown are expressions outside
	// the ONLY_FULL_GROUP_BY limitation. we handle these by using a random expression, similarly to what mysql does
	for i, col := range ownCols {
		switch outputTypes[i] {
		case grouping:
			groupBy.Grouping = groupBy.Grouping.Add(ctx, col)
		case aggregation, unknown:
			// here we just add everything to the aggregations list. when we are going over these when building the flowNode,
			// any that do not implement AggrFunc are known to need special handling
			err := groupBy.addAggregations(ctx, sel, col)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	if len(groupBy.Grouping) > 0 && len(groupBy.Aggregations) == 0 {
		return nil, nil, &UnsupportedError{
			AST:  sel.GroupBy,
			Type: GroupByNoAggregation,
		}
	}

	if len(groupBy.Grouping) == 0 {
		groupBy.ScalarAggregation = true
	}

	// We add a projection before the aggregation so that if we are aggregating over
	// something more complicated than a column, we can evaluate it first
	projNode := conv.NewNode("projection", &Project{}, []*Node{input})
	groupingNode := conv.NewNode("aggregation", groupBy, []*Node{projNode})

	return groupingNode, columns, nil
}

func (groupBy *GroupBy) addAggregations(ctx *PlanContext, sel *sqlparser.Select, col *Column) error {
	groupBy.Aggregations = append(groupBy.Aggregations, col)

	// to make it possible to do offset lookups in later phases, we add these aggregation functions as being introduced
	// by this operator. apart from setting the dependency for this particular expression, we also need to search for
	// other uses of this aggregation expression. So, we traverse the ORDER BY and HAVING clauses, the only other places
	// where aggregations can be found
	ast, err := col.SingleAST()
	if err != nil {
		return err
	}
	_, isAggrFunc := ast.(sqlparser.AggrFunc)
	if !isAggrFunc {
		return nil
	}
	ctx.SemTable.Direct[ast] = groupBy.TableID
	markOtherF := markOther(ctx.SemTable, ast, groupBy.TableID)
	sqlparser.Rewrite(sel.OrderBy, markOtherF, nil)
	sqlparser.Rewrite(sel.Having, markOtherF, nil)
	return nil
}

func (conv *Converter) planProjectionWithoutAggr(ctx *PlanContext, sel *sqlparser.Select) (columns Columns, err error) {
	for _, field := range sel.SelectExprs {
		switch field := field.(type) {
		case *sqlparser.StarExpr:
			return nil, &UnsupportedError{AST: field, Type: ColumnsNotExpanded}
		case *sqlparser.AliasedExpr:
			createdColumn := &Column{
				AST:  []sqlparser.Expr{field.Expr},
				Name: field.ColumnName(),
			}

			columns = columns.Add(ctx, createdColumn)
		}
	}

	return
}

func splitPredicates(expr sqlparser.Expr) (predicates sqlparser.Expr, params []Parameter) {
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		comp, ok := e.(*sqlparser.ComparisonExpr)
		if !ok || (comp.Operator != sqlparser.EqualOp && comp.Operator != sqlparser.InOp) {
			// if it's something other than `=` or `IN`, we are not interested
			predicates = sqlparser.AndExpressions(predicates, e)
			continue
		}
		if col, cok := comp.Left.(*sqlparser.ColName); cok {
			switch arg := comp.Right.(type) {
			case sqlparser.Argument:
				params = append(params, Parameter{Name: string(arg), key: ColumnFromAST(col), Op: comp.Operator})
				continue
			case sqlparser.ListArg:
				params = append(params, Parameter{Name: string(arg), key: ColumnFromAST(col), Op: comp.Operator})
				continue
			}
		}
		predicates = sqlparser.AndExpressions(predicates, e)
	}
	return
}

func (conv *Converter) buildFilterOp(input *Node, predicate sqlparser.Expr) *Node {
	return conv.NewNode("filter", &Filter{Predicates: predicate}, []*Node{input})
}

func (conv *Converter) buildFromTableExpr(ctx *PlanContext, tableExpr sqlparser.TableExpr) (node *Node, params []Parameter, columns Columns, err error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch tbl := tableExpr.Expr.(type) {
		case sqlparser.TableName:
			node, err := conv.buildFromTableName(ctx, tableExpr, tbl)
			return node, nil, Columns{}, err
		case *sqlparser.DerivedTable:
			if len(tableExpr.Columns) > 0 {
				err = &UnsupportedError{AST: tableExpr, Type: DerivedTableColumnAlias}
				return
			}
			node, params, columns, err = conv.selectStmtToOperator(ctx, tbl.Select)
			if err != nil {
				return nil, nil, nil, err
			}
			if proj, isProj := node.Op.(*Project); isProj {
				tblID := ctx.SemTable.TableSetFor(tableExpr)
				proj.TableID = &tblID
				proj.Alias = tableExpr.As.String()
				node.Name = "derived_" + node.Name
			}
			return
		default:
			return nil, nil, Columns{}, &UnsupportedError{
				AST:  tbl,
				Type: AliasedTableExpression,
			}
		}
	case *sqlparser.JoinTableExpr:
		return conv.buildFromJoin(ctx, tableExpr)
	}

	return nil, nil, Columns{}, &UnsupportedError{
		AST:  tableExpr,
		Type: TableExpression,
	}
}

func (conv *Converter) buildFromJoin(ctx *PlanContext, join *sqlparser.JoinTableExpr) (node *Node, params []Parameter, columns Columns, err error) {
	if join.Join == sqlparser.RightJoinType {
		join.Join = sqlparser.LeftJoinType
		join.LeftExpr, join.RightExpr = join.RightExpr, join.LeftExpr
	}
	if join.Join != sqlparser.NormalJoinType && join.Join != sqlparser.LeftJoinType {
		return nil, nil, nil, &UnsupportedError{
			AST:  join,
			Type: JoinType,
		}
	} else if len(join.Condition.Using) > 0 {
		return nil, nil, nil, &UnsupportedError{
			AST:  join,
			Type: JoinWithUsing,
		}
	}
	lhs, lhsParams, lhsCols, err := conv.buildFromTableExpr(ctx, join.LeftExpr)
	if err != nil {
		return nil, nil, Columns{}, err
	}
	rhs, rhsParams, rhsCols, err := conv.buildFromTableExpr(ctx, join.RightExpr)
	if err != nil {
		return nil, nil, Columns{}, err
	}

	op := &Join{
		Inner: join.Join == sqlparser.NormalJoinType,
	}

	joinCols := Columns{}.
		Add(ctx, lhsCols...).
		Add(ctx, rhsCols...)
	params = append(lhsParams, rhsParams...)
	if op.Inner {
		// for inner joins, we can handle any predicates on the join condition as normal filtering predicates.
		// we just need at least one equality comparison between the two tables and we can plan a hash join
		filter := &Filter{Predicates: join.Condition.On}
		joinNode := conv.NewNode("join", op, []*Node{lhs, rhs})
		return conv.NewNode("filter", filter, []*Node{joinNode}), params, joinCols, nil
	}

	var nullFilter *NullFilter

	// if we have an outer join, we have to be a bit more careful with the join predicates
	// since we can only do hash joins, we need at least one comparison predicate between the two tables
	// but if we also have other predicates, we can deal with them
	for _, e := range sqlparser.SplitAndExpression(nil, join.Condition.On) {
		deps := ctx.SemTable.DirectDeps(e)
		lhsID := lhs.Covers()
		rhsID := rhs.Covers()
		bothSides := rhsID.Merge(lhsID)
		switch {
		case deps.NumberOfTables() == 0 || !deps.IsSolvedBy(bothSides):
			// this is a predicate the does not need any table input, or needs tables outside these two
			return nil, nil, nil, &UnsupportedError{
				AST:  e,
				Type: JoinPredicates,
			}
		case deps.IsSolvedBy(lhsID):
			// for this situation, we need to not filter out rows,
			// but instead turn the columns coming from the RHS into null
			if nullFilter == nil {
				nullFilter = &NullFilter{Join: op, OuterSide: rhsID}
			}
			nullFilter.Predicates = sqlparser.AndExpressions(nullFilter.Predicates, e)
		case deps.IsSolvedBy(rhsID):
			// push join condition to outer side
			// this is pretty easy - we just make a filter under the outer side
			rhs = conv.NewNode("filter", &Filter{Predicates: e}, []*Node{rhs})
		case deps.IsSolvedBy(bothSides):
			// a predicate that depends on both sides
			op.Predicates = sqlparser.AndExpressions(op.Predicates, e)
		}
	}

	if op.Predicates == nil {
		return nil, nil, nil, &UnsupportedError{
			AST:  join.Condition.On,
			Type: JoinPredicates,
		}
	}

	joinNode := conv.NewNode("join", op, []*Node{lhs, rhs})
	if nullFilter != nil {
		return conv.NewNode("nullFilter", nullFilter, []*Node{joinNode}), params, joinCols, nil
	}

	return joinNode, params, joinCols, nil
}

func externalTableName(keyspace string, name string) string {
	if keyspace == "" {
		panic("missing keyspace")
	}
	return fmt.Sprintf("table[%s.%s]", keyspace, name)
}

func (conv *Converter) buildFromTableName(ctx *PlanContext, tableExpr *sqlparser.AliasedTableExpr, tblName sqlparser.TableName) (*Node, error) {
	var tableNode *Node

	tableID := ctx.SemTable.TableSetFor(tableExpr)
	keyspace := tblName.Qualifier.String()
	name := tblName.Name.String()
	if keyspace == "" && name == "dual" {
		return nil, &UnsupportedError{
			AST:  tblName,
			Type: DualTable,
		}
	}

	if keyspace == "" {
		// TODO: is there a more efficient way to find this table's keyspace?
		ti, err := ctx.SemTable.TableInfoFor(tableID)
		if err != nil {
			return nil, err
		}
		keyspace = ti.GetVindexTable().Keyspace.Name
	}

	tableName := externalTableName(keyspace, name)
	if v, ok := conv.current[tableName]; ok {
		tableNode, ok = conv.nodes[noderef{tableName, v}]
		if !ok {
			panic("current[name] but no match in nodes")
		}
		tableOp := tableNode.Op.(*Table)
		tblSpec, err := ctx.DDL.LoadTableSpec(keyspace, name)
		if err != nil {
			return nil, err
		}
		if tableSpecRequiresNewTableNode(tableOp.Spec, tblSpec) {
			tableNode, err = conv.makeTableNode(keyspace, name, tblSpec)
			if err != nil {
				return nil, err
			}
		}
	} else {
		var err error
		tableNode, err = conv.loadNamedTable(ctx.DDL, keyspace, name)
		if err != nil {
			return nil, err
		}
	}

	return newTableRef(ctx.SemTable, tableNode, conv.version, tableID, tableExpr.Hints)
}

func (conv *Converter) buildFromOp(ctx *PlanContext, from []sqlparser.TableExpr) (last *Node, params []Parameter, columns Columns, err error) {
	for _, tableExpr := range from {
		node, tParams, tCols, err := conv.buildFromTableExpr(ctx, tableExpr)
		if err != nil {
			return nil, nil, Columns{}, err
		}
		if last == nil {
			last = node
			params = tParams
			columns = tCols
		} else {
			op := &Join{
				Predicates: nil,
				Inner:      true,
			}
			last = conv.NewNode("join", op, []*Node{last, node})
			params = append(params, tParams...)
			columns = columns.Add(ctx, tCols...)
		}
	}
	return
}

func (conv *Converter) unionToOperator(
	ctx *PlanContext,
	stmt *sqlparser.Union,
) (*Node, []Parameter, Columns, error) {
	// UNION can be used inside a query that has parameters, but the parameters cannot exist inside the UNION
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case sqlparser.Argument, sqlparser.ListArg:
			return false, &UnsupportedError{
				AST:  node,
				Type: ParametersInsideUnion,
			}
		}
		return true, nil
	}, stmt)
	if err != nil {
		return nil, nil, nil, err
	}

	lft, lftParams, lftCols, err := conv.selectStmtToOperator(ctx, stmt.Left)
	if err != nil {
		return nil, nil, Columns{}, err
	}

	rgt, rgtParams, _, err := conv.selectStmtToOperator(ctx, stmt.Right)
	if err != nil {
		return nil, nil, Columns{}, err
	}

	inputColumns, err := mapExpressionsToInputCols(ctx, stmt)
	if err != nil {
		return nil, nil, Columns{}, err
	}

	unionCols := Columns{}
	for x := range lftCols {
		unionCols = unionCols.Add(ctx, ColumnFromAST(sqlparser.NewOffset(x, nil)))
	}

	unionOp := &Union{
		InputColumns: inputColumns,
		Columns:      lftCols,
	}
	unionNode := conv.NewNode("union", unionOp, []*Node{lft, rgt})
	projNode := conv.NewNode("project", &Project{}, []*Node{unionNode})
	parameters := append(lftParams, rgtParams...)
	if !stmt.Distinct {
		return projNode, parameters, lftCols, nil
	}

	return conv.NewNode("distinct", &Distinct{}, []*Node{projNode}), parameters, unionCols, nil
}

func mapExpressionsToInputCols(ctx *PlanContext, stmt *sqlparser.Union) ([2]Columns, error) {
	inputColumns := [2]Columns{}
	var err error
	inputColumns[0], err = mapExprsToColumns(ctx, sqlparser.GetFirstSelect(stmt.Left).SelectExprs)
	if err != nil {
		return [2]Columns{}, err
	}
	inputColumns[1], err = mapExprsToColumns(ctx, sqlparser.GetFirstSelect(stmt.Right).SelectExprs)
	if err != nil {
		return [2]Columns{}, err
	}
	return inputColumns, nil
}

func mapExprsToColumns(ctx *PlanContext, exprs sqlparser.SelectExprs) (Columns, error) {
	output := Columns{}
	for _, e := range exprs {
		ae, ok := e.(*sqlparser.AliasedExpr)
		if !ok {
			return Columns{}, &UnsupportedError{AST: e, Type: NotAliasedExpression}
		}
		output = output.Add(ctx, ColumnFromAST(ae.Expr))
	}
	return output, nil
}

func ColumnFromAST(expr sqlparser.Expr) *Column {
	name := ""
	if col, ok := expr.(*sqlparser.ColName); ok {
		name = col.Name.String()
	}
	return &Column{
		AST:  []sqlparser.Expr{expr},
		Name: name,
	}
}

func newBogoKeyColumn() *Column {
	return &Column{
		AST:  []sqlparser.Expr{sqlparser.NewIntLiteral("0")},
		Name: "bogokey",
	}
}
