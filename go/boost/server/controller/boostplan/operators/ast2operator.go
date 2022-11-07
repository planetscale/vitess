package operators

import (
	"fmt"
	"reflect"

	"go.uber.org/multierr"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func (conv *Converter) toOperator(ctx *PlanContext, stmt sqlparser.SelectStatement, name string) (*Node, error) {
	node, params, columns, err := conv.selectStmtToOperator(ctx, stmt)
	if err != nil {
		return nil, err
	}

	if len(params) == 0 {
		params = []Parameter{{
			name: "bogokey",
			key:  newBogoKeyColumn(),
		}}
	}

	pushDownParameter(ctx.SemTable, node, params)

	view := &View{
		Parameters: params,
		Columns:    columns,
	}

	viewNode := conv.NewNode(name, view, []*Node{node})

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

	if sel.Limit != nil {
		env := evalengine.EmptyExpressionEnv()
		expr, err := evalengine.Translate(sel.Limit.Rowcount, nil)
		if err != nil {
			return nil, nil, nil, err
		}
		value, err := env.Evaluate(expr)
		if err != nil {
			return nil, nil, nil, err
		}

		k, err := value.Value().ToUint64()
		if err != nil {
			return nil, nil, nil, err
		}
		node = conv.NewNode("topK", &TopK{
			Order: sel.OrderBy,
			K:     uint(k),
		}, []*Node{node})
	}

	return
}

func checkForUnsupported(sel *sqlparser.Select) error {
	var errors []error
	ordered := len(sel.OrderBy) != 0
	limited := sel.Limit != nil

	switch {
	case ordered && !limited:
		errors = append(errors, &UnsupportedError{
			AST:  sel.OrderBy,
			Type: OrderByNoLimit,
		})
	case limited && !ordered:
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

	sqlparser.Rewrite(sel, func(cursor *sqlparser.Cursor) bool {
		switch cursor.Node().(type) {
		case sqlparser.Argument:
			switch op := cursor.Parent().(type) {
			case *sqlparser.ComparisonExpr:
				if op.Operator != sqlparser.EqualOp {
					// TODO: We also want to extend this to support IN()
					errors = append(errors, &UnsupportedError{
						AST:  cursor.Parent(),
						Type: ParameterNotEqual,
					})
				}
			default:
				errors = append(errors, &UnsupportedError{
					AST:  cursor.Parent(),
					Type: ParameterLocation,
				})
			}
		}
		return true
	}, nil)

	return multierr.Combine(errors...)
}

func markOther(st *semantics.SemTable, col sqlparser.AggrFunc, tableID semantics.TableSet) func(cursor *sqlparser.Cursor) bool {
	return func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case sqlparser.AggrFunc:
			if sqlparser.EqualsAggrFunc(col, node) {
				st.Direct[node] = tableID
			}
		case *sqlparser.Subquery:
			return false
		}
		return true
	}
}

func (conv *Converter) buildAggregation(ctx *PlanContext, sel *sqlparser.Select, input *Node) (output *Node, columns Columns, err error) {
	groupBy := &GroupBy{
		Grouping: Columns{},
		TableID:  ctx.SemTable.GetNextTableSet(),
	}
	colMap := map[string]sqlparser.Expr{}

	for _, field := range sel.SelectExprs {
		switch field := field.(type) {
		case *sqlparser.StarExpr:
			return nil, nil, &UnsupportedError{AST: field, Type: ColumnsNotExpanded}
		case *sqlparser.AliasedExpr:
			createdColumn := &Column{
				AST:  []sqlparser.Expr{field.Expr},
				Name: field.ColumnName(),
			}

			switch col := field.Expr.(type) {
			case sqlparser.AggrFunc:
				addAggregations(ctx, sel, col, groupBy)
			default:
				if sqlparser.ContainsAggregation(col) {
					return nil, nil, &UnsupportedError{AST: col, Type: AggregationInComplexExpression}
				}
				if !field.As.IsEmpty() {
					colMap[field.As.String()] = field.Expr
				} else if col, isCol := field.Expr.(*sqlparser.ColName); isCol {
					colMap[col.Name.String()] = col
				}
			}
			columns = columns.Add(ctx, createdColumn)
		}
	}
	for _, expr := range sel.GroupBy {
		if col, isCol := expr.(*sqlparser.ColName); isCol && col.Qualifier.IsEmpty() {
			expr = colMap[col.Name.String()]
		}
		createdColumn := ColumnFromAST(expr)
		groupBy.Grouping = groupBy.Grouping.Add(ctx, createdColumn)
	}

	if len(groupBy.Grouping) > 0 && len(groupBy.Aggregations) == 0 {
		return nil, nil, &UnsupportedError{
			AST:  sel.GroupBy,
			Type: GroupByNoAggregation,
		}
	}

	// We add a projection before the aggregation so that if we are aggregating over
	// something more complicated than a column, we can evaluate it first
	projNode := conv.NewNode("projection", &Project{}, []*Node{input})
	groupingNode := conv.NewNode("aggregation", groupBy, []*Node{projNode})

	return groupingNode, columns, nil
}

func addAggregations(ctx *PlanContext, sel *sqlparser.Select, col sqlparser.AggrFunc, groupBy *GroupBy) {
	groupBy.Aggregations = append(groupBy.Aggregations, col)

	// to make it possible to do offset lookups in later phases, we add these aggregation functions as being introduced
	// by this operator. apart from setting the dependency for this particular expression, we also need to search for
	// other uses of this aggregation expression. So, we traverse the ORDER BY and HAVING clauses, the only other places
	// where aggregations can be found
	ctx.SemTable.Direct[col] = groupBy.TableID
	markOtherF := markOther(ctx.SemTable, col, groupBy.TableID)
	sqlparser.Rewrite(sel.OrderBy, markOtherF, nil)
	sqlparser.Rewrite(sel.Having, markOtherF, nil)
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
		col, cok := comp.Left.(*sqlparser.ColName)
		arg, pok := comp.Right.(sqlparser.Argument)
		if !cok || !pok {
			predicates = sqlparser.AndExpressions(predicates, e)
			continue
		}
		params = append(params, Parameter{
			name: string(arg),
			key:  ColumnFromAST(col),
		})
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
		Inner:      join.Join == sqlparser.NormalJoinType,
		Predicates: join.Condition.On,
	}

	joinCols := Columns{}.
		Add(ctx, lhsCols...).
		Add(ctx, rhsCols...)
	return conv.NewNode("join", op, []*Node{lhs, rhs}), append(lhsParams, rhsParams...), joinCols, nil
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
		if tableSpecRequiresNewBase(tableOp.Spec, tblSpec) {
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

	return newTableRef(ctx.SemTable, tableNode, conv.version, tableID)
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
		_, isArg := node.(sqlparser.Argument)
		if isArg {
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

	return conv.NewNode("distinct", &Distinct{}, []*Node{projNode}), parameters, lftCols, nil
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
