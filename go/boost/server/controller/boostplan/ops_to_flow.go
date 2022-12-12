package boostplan

import (
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type Migration interface {
	AddIngredient(name string, fields []string, impl flownode.NodeImpl, upquerySQL sqlparser.SelectStatement) graph.NodeIdx
	AddBase(name string, fields []string, b flownode.AnyBase) graph.NodeIdx
	Maintain(name, publicID string, na graph.NodeIdx, key []int, parameters []boostpb.ViewParameter, colLen int)
}

func queryToFlowParts(mig Migration, tr *operators.TableReport, query *operators.Query) (*operators.QueryFlowParts, error) {
	var newNodes []graph.NodeIdx
	var reusedNodes []graph.NodeIdx

	var nodeQueue = make([]*operators.Node, 0, len(query.Roots))
	nodeQueue = append(nodeQueue, query.Roots...)

	var edgeCounts = make(map[string]int)
	for _, n := range nodeQueue {
		edgeCounts[n.VersionedName()] = 0
	}

	for len(nodeQueue) > 0 {
		n := nodeQueue[0]
		nodeQueue = nodeQueue[1:]

		flowNode, err := opNodeToFlowParts(mig, n)
		if err != nil {
			return nil, err
		}
		n.Flow = flowNode
		switch flowNode.Age {
		case operators.FlowNodeNew:
			newNodes = append(newNodes, flowNode.Address)

		case operators.FlowNodeExisting:
			reusedNodes = append(reusedNodes, flowNode.Address)
		}

		for _, child := range n.Children {
			nd := child.VersionedName()

			var inEdges int
			if e, ok := edgeCounts[nd]; ok {
				inEdges = e
			} else {
				inEdges = len(child.Ancestors)
			}

			if inEdges == 1 {
				nodeQueue = append(nodeQueue, child)
			}
			edgeCounts[nd] = inEdges - 1
		}
	}

	return &operators.QueryFlowParts{
		Name:        query.PublicID,
		NewNodes:    newNodes,
		ReusedNodes: reusedNodes,
		QueryLeaf:   query.Leaf(),
		TableReport: tr,
	}, nil
}

func opNodeToFlowParts(mig Migration, node *operators.Node) (operators.FlowNode, error) {
	if node.Flow.Valid() {
		return node.Flow, nil
	}

	var fn operators.FlowNode
	var err error

	switch op := node.Op.(type) {
	case *operators.Filter:
		fn, err = makeFilterOpNode(mig, node, op)
	case *operators.Join:
		fn, err = makeJoinOpNode(mig, node, op)
	case *operators.Table:
		fn, err = makeBaseOpNode(mig, node, op)
	case *operators.Project:
		fn, err = makeProjectOpNode(mig, node, op, node.Ancestors[0])
	case *operators.GroupBy:
		fn, err = makeGroupByOpNode(mig, node, op)
	case *operators.View:
		fn, err = makeViewOpNode(mig, node, op)
	case *operators.NodeTableRef:
		fn, err = makeTableRefNode(mig, op)
	case *operators.Union:
		fn, err = makeUnionNode(mig, node, op)
	case *operators.TopK:
		fn, err = makeTopKNode(mig, node, op)
	case *operators.Distinct:
		fn = makeDistinctNode(mig, node)
	case *operators.NullFilter:
		fn, err = makeNullFilterNode(mig, node, op, node.Ancestors[0])
	default:
		panic(fmt.Sprintf("opNodeToFlowParts doesn't know '%T", node.Op))
	}

	if err != nil {
		return operators.FlowNode{}, err
	}
	// any new flow nodes have been instantiated by now, so we replace them with
	// existing ones, but still return `Node::New` below in order to notify higher
	// layers of the new nodes.
	switch fn.Age {
	case operators.FlowNodeNew:
		node.Flow = operators.FlowNode{Address: fn.Address, Age: operators.FlowNodeExisting}
	case operators.FlowNodeExisting:
		node.Flow = fn
	}
	return fn, nil
}

func makeUnionNode(mig Migration, node *operators.Node, op *operators.Union) (operators.FlowNode, error) {
	colNames := columnOpNames(op.Columns)
	emitColumnID := make(map[graph.NodeIdx][]int)
	for i, ancestor := range node.Ancestors {
		emitColumnID[ancestor.Flow.Address] = op.ColumnsIdx[i]
	}
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: mig.AddIngredient(node.Name, colNames, flownode.NewUnion(emitColumnID), node.Upquery),
	}, nil
}

func makeTopKNode(mig Migration, node *operators.Node, op *operators.TopK) (operators.FlowNode, error) {
	colnames := columnOpNames(node.Columns)
	parentNa, err := node.Ancestors[0].FlowNodeAddr()
	if err != nil {
		return operators.FlowNode{}, err
	}
	var order []boostpb.OrderedColumn
	for i, o := range op.Order {
		order = append(order, boostpb.OrderedColumn{
			Col:  op.OrderOffsets[i],
			Desc: o.Direction == sqlparser.DescOrder,
		})
	}

	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: mig.AddIngredient(node.Name, colnames, flownode.NewTopK(parentNa, order, op.ParamOffsets, op.K), node.Upquery),
	}, nil
}

func makeTableRefNode(mig Migration, op *operators.NodeTableRef) (operators.FlowNode, error) {
	if !op.Node.Flow.Valid() {
		// if the referenced table has never been added to the flow graph before, we must do so now
		var err error
		op.Node.Flow, err = makeBaseOpNode(mig, op.Node, op.Node.Op.(*operators.Table))
		if err != nil {
			return operators.FlowNode{}, err
		}
	}
	return operators.FlowNode{
		Age:     operators.FlowNodeExisting,
		Address: op.Node.Flow.Address,
	}, nil
}

func makeViewOpNode(mig Migration, node *operators.Node, op *operators.View) (operators.FlowNode, error) {
	parent := node.Ancestors[0]
	na, err := parent.FlowNodeAddr()
	if err != nil {
		return operators.FlowNode{}, err
	}

	var parameters = make([]boostpb.ViewParameter, 0, len(op.Parameters))
	for _, param := range op.Parameters {
		parameters = append(parameters, boostpb.ViewParameter{
			Name:  param.Name,
			Multi: param.Op == sqlparser.InOp,
		})
	}

	if len(op.ParametersIdx) > 0 {
		mig.Maintain(node.Name, op.PublicID, na, op.ParametersIdx, parameters, len(op.Columns))
	} else {
		mig.Maintain(node.Name, op.PublicID, na, []int{0}, nil, len(op.Columns))
	}

	switch parent.Flow.Age {
	case operators.FlowNodeNew:
		return operators.FlowNode{Age: operators.FlowNodeExisting, Address: parent.Flow.Address}, nil
	case operators.FlowNodeExisting:
		return parent.Flow, nil
	}
	panic("could not make the view node op")
}

func makeGroupByOpNode(mig Migration, node *operators.Node, op *operators.GroupBy) (operators.FlowNode, error) {
	var (
		colnames   []string
		aggrExprs  []flownode.AggrExpr
		projExprs  []flownode.Projection
		groupCount = len(op.GroupingIdx)
	)

	for i := 0; i < groupCount; i++ {
		colnames = append(colnames, node.Columns[i].Name)
	}
	for i, col := range op.Aggregations {
		var kind flownode.AggregationKind
		aggr, err := col.SingleAST()
		if err != nil {
			return operators.FlowNode{}, err
		}
		aggregateOver := op.AggregationsIdx[i]
		switch aggr.(type) {
		case *sqlparser.Sum:
			kind = flownode.AggregationSum
		case *sqlparser.Count:
			kind = flownode.AggregationCount
		case *sqlparser.CountStar:
			kind = flownode.AggregationCountStar
		case *sqlparser.Min:
			return operators.FlowNode{}, &operators.UnsupportedError{AST: aggr, Type: operators.NoExtremum}
		case *sqlparser.Max:
			return operators.FlowNode{}, &operators.UnsupportedError{AST: aggr, Type: operators.NoExtremum}
		case *sqlparser.Avg:
			var sumOffset = -1
			var countOffset = -1
			for idx, col := range op.Aggregations {
				aggregatingOverSameColumn := op.AggregationsIdx[idx] == aggregateOver
				if aggregatingOverSameColumn {
					ast, err := col.SingleAST()
					if err != nil {
						return operators.FlowNode{}, err
					}
					switch ast.(type) {
					case *sqlparser.Sum:
						sumOffset = idx
					case *sqlparser.Count:
						countOffset = idx
					}
				}
			}
			if sumOffset < 0 {
				sumOffset = len(aggrExprs)
				aggrExprs = append(aggrExprs, flownode.AggregationOver(flownode.AggregationSum, aggregateOver))
				colnames = append(colnames, "bogo_aggr_sum")
			}
			if countOffset < 0 {
				countOffset = len(aggrExprs)
				aggrExprs = append(aggrExprs, flownode.AggregationOver(flownode.AggregationCount, aggregateOver))
				colnames = append(colnames, "bogo_aggr_count")
			}

			// AVG(x) = SUM(x) / COUNT(x)
			avgExpr := &sqlparser.BinaryExpr{
				Operator: sqlparser.DivOp,
				Left:     &sqlparser.Offset{V: groupCount + sumOffset},
				Right:    &sqlparser.Offset{V: groupCount + countOffset},
			}
			evalExpr, err := evalengine.Translate(avgExpr, nil)
			if err != nil {
				// the evalengine should never fail to translate this
				return operators.FlowNode{}, operators.NewBug(err.Error())
			}
			projExprs = append(projExprs, &flownode.ProjectedExpr{AST: avgExpr, EvalExpr: evalExpr})
			continue
		case sqlparser.AggrFunc:
			return operators.FlowNode{}, &operators.UnsupportedError{AST: aggr, Type: operators.Aggregation}
		default:
			return operators.FlowNode{}, &operators.UnsupportedError{AST: aggr, Type: operators.NoFullGroupBy}
		}

		over := flownode.AggregationOver(kind, aggregateOver)
		aggrExprs = append(aggrExprs, over)
		colnames = append(colnames, node.Columns[groupCount+i].Name)
	}

	parentNa, err := node.Ancestors[0].FlowNodeAddr()
	if err != nil {
		return operators.FlowNode{}, err
	}

	grouped := flownode.NewGrouped(parentNa, op.ScalarAggregation, op.GroupingIdx, aggrExprs)
	groupingFlowNode := operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: mig.AddIngredient(node.Name, colnames, grouped, node.Upquery),
	}

	if len(projExprs) > 0 {
		var projections []flownode.Projection
		for i := range op.GroupingIdx {
			projections = append(projections, flownode.ProjectedCol(i))
		}
		for i, col := range op.Aggregations {
			aggregation, err := col.SingleAST()
			if err != nil {
				return operators.FlowNode{}, err
			}
			switch aggregation.(type) {
			case *sqlparser.Avg:
				projections = append(projections, projExprs[0])
				projExprs = projExprs[1:]
			default:
				projections = append(projections, flownode.ProjectedCol(groupCount+i))
			}
		}

		proj := flownode.NewProject(groupingFlowNode.Address, projections)
		return operators.FlowNode{
			Age:     operators.FlowNodeNew,
			Address: mig.AddIngredient(node.Name+"_extra_proj", columnOpNames(node.Columns), proj, nil),
		}, nil
	}

	return groupingFlowNode, nil
}

func checkParametersInExpression(ast sqlparser.SQLNode) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case sqlparser.Argument, sqlparser.ListArg:
			return false, &operators.UnsupportedError{
				AST:  ast,
				Type: operators.ParameterLocation,
			}
		}
		return true, nil
	}, ast)
}

func makeFilterOpNode(mig Migration, node *operators.Node, op *operators.Filter) (operators.FlowNode, error) {
	parentNa, err := node.Ancestors[0].FlowNodeAddr()
	if err != nil {
		return operators.FlowNode{}, err
	}
	colnames := columnOpNames(node.Columns)
	conditions := make([]flownode.FilterConditionTuple, 0, len(op.EvalExpr))

	err = checkParametersInExpression(op.Predicates)
	if err != nil {
		return operators.FlowNode{}, err
	}

	for i, expr := range op.EvalExpr {
		conditions = append(conditions, flownode.FilterConditionTuple{
			Cond: expr,
			Expr: op.ExprStr[i],
		})
	}
	filter := flownode.NewFilter(parentNa, conditions)
	ingredient := mig.AddIngredient(node.Name, colnames, filter, node.Upquery)
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: ingredient,
	}, nil
}

func makeJoinOpNode(mig Migration, node *operators.Node, op *operators.Join) (operators.FlowNode, error) {
	leftNa, err := node.Ancestors[0].FlowNodeAddr()
	if err != nil {
		return operators.FlowNode{}, err
	}
	rightNa, err := node.Ancestors[1].FlowNodeAddr()
	if err != nil {
		return operators.FlowNode{}, err
	}
	joinType := flownode.ParserJoinTypeToFlowNodeJoinType(op.Inner)
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: mig.AddIngredient(node.Name, columnOpNames(node.Columns), flownode.NewJoin(leftNa, rightNa, joinType, op.On, op.Emit), node.Upquery),
	}, nil
}

func makeProjections(opProjections []operators.Projection) ([]flownode.Projection, error) {
	projections := make([]flownode.Projection, 0, len(opProjections))
	for _, proj := range opProjections {
		switch proj.Kind {
		case operators.ProjectionColumn:
			projections = append(projections, flownode.ProjectedCol(proj.Column))

		case operators.ProjectionLiteral:
			lit, err := flownode.ProjectedLiteralFromAST(proj.AST.(*sqlparser.Literal))
			if err != nil {
				return nil, err
			}
			projections = append(projections, lit)

		case operators.ProjectionEval:
			if err := checkParametersInExpression(proj.AST); err != nil {
				return nil, err
			}
			projections = append(projections, &flownode.ProjectedExpr{AST: proj.AST, EvalExpr: proj.Eval})
		}
	}
	return projections, nil
}

func makeProjectOpNode(mig Migration, node *operators.Node, op *operators.Project, parent *operators.Node) (operators.FlowNode, error) {
	parentNa, err := parent.FlowNodeAddr()
	if err != nil {
		return operators.FlowNode{}, err
	}

	projections, err := makeProjections(op.Projections)
	if err != nil {
		return operators.FlowNode{}, err
	}

	colNames := columnOpNames(op.Columns)

	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: mig.AddIngredient(node.Name, colNames, flownode.NewProject(parentNa, projections), node.Upquery),
	}, nil
}

func makeNullFilterNode(mig Migration, node *operators.Node, op *operators.NullFilter, parent *operators.Node) (operators.FlowNode, error) {
	parentNa, err := parent.FlowNodeAddr()
	if err != nil {
		return operators.FlowNode{}, err
	}

	projections, err := makeProjections(op.Projections)
	if err != nil {
		return operators.FlowNode{}, err
	}

	colNames := columnOpNames(node.Columns)

	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: mig.AddIngredient(node.Name, colNames, flownode.NewProject(parentNa, projections), node.Upquery),
	}, nil
}

func columnOpNames(columns []*operators.Column) (names []string) {
	for _, c := range columns {
		names = append(names, c.Name)
	}
	return
}

func makeBaseOpNode(mig Migration, node *operators.Node, op *operators.Table) (operators.FlowNode, error) {
	var columnNames []string
	var columnTypes []boostpb.Type

	for i, columnSpec := range op.ColumnSpecs {
		// set the base column IDs
		columnID := i
		columnSpec.ColumnID = &columnID

		// extract column names
		columnNames = append(columnNames, columnSpec.Column.Name.Lowered())
		sqlType := columnSpec.Column.Type.SQLType()

		collationID, err := collationForColumn(op.Spec, columnSpec.Column)
		if err != nil {
			return operators.FlowNode{}, err
		}
		if collationID == collations.Unknown {
			panic("invalid collation")
		}

		nullable := columnSpec.Column.Type.Options.Null == nil || *columnSpec.Column.Type.Options.Null

		ctype := boostpb.Type{
			T:         sqlType,
			Collation: collationID,
			Nullable:  nullable,
		}

		if cso := columnSpec.Column.Type.Options; cso != nil {
			switch lit := cso.Default.(type) {
			case *sqlparser.Literal:
				def, err := evalengine.LiteralToValue(lit)
				if err != nil {
					panic(err)
				}
				ctype.Default = boostpb.ValueFromVitess(def)
			case *sqlparser.NullVal:
				ctype.Default = boostpb.ValueFromVitess(sqltypes.NULL)
			}
		}

		columnTypes = append(columnTypes, ctype)
	}

	var keyColumnIds []int
	for _, k := range op.Keys {
		id := slices.IndexFunc(op.ColumnSpecs, func(cs operators.ColumnSpec) bool {
			return cs.Column.Name.EqualString(k.Name)
		})
		keyColumnIds = append(keyColumnIds, id)
	}

	base := flownode.NewExternalBase(keyColumnIds, columnTypes, op.Keyspace, op.TableName)
	address := mig.AddBase(node.Name, columnNames, base)
	return operators.FlowNode{Age: operators.FlowNodeNew, Address: address}, nil
}

func makeDistinctNode(mig Migration, node *operators.Node) operators.FlowNode {
	src := node.Ancestors[0].Flow.Address

	var groupBy []int
	for i := range node.Columns {
		groupBy = append(groupBy, i)
	}

	distinct := flownode.NewDistinct(src, groupBy)
	flowNode := operators.FlowNode{
		Address: mig.AddIngredient(node.Name, columnOpNames(node.Columns), distinct, node.Upquery),
		Age:     operators.FlowNodeNew,
	}
	return flowNode
}
