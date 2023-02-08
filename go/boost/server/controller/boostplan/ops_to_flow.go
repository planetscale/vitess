package boostplan

import (
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/common/dbg"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type Migration interface {
	AddTable(name string, fields []string, b *flownode.Table) graph.NodeIdx
	AddIngredient(name string, fields []string, impl flownode.NodeImpl) (graph.NodeIdx, error)
	AddView(name string, na graph.NodeIdx, connect func(g *graph.Graph[*flownode.Node], reader *flownode.Reader))
	AddUpquery(idx graph.NodeIdx, statement sqlparser.SelectStatement)
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
		if node.Upquery != nil {
			mig.AddUpquery(node.Flow.Address, node.Upquery)
		}
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
		fn, err = makeDistinctNode(mig, node)
	case *operators.NullFilter:
		fn, err = makeNullFilterNode(mig, node, op, node.Ancestors[0])
	default:
		panic(fmt.Sprintf("opNodeToFlowParts doesn't know '%T", node.Op))
	}

	if err != nil {
		return fn, err
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

	if node.Upquery != nil {
		mig.AddUpquery(node.Flow.Address, node.Upquery)
	}
	return fn, nil
}

func makeUnionNode(mig Migration, node *operators.Node, op *operators.Union) (operators.FlowNode, error) {
	colNames := columnOpNames(op.Columns)
	emitColumnID := make(map[graph.NodeIdx][]int)
	for i, ancestor := range node.Ancestors {
		emitColumnID[ancestor.Flow.Address] = op.ColumnsIdx[i]
	}
	ingr, err := mig.AddIngredient(node.Name, colNames, flownode.NewUnion(emitColumnID))
	if err != nil {
		return operators.FlowNode{}, err
	}
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: ingr,
	}, nil
}

func makeTopKNode(mig Migration, node *operators.Node, op *operators.TopK) (operators.FlowNode, error) {
	colnames := columnOpNames(node.Columns)
	parentNa := node.Ancestors[0].FlowNodeAddr()

	var order []flownode.OrderedColumn
	for i, o := range op.Order {
		order = append(order, flownode.OrderedColumn{
			Col:  op.OrderOffsets[i],
			Desc: o.Direction == sqlparser.DescOrder,
		})
	}

	ingr, err := mig.AddIngredient(node.Name, colnames, flownode.NewTopK(parentNa, order, op.ParamOffsets, op.K))
	if err != nil {
		return operators.FlowNode{}, err
	}
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: ingr,
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
	na := parent.FlowNodeAddr()

	var parameters = make([]flownode.ViewParameter, 0, len(op.Parameters))
	for _, param := range op.Parameters {
		parameters = append(parameters, flownode.ViewParameter{
			Name:  param.Name,
			Multi: param.Op == sqlparser.InOp,
		})
	}

	mig.AddView(node.Name, na, func(g *graph.Graph[*flownode.Node], reader *flownode.Reader) {
		reader.SetPublicID(op.PublicID)

		if len(op.ParametersIdx) > 0 {
			reader.OnConnected(g, op.ParametersIdx, parameters, len(op.Columns))
		} else {
			reader.OnConnected(g, []int{0}, nil, len(op.Columns))
		}
	})

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
		aggrExprs  []flownode.Aggregation
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
			kind = flownode.AggregationCount
			aggregateOver = -1
		case *sqlparser.Min:
			kind = flownode.ExtremumMin
		case *sqlparser.Max:
			kind = flownode.ExtremumMax
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
				aggrExprs = append(aggrExprs, flownode.Aggregation{Kind: flownode.AggregationSum, Over: aggregateOver})
				colnames = append(colnames, "bogo_aggr_sum")
			}
			if countOffset < 0 {
				countOffset = len(aggrExprs)
				aggrExprs = append(aggrExprs, flownode.Aggregation{Kind: flownode.AggregationCount, Over: aggregateOver})
				colnames = append(colnames, "bogo_aggr_count")
			}

			// AVG(x) = SUM(x) / COUNT(x)
			avgExpr := &sqlparser.BinaryExpr{
				Operator: sqlparser.DivOp,
				Left:     &sqlparser.Offset{V: groupCount + sumOffset},
				Right:    &sqlparser.Offset{V: groupCount + countOffset},
			}
			evalExpr, err := evalengine.Translate(avgExpr, nil)

			// the evalengine should never fail to translate this
			dbg.Assert(err == nil, "evalengine failed to translate static expression: %w", err)

			projExprs = append(projExprs, &flownode.ProjectedExpr{AST: avgExpr, EvalExpr: evalExpr})
			continue
		case sqlparser.AggrFunc:
			return operators.FlowNode{}, &operators.UnsupportedError{AST: aggr, Type: operators.Aggregation}
		default:
			return operators.FlowNode{}, &operators.UnsupportedError{AST: aggr, Type: operators.NoFullGroupBy}
		}

		aggrExprs = append(aggrExprs, flownode.Aggregation{Kind: kind, Over: aggregateOver})
		colnames = append(colnames, node.Columns[groupCount+i].Name)
	}

	parentNa := node.Ancestors[0].FlowNodeAddr()
	grouped := flownode.NewGrouped(parentNa, op.ScalarAggregation, op.GroupingIdx, aggrExprs)
	ingr, err := mig.AddIngredient(node.Name, colnames, grouped)
	if err != nil {
		return operators.FlowNode{}, err
	}
	groupingFlowNode := operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: ingr,
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
		ingr, err := mig.AddIngredient(node.Name+"_extra_proj", columnOpNames(node.Columns), proj)
		if err != nil {
			return operators.FlowNode{}, err
		}
		return operators.FlowNode{
			Age:     operators.FlowNodeNew,
			Address: ingr,
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
	parentNa := node.Ancestors[0].FlowNodeAddr()
	colnames := columnOpNames(node.Columns)
	conditions := make([]flownode.FilterConditionTuple, 0, len(op.EvalExpr))

	err := checkParametersInExpression(op.Predicates)
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
	ingredient, err := mig.AddIngredient(node.Name, colnames, filter)
	if err != nil {
		return operators.FlowNode{}, err
	}
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: ingredient,
	}, nil
}

func makeJoinOpNode(mig Migration, node *operators.Node, op *operators.Join) (operators.FlowNode, error) {
	leftNa := node.Ancestors[0].FlowNodeAddr()
	rightNa := node.Ancestors[1].FlowNodeAddr()

	var joinKind flownode.JoinKind
	if op.Inner {
		joinKind = flownode.JoinTypeInner
	} else {
		joinKind = flownode.JoinTypeOuter
	}
	ingr, err := mig.AddIngredient(node.Name, columnOpNames(node.Columns), flownode.NewJoin(leftNa, rightNa, joinKind, op.On, op.Emit))
	if err != nil {
		return operators.FlowNode{}, err
	}
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: ingr,
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
	parentNa := parent.FlowNodeAddr()
	projections, err := makeProjections(op.Projections)
	if err != nil {
		return operators.FlowNode{}, err
	}

	colNames := columnOpNames(op.Columns)

	ingr, err := mig.AddIngredient(node.Name, colNames, flownode.NewProject(parentNa, projections))
	if err != nil {
		return operators.FlowNode{}, err
	}
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: ingr,
	}, nil
}

func makeNullFilterNode(mig Migration, node *operators.Node, op *operators.NullFilter, parent *operators.Node) (operators.FlowNode, error) {
	parentNa := parent.FlowNodeAddr()
	projections, err := makeProjections(op.Projections)
	if err != nil {
		return operators.FlowNode{}, err
	}

	colNames := columnOpNames(node.Columns)

	ingr, err := mig.AddIngredient(node.Name, colNames, flownode.NewProject(parentNa, projections))
	if err != nil {
		return operators.FlowNode{}, err
	}
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: ingr,
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
	var columnTypes []sql.Type

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

		ctype := sql.Type{
			T:         sqlType,
			Collation: collationID,
			Nullable:  nullable,
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

	table := flownode.NewTable(op.Keyspace, op.TableName, keyColumnIds, columnTypes)
	address := mig.AddTable(node.Name, columnNames, table)
	return operators.FlowNode{Age: operators.FlowNodeNew, Address: address}, nil
}

func makeDistinctNode(mig Migration, node *operators.Node) (operators.FlowNode, error) {
	src := node.Ancestors[0].Flow.Address

	var groupBy []int
	for i := range node.Columns {
		groupBy = append(groupBy, i)
	}

	distinct := flownode.NewDistinct(src, groupBy)
	ingr, err := mig.AddIngredient(node.Name, columnOpNames(node.Columns), distinct)
	if err != nil {
		return operators.FlowNode{}, err
	}
	flowNode := operators.FlowNode{
		Address: ingr,
		Age:     operators.FlowNodeNew,
	}
	return flowNode, nil
}
