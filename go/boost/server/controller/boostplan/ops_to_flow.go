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
	AddIngredient(name string, fields []string, impl flownode.NodeImpl) graph.NodeIdx
	AddBase(name string, fields []string, b flownode.AnyBase) graph.NodeIdx
	Maintain(name string, na graph.NodeIdx, key []int, parameters []boostpb.ViewParameter, colLen int)
}

func OpQueryToFlowParts(query *operators.Query, tr *operators.TableReport, mig Migration) (*operators.QueryFlowParts, error) {
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

		flowNode, err := opNodeToFlowParts(n, mig)
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
		Name:        query.Name,
		NewNodes:    newNodes,
		ReusedNodes: reusedNodes,
		QueryLeaf:   query.View.Flow.Address,
		TableReport: tr,
	}, nil
}

func opNodeToFlowParts(node *operators.Node, mig Migration) (operators.FlowNode, error) {
	if node.Flow.Valid() {
		return node.Flow, nil
	}

	var fn operators.FlowNode
	var err error

	switch op := node.Op.(type) {
	case *operators.Filter:
		fn, err = makeFilterOpNode(node, mig, op)
	case *operators.Join:
		fn, err = makeJoinOpNode(node, mig, op)
	case *operators.Table:
		fn, err = makeBaseOpNode(node.Name, op, mig)
	case *operators.Project:
		fn, err = makeProjectOpNode(node.Name, node.Ancestors[0], op, mig)
	case *operators.GroupBy:
		fn, err = makeGroupByOpNode(node, mig, op)
	case *operators.View:
		fn, err = makeViewOpNode(node, mig, op)
	case *operators.NodeTableRef:
		fn, err = makeTableRefNode(op, mig)
	case *operators.Union:
		fn, err = makeUnionNode(node, mig, op)
	case *operators.TopK:
		fn, err = makeTopKNode(node, mig, op)
	case *operators.Distinct:
		fn = makeDistinctNode(node, mig)
	case *operators.NullFilter:
		fn, err = makeNullFilterNode(node, node.Ancestors[0], mig, op)
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

func makeUnionNode(node *operators.Node, mig Migration, op *operators.Union) (operators.FlowNode, error) {
	colNames := columnOpNames(op.Columns)
	emitColumnID := make(map[graph.NodeIdx][]int)
	for i, ancestor := range node.Ancestors {
		emitColumnID[ancestor.Flow.Address] = op.ColumnsIdx[i]
	}
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: mig.AddIngredient(node.Name, colNames, flownode.NewUnion(emitColumnID)),
	}, nil
}

func makeTopKNode(node *operators.Node, mig Migration, op *operators.TopK) (operators.FlowNode, error) {
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
		Address: mig.AddIngredient(node.Name, colnames, flownode.NewTopK(parentNa, order, op.ParamOffsets, op.K)),
	}, nil
}

func makeTableRefNode(op *operators.NodeTableRef, mig Migration) (operators.FlowNode, error) {
	if !op.Node.Flow.Valid() {
		// if the referenced table has never been added to the flow graph before, we must do so now
		var err error
		op.Node.Flow, err = makeBaseOpNode(op.Node.Name, op.Node.Op.(*operators.Table), mig)
		if err != nil {
			return operators.FlowNode{}, err
		}
	}
	return operators.FlowNode{
		Age:     operators.FlowNodeExisting,
		Address: op.Node.Flow.Address,
	}, nil
}

func makeViewOpNode(node *operators.Node, mig Migration, op *operators.View) (operators.FlowNode, error) {
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
		mig.Maintain(node.Name, na, op.ParametersIdx, parameters, len(op.Columns))
	} else {
		mig.Maintain(node.Name, na, []int{0}, nil, len(op.Columns))
	}

	switch parent.Flow.Age {
	case operators.FlowNodeNew:
		return operators.FlowNode{Age: operators.FlowNodeExisting, Address: parent.Flow.Address}, nil
	case operators.FlowNodeExisting:
		return parent.Flow, nil
	}
	panic("could not make the view node op")
}

func makeGroupByOpNode(node *operators.Node, mig Migration, op *operators.GroupBy) (operators.FlowNode, error) {
	colnames := columnOpNames(node.Columns)
	aggrExprs := make([]flownode.AggrExpr, 0, len(op.AggregationsTypes))
	for i, aggregationsType := range op.AggregationsTypes {
		aggrExprs = append(aggrExprs, flownode.AggregationOver(aggregationsType, op.AggregationsIdx[i]))
	}

	parentNa, err := node.Ancestors[0].FlowNodeAddr()
	if err != nil {
		return operators.FlowNode{}, err
	}

	grouped := flownode.NewGrouped(parentNa, op.GroupingIdx, aggrExprs)
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: mig.AddIngredient(node.Name, colnames, grouped),
	}, nil
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

func makeFilterOpNode(node *operators.Node, mig Migration, op *operators.Filter) (operators.FlowNode, error) {
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
	ingredient := mig.AddIngredient(node.Name, colnames, filter)
	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: ingredient,
	}, nil
}

func makeJoinOpNode(node *operators.Node, mig Migration, op *operators.Join) (operators.FlowNode, error) {
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
		Address: mig.AddIngredient(node.Name, columnOpNames(node.Columns), flownode.NewJoin(leftNa, rightNa, joinType, op.On, op.Emit)),
	}, nil
}

func makeProjectOpNode(name string, parent *operators.Node, op *operators.Project, mig Migration) (operators.FlowNode, error) {
	parentNa, err := parent.FlowNodeAddr()
	if err != nil {
		return operators.FlowNode{}, err
	}

	for _, proj := range op.Projections {
		expr, ok := proj.(*flownode.ProjectedExpr)
		if !ok {
			continue
		}
		err = checkParametersInExpression(expr.AST)
		if err != nil {
			return operators.FlowNode{}, err
		}
	}

	colNames := columnOpNames(op.Columns)

	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: mig.AddIngredient(name, colNames, flownode.NewProject(parentNa, op.Projections)),
	}, nil
}

func makeNullFilterNode(node, parent *operators.Node, mig Migration, op *operators.NullFilter) (operators.FlowNode, error) {
	parentNa, err := parent.FlowNodeAddr()
	if err != nil {
		return operators.FlowNode{}, err
	}

	colNames := columnOpNames(node.Columns)

	return operators.FlowNode{
		Age:     operators.FlowNodeNew,
		Address: mig.AddIngredient(node.Name, colNames, flownode.NewProject(parentNa, op.Projections)),
	}, nil
}

func columnOpNames(columns []*operators.Column) (names []string) {
	for _, c := range columns {
		names = append(names, c.Name)
	}
	return
}

func makeBaseOpNode(name string, op *operators.Table, mig Migration) (operators.FlowNode, error) {
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

	base := flownode.NewExternalBase(keyColumnIds, columnTypes, op.Keyspace)
	address := mig.AddBase(name, columnNames, base)
	return operators.FlowNode{Age: operators.FlowNodeNew, Address: address}, nil
}

func makeDistinctNode(node *operators.Node, mig Migration) operators.FlowNode {
	src := node.Ancestors[0].Flow.Address

	var groupBy []int
	for i := range node.Columns {
		groupBy = append(groupBy, i)
	}

	distinct := flownode.NewDistinct(src, groupBy)
	flowNode := operators.FlowNode{
		Address: mig.AddIngredient(node.Name, columnOpNames(node.Columns), distinct),
		Age:     operators.FlowNodeNew,
	}
	return flowNode
}
