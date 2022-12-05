package boostplan

import (
	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	SemanticNode struct {
		node     *operators.Node
		semTable *semantics.SemTable
	}
	Upqueries struct {
		inc   *Incorporator
		index map[graph.NodeIdx]SemanticNode
	}
)

func (up *Upqueries) indexQueryNodes(roots map[string]*operators.Query) error {
	var iter []SemanticNode

	for _, q := range roots {
		semnode := SemanticNode{node: q.View, semTable: q.SemTable}
		iter = append(iter, semnode)
	}

	for len(iter) > 0 {
		curr := iter[len(iter)-1]
		iter = iter[:len(iter)-1]

		addr, err := curr.node.FlowNodeAddr()
		if err != nil {
			return err
		}
		up.index[addr] = SemanticNode{
			node:     curr.node,
			semTable: curr.semTable,
		}
		for _, ancestor := range curr.node.Ancestors {
			iter = append(iter, SemanticNode{node: ancestor, semTable: curr.semTable})
		}
	}
	return nil
}

func NewUpqueryPlanner(inc *Incorporator) *Upqueries {
	up := &Upqueries{
		inc:   inc,
		index: make(map[graph.NodeIdx]SemanticNode),
	}
	if err := up.indexQueryNodes(inc.leafs); err != nil {
		// indexing cannot really fail unless the plan was bugged and didn't place
		// all our flownodes into the graph
		panic(err)
	}
	return up
}

// GetQueryFor returns the AST for a query that represents the subtree starting from `addr`.`
func (up *Upqueries) GetQueryFor(addr graph.NodeIdx, keys []int) (sqlparser.Statement, error) {
	node, found := up.index[addr]
	if !found {
		return nil, nil
	}
	ctx := operators.PlanContext{
		SemTable: node.semTable,
	}
	return operators.ToSQL(&ctx, node.node, keys)
}
