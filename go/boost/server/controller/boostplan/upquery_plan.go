package boostplan

import (
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
)

type Upqueries struct {
	inc   *Incorporator
	index map[graph.NodeIdx]*operators.Node
}

func (up *Upqueries) indexQueryNodes(roots map[string]*operators.Query) error {
	var iter []*operators.Node

	for _, q := range roots {
		iter = append(iter, q.View)
	}

	for len(iter) > 0 {
		node := iter[len(iter)-1]
		addr, err := node.FlowNodeAddr()
		if err != nil {
			return err
		}
		up.index[addr] = node
		iter = append(iter[:len(iter)-1], node.Ancestors...)
	}
	return nil
}

func NewUpqueryPlanner(inc *Incorporator) *Upqueries {
	up := &Upqueries{
		inc:   inc,
		index: make(map[graph.NodeIdx]*operators.Node),
	}
	if err := up.indexQueryNodes(inc.leafs); err != nil {
		// indexing cannot really fail unless the plan was bugged and didn't place
		// all our flownodes into the graph
		panic(err)
	}
	return up
}
