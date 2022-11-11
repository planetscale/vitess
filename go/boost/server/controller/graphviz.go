package controller

import (
	"fmt"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common/graphviz"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
)

func renderGraphviz(gvz *graphviz.Graph[graph.NodeIdx], g *graph.Graph[*flownode.Node], materialization *Materialization, domainClustering bool) {
	lastnode := graph.NodeIdx(g.NodeCount())
	for idx := graph.NodeIdx(0); idx < lastnode; idx++ {
		node := g.Value(idx)

		n := gvz.AddNode(idx)
		if domainClustering && node.Domain() != boostpb.InvalidDomainIndex {
			n.Subgraph = fmt.Sprintf("domain_%d", node.Domain())
		}
		node.ResolveSchema(g)
		node.Describe(n, flownode.DescribeOptions{
			Materialization: materialization.GetStatus(node),
			ShowSchema:      true,
		})
	}

	for _, edge := range g.RawEdges() {
		src := edge.Source()
		tgt := edge.Target()
		edg := gvz.AddEdge(src, tgt)

		switch nn := g.Value(src); {
		case nn.IsEgress():
			edg.Attr["color"] = "#CCCCCC"
		case nn.IsSource():
			edg.Attr["style"] = "invis"
		}
	}
}
