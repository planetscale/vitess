package controller

import (
	"context"
	"fmt"
	"io"

	"vitess.io/vitess/go/boost/common/graphviz"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
)

func (ctrl *Controller) Graphviz(ctx context.Context, buf io.Writer, req *vtboostpb.GraphvizRequest) error {
	gvz := graphviz.NewGraph[graph.NodeIdx]()
	gvz.Clustering = req.Clustering != vtboostpb.GraphvizRequest_NONE

	g := ctrl.g
	lastnode := graph.NodeIdx(g.NodeCount())
	for idx := graph.NodeIdx(0); idx < lastnode; idx++ {
		node := g.Value(idx)

		n := gvz.AddNode(idx)
		if req.Clustering == vtboostpb.GraphvizRequest_DOMAIN && node.Domain() != dataflow.InvalidDomainIdx {
			n.Subgraph = fmt.Sprintf("domain_%d", node.Domain())
		}
		_, err := node.ResolveSchema(g)
		if err != nil {
			return err
		}
		node.RenderGraphviz(n, flownode.GraphvizOptions{
			Materialization: ctrl.mat.GetStatus(node),
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
		case nn.IsRoot():
			edg.Attr["style"] = "invis"
		}
	}

	if !req.HideReplayPaths {
		ctrl.mat.RenderGraphviz(gvz)
	}

	if !req.HideMemoryStats {
		plan, err := ctrl.PrepareEvictionPlan(ctx)
		if err != nil {
			return err
		}
		if req.ForceMemoryLimits != nil {
			plan.SetCustomLimits(req.ForceMemoryLimits)
		}
		plan.RenderGraphviz(gvz, req.Clustering == vtboostpb.GraphvizRequest_QUERY)
	}

	gvz.Render(buf)
	return nil
}