package controller

import (
	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/dataflow"

	"vitess.io/vitess/go/boost/graph"
)

type nodeWithAge struct {
	Idx graph.NodeIdx
	New bool
}

func (mig *migration) augmentationInform(nodes map[dataflow.DomainIdx][]nodeWithAge) error {
	g := mig.target.graph()

	for domain, nodes := range nodes {
		oldNodes := make(map[graph.NodeIdx]bool)
		for _, nn := range nodes {
			if !nn.New {
				oldNodes[nn.Idx] = true
			}
		}

		for _, nn := range nodes {
			if !nn.New {
				continue
			}
			ni := nn.Idx
			node, err := g.Value(ni).Finalize(g)
			if err != nil {
				return err
			}

			var oldParents []dataflow.LocalNodeIdx
			it := g.NeighborsDirected(ni, graph.DirectionIncoming)
			for it.Next() {
				ni := it.Current
				if !ni.IsRoot() && oldNodes[ni] {
					n := g.Value(ni)
					if n.Domain() == domain {
						oldParents = append(oldParents, n.LocalAddr())
					}
				}
			}

			pkt := &packet.AddNodeRequest{
				Node:    node.ToProto(),
				Parents: oldParents,
			}
			if err := mig.Send(domain).AddNode(pkt); err != nil {
				return err
			}
		}
	}
	return nil
}
