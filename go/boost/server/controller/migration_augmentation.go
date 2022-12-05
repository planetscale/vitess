package controller

import (
	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/graph"
)

type nodeWithAge struct {
	Idx graph.NodeIdx
	New bool
}

func (mig *migration) augmentationInform(nodes map[boostpb.DomainIndex][]nodeWithAge) error {
	g := mig.target.ingredients

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
			node := g.Value(ni).Finalize(g)

			var oldParents []boostpb.LocalNodeIndex
			it := g.NeighborsDirected(ni, graph.DirectionIncoming)
			for it.Next() {
				ni := it.Current
				if !ni.IsSource() && oldNodes[ni] {
					n := g.Value(ni)
					if n.Domain() == domain {
						oldParents = append(oldParents, n.LocalAddr())
					}
				}
			}

			var pkt boostpb.Packet
			pkt.Inner = &boostpb.Packet_AddNode_{
				AddNode: &boostpb.Packet_AddNode{
					Node:    node.ToProto(),
					Parents: oldParents,
				},
			}
			if err := mig.SendPacket(domain, &pkt); err != nil {
				return err
			}
		}
	}
	return nil
}
