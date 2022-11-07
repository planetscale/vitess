package controller

import (
	"context"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/graph"
)

type NewNode struct {
	Idx graph.NodeIdx
	New bool
}

func migrationAugmentationInform(ctx context.Context, controller *Controller, nodes map[boostpb.DomainIndex][]NewNode) error {
	source := controller.source

	for domain, nodes := range nodes {
		sender := controller.domains[domain]

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
			node := controller.ingredients.Value(ni).Finalize(controller.ingredients)

			var oldParents []boostpb.LocalNodeIndex
			g := controller.ingredients
			it := g.NeighborsDirected(ni, graph.DirectionIncoming)
			for it.Next() {
				ni := it.Current
				if ni != source && oldNodes[ni] {
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

			if err := sender.SendToHealthy(ctx, &pkt, controller.workers); err != nil {
				return err
			}
		}
	}
	return nil
}
