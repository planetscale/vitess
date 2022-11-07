//! Functions for adding ingress/egress nodes.
//!
//! In particular:
//!
//!  - New nodes that are children of nodes in a different domain must be preceeded by an ingress
//!  - Egress nodes must be added to nodes that now have children in a different domain
//!  - Egress nodes that gain new children must gain channels to facilitate forwarding

package controller

import (
	"context"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
)

// Add in ingress and egress nodes as appropriate in the graph to facilitate cross-domain
// communication.
func migrationAddRouting(g *graph.Graph[*flownode.Node], source graph.NodeIdx, new map[graph.NodeIdx]bool, topolist []graph.NodeIdx) map[graph.NodeIdxPair]graph.NodeIdx {
	// find all new nodes in topological order. we collect first since we'll be mutating the graph
	// below. it's convenient to have the nodes in topological order, because we then know that
	// we'll first add egress nodes, and then the related ingress nodes. if we're ever required to
	// add an ingress node, and its parent isn't an egress node, we know that we're seeing a
	// connection between an old node in one domain, and a new node in a different domain.

	// we need to keep track of all the times we change the parent of a node (by replacing it with
	// an egress, and then with an ingress), since this remapping must be communicated to the nodes
	// so they know the true identifier of their parent in the graph.
	var swaps = make(map[graph.NodeIdxPair]graph.NodeIdx)

	// in the code below, there are three node type of interest: ingress, egress, and sharder. we
	// want to ensure the following properties:
	//
	//  - every time an edge crosses a domain boundary, the target of the edge is an ingress node.
	//  - every ingress node has a parent that is either a sharder or an egress node.
	//  - if an ingress does *not* have such a parent, we add an egress node to the ingress'
	//    ancestor's domain, and interject it between the ingress and its old parent.
	//  - every domain has at most one egress node as a child of any other node.
	//  - every domain has at most one ingress node connected to any single egress node.
	//
	// this is a lot to keep track of. the last two invariants (which are mostly for efficiency) in
	// particular require some extra bookkeeping, especially considering that they may end up
	// causing re-use of ingress and egress nodes that were added in a *previous* migration.
	//
	// we do this in a couple of passes, as described below.

	for _, node := range topolist {
		dom := g.Value(node).Domain()
		parents := g.NeighborsDirected(node, graph.DirectionIncoming).Collect(nil)

		for _, parent := range parents {
			graphParent := g.Value(parent)
			if graphParent.IsSource() || graphParent.Domain() == dom {
				continue
			}

			// parent is in other domain! does it already have an egress?
			var ingress *graph.NodeIdx
			if parent != source {
				pchild := g.NeighborsDirected(parent, graph.DirectionOutgoing)

			search:
				for pchild.Next() {
					if g.Value(pchild.Current).IsEgress() {
						// it does! does `domain` have an ingress already listed there?
						i := g.NeighborsDirected(pchild.Current, graph.DirectionOutgoing)
						for i.Next() {
							if g.Value(i.Current).Domain() == dom {
								// it does! we can just reuse that ingress :D
								ingress = &i.Current
								// FIXME(malte): this is buggy! it will re-use ingress nodes even if
								// they have a different sharding to the one we're about to add
								// (whose sharding is only determined below).
								break search
							}
						}
					}
				}
			}

			var finalingress graph.NodeIdx
			if ingress == nil {
				// we need to make a new ingress
				i := graphParent.Mirror(&flownode.Ingress{})

				// it belongs to this domain, not that of the parent
				i.AddTo(dom)

				// the ingress is sharded the same way as its target, but with remappings of parent
				// columns applied
				var sharding boostpb.Sharding
				if sharder := graphParent.AsSharder(); sharder != nil {
					parentOutSharding := sharder.ShardedBy()
					// TODO(malte): below is ugly, but the only way to get the sharding width at
					// this point; the sharder parent does not currently have the information.
					// Change this once we support per-subgraph sharding widths and
					// the sharder knows how many children it is supposed to have.
					if _, width, ok := g.Value(node).Sharding().ByColumn(); ok {
						sharding = boostpb.NewShardingByColumn(parentOutSharding, width)
					} else {
						panic("unexpected sharding mode")
					}
				} else {
					sharding = graphParent.Sharding()
				}

				i.ShardBy(sharding)
				finalingress = g.AddNode(i)
				g.AddEdge(parent, finalingress)
				new[finalingress] = true
			} else {
				finalingress = *ingress
			}

			// we need to hook the ingress node in between us and our remote parent
			{
				old := g.FindEdge(parent, node)
				g.RemoveEdge(old)
				g.AddEdge(finalingress, node)
			}

			// we now need to refer to the ingress instead of the "real" parent
			swaps[graph.NodeIdxPair{One: node, Two: parent}] = finalingress
		}

		// we now have all the ingress nodes we need. it's time to check that they are all
		// connected to an egress or a sharder (otherwise they would never receive anything!).
		// Note that we need to re-load the list of parents, because it might have changed as a
		// result of adding ingress nodes.
		parents = g.NeighborsDirected(node, graph.DirectionIncoming).Collect(nil)
		for _, ingress := range parents {
			graphIngress := g.Value(ingress)
			if !graphIngress.IsIngress() {
				continue
			}

			senders := g.NeighborsDirected(ingress, graph.DirectionIncoming)
			if !senders.Next() {
				panic("ingress has no parents")
			}
			sender := senders.Current
			if senders.Next() {
				panic("ingress has more than one parent")
			}

			if sender == source {
				// no need for egress from source
				continue
			}

			graphSender := g.Value(sender)
			if graphSender.IsSender() {
				// all good -- we're already hooked up with an egress or sharder!
				continue
			}

			// ingress is not already connected to egress/sharder
			// next, check if source node already has an egress
			var egress *graph.NodeIdx
			es := g.NeighborsDirected(sender, graph.DirectionOutgoing)
			for es.Next() {
				if g.Value(es.Current).IsEgress() {
					egress = &es.Current
					break
				}
			}

			var finalegress graph.NodeIdx
			if egress == nil {
				// need to inject an egress above us

				// NOTE: technically, this doesn't need to mirror its parent, but meh
				egressNode := graphSender.Mirror(flownode.NewEgress())
				egressNode.AddTo(graphSender.Domain())
				egressNode.ShardBy(graphSender.Sharding())

				finalegress = g.AddNode(egressNode)
				g.AddEdge(sender, finalegress)

				// we also now need to deal with this egress node
				new[finalegress] = true
			} else {
				finalegress = *egress
			}

			{
				old := g.FindEdge(sender, ingress)
				g.RemoveEdge(old)
				g.AddEdge(finalegress, ingress)
			}

			// NOTE: we *don't* need to update swaps here, because ingress doesn't care
		}
	}

	return swaps
}

func migrationRoutingConnect(ctx context.Context, g *graph.Graph[*flownode.Node], domains map[boostpb.DomainIndex]*DomainHandle, workers map[WorkerID]*Worker, new map[graph.NodeIdx]bool) error {
	log := common.Logger(ctx)
	for node := range new {
		n := g.Value(node)
		if n.IsIngress() {
			// check the egress connected to this ingress
		} else {
			continue
		}

		it := g.NeighborsDirected(node, graph.DirectionIncoming)
		for it.Next() {
			senderNode := g.Value(it.Current)
			switch {
			case senderNode.IsEgress():
				log.Debug("migration routing: connect", it.Current.ZapField("egress"), node.ZapField("ingress"))

				shards := domains[n.Domain()].Shards()
				domain := domains[senderNode.Domain()]

				if shards != 1 && !senderNode.Sharding().IsNone() {
					for s := uint(0); s < shards; s++ {
						var pkt boostpb.Packet
						pkt.Inner = &boostpb.Packet_UpdateEgress_{
							UpdateEgress: &boostpb.Packet_UpdateEgress{
								Node: senderNode.LocalAddr(),
								NewTx: &boostpb.Packet_UpdateEgress_Tx{
									Node:  uint32(node),
									Local: uint32(n.LocalAddr()),
									Domain: &boostpb.DomainAddr{
										Domain: n.Domain(),
										Shard:  s,
									},
								},
							},
						}
						if err := domain.SendToHealthyShard(ctx, s, &pkt, workers); err != nil {
							return err
						}
					}
				} else {
					// consider the case where len != 1. that must mean that the
					// sender_node.sharded_by() == Sharding::None. so, we have an unsharded egress
					// sending to a sharded child. but that shouldn't be allowed -- such a node
					// *must* be a Sharder.
					if shards != 1 {
						panic("unsharded egress sending to a sharded child")
					}

					var pkt boostpb.Packet
					pkt.Inner = &boostpb.Packet_UpdateEgress_{UpdateEgress: &boostpb.Packet_UpdateEgress{
						Node: senderNode.LocalAddr(),
						NewTx: &boostpb.Packet_UpdateEgress_Tx{
							Node:  uint32(node),
							Local: uint32(n.LocalAddr()),
							Domain: &boostpb.DomainAddr{
								Domain: n.Domain(),
								Shard:  0,
							},
						},
					}}
					if err := domain.SendToHealthy(ctx, &pkt, workers); err != nil {
						return err
					}
				}
			case senderNode.IsSharder():
				shards := domains[n.Domain()].Shards()
				update := &boostpb.Packet_UpdateSharder{
					Node:   senderNode.LocalAddr(),
					NewTxs: make([]*boostpb.Packet_UpdateSharder_Tx, 0, shards),
				}
				for s := uint(0); s < shards; s++ {
					update.NewTxs = append(update.NewTxs, &boostpb.Packet_UpdateSharder_Tx{
						Local: n.LocalAddr(),
						Domain: &boostpb.DomainAddr{
							Domain: n.Domain(),
							Shard:  s,
						},
					})
				}

				var pkt boostpb.Packet
				pkt.Inner = &boostpb.Packet_UpdateSharder_{UpdateSharder: update}
				if err := domains[senderNode.Domain()].SendToHealthy(ctx, &pkt, workers); err != nil {
					return err
				}
			case senderNode.IsSource():
			// noop
			default:
				panic("ingress parent is not a sender")
			}
		}
	}
	return nil
}
