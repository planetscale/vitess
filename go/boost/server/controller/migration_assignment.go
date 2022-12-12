package controller

import (
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
)

func (mig *migration) assign(topolist []graph.NodeIdx) {
	g := mig.target.ingredients
	nextdomain := func() boostpb.DomainIndex {
		next := boostpb.DomainIndex(mig.target.nDomains)
		mig.target.nDomains++
		return next
	}

	for _, node := range topolist {
		assignment := func() boostpb.DomainIndex {
			n := g.Value(node)

			// TODO(NORIA): the code below is probably _too_ good at keeping things in one domain.
			// 		having all bases in one domain (e.g., if sharding is disabled) isn't great because
			// 		write performance will suffer terribly.

			if n.IsShardMerger() {
				// shard mergers are always in their own domain.
				// we *could* use the same domain for multiple separate shard mergers
				// but it's unlikely that would do us any good.
				return nextdomain()
			}

			if n.IsReader() {
				// readers always re-materialize, so sharing a domain doesn't help them much.
				// having them in their own domain also means that they get to aggregate reader
				// replay requests in their own little thread, and not interfere as much with other
				// internal traffic.
				return nextdomain()
			}

			if n.IsExternalBase() {
				return nextdomain()
			}

			if n.IsBase() {
				// bases are in a little bit of an awkward position becuase they can't just blindly
				// join in domains of other bases in the face of sharding. consider the case of two
				// bases, A and B, where A is sharded by A[0] and B by B[0]. Can they share a
				// domain? The way we deal with this is that we walk *down* from the base until we
				// hit any sharders or shard mergers, and then we walk *up* from each node visited
				// on the way down until we hit a base without traversing a sharding.
				// XXX: maybe also do this extended walk for non-bases?

				var childrenSameShard []graph.NodeIdx
				var frontier []graph.NodeIdx

				it := g.NeighborsDirected(node, graph.DirectionOutgoing)
				for it.Next() {
					frontier = append(frontier, it.Current)
				}

				for len(frontier) > 0 {
					ff := slices.Clone(frontier)
					frontier = frontier[:0]

					for _, cni := range ff {
						c := g.Value(cni)
						if c.IsSharder() || c.IsShardMerger() {
							// nothing
						} else {
							childrenSameShard = append(childrenSameShard, cni)
							it = g.NeighborsDirected(cni, graph.DirectionOutgoing)
							for it.Next() {
								frontier = append(frontier, it.Current)
							}
						}
					}
				}

				var friendlyBase *flownode.Node
				frontier = childrenSameShard

			search:
				for len(frontier) > 0 {
					ff := slices.Clone(frontier)
					frontier = frontier[:0]

					for _, pni := range ff {
						if pni == node {
							continue
						}

						p := g.Value(pni)
						switch {
						case p.IsSource() || p.IsSharder() || p.IsShardMerger():
						case p.IsAnyBase():
							if p.Domain() != boostpb.InvalidDomainIndex {
								friendlyBase = p
								break search
							}
						default:
							it = g.NeighborsDirected(pni, graph.DirectionIncoming)
							for it.Next() {
								frontier = append(frontier, it.Current)
							}
						}
					}
				}

				if friendlyBase != nil {
					return friendlyBase.Domain()
				}
				return nextdomain()
			}

			anyParents := func(prime func(*flownode.Node) bool, check func(*flownode.Node) bool) bool {
				var stack []graph.NodeIdx
				it := g.NeighborsDirected(node, graph.DirectionIncoming)
				for it.Next() {
					if prime(g.Value(it.Current)) {
						stack = append(stack, it.Current)
					}
				}

				for len(stack) > 0 {
					p := stack[len(stack)-1]
					stack = stack[:len(stack)-1]

					nn := g.Value(p)
					if nn.IsSource() {
						continue
					}
					if check(nn) {
						return true
					}
					it = g.NeighborsDirected(p, graph.DirectionIncoming)
					for it.Next() {
						stack = append(stack, it.Current)
					}
				}

				return false
			}

			type parent struct {
				pni graph.NodeIdx
				p   *flownode.Node
			}
			var parents []parent

			piter := g.NeighborsDirected(node, graph.DirectionIncoming)
			for piter.Next() {
				parents = append(parents, parent{piter.Current, g.Value(piter.Current)})
			}

			var assignment *boostpb.DomainIndex
			for _, parent := range parents {
				p := parent.p
				switch {
				case p.IsExternalBase():
					// we don't want to be on the same domain as an external base
				case p.IsSharder():
					// we're a child of a sharder (which currently has to be unsharded). we
					// can't be in the same domain as the sharder (because we're starting a new
					// sharding)
				case p.IsSource():
					// the source isn't a useful source of truth
				case assignment == nil:
					// the key may move to a different column, so we can't actually check for
					// ByColumn equality. this'll do for now.
					if dom := p.Domain(); dom != boostpb.InvalidDomainIndex {
						assignment = &dom
					}
				}

				if assignment != nil {
					candidate := *assignment
					if anyParents(
						func(p *flownode.Node) bool {
							return p.Domain() != boostpb.InvalidDomainIndex && p.Domain() != candidate
						},
						func(pp *flownode.Node) bool { return pp.Domain() == candidate }) {
						assignment = nil
						continue
					}
					break
				}
			}

			if assignment == nil {
				// check our siblings too
				// XXX: we could keep traversing here to find cousins and such
				for _, parent := range parents {
					pni := parent.pni
					siblings := g.NeighborsDirected(pni, graph.DirectionOutgoing)
					for siblings.Next() {
						s := g.Value(siblings.Current)
						if s.Domain() == boostpb.InvalidDomainIndex {
							continue
						}
						if s.Sharding().IsNone() != n.Sharding().IsNone() {
							continue
						}
						candidate := s.Domain()
						if anyParents(
							func(p *flownode.Node) bool {
								return p.Domain() != boostpb.InvalidDomainIndex && p.Domain() != candidate
							},
							func(pp *flownode.Node) bool { return pp.Domain() == candidate }) {
							continue
						}

						assignment = &candidate
						break
					}
				}
			}

			if assignment != nil {
				return *assignment
			}
			return nextdomain()
		}()

		g.Value(node).AddTo(assignment)
	}
}
