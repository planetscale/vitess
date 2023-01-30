package controller

import (
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
)

func (mig *migration) assign(topolist []graph.NodeIdx) {
	g := mig.target.graph()

	for _, node := range topolist {
		assignment := func() dataflow.DomainIdx {
			n := g.Value(node)

			// TODO(NORIA): the code below is probably _too_ good at keeping things in one domain.
			// 		having all bases in one domain (e.g., if sharding is disabled) isn't great because
			// 		write performance will suffer terribly.

			if n.IsShardMerger() {
				// shard mergers are always in their own domain.
				// we *could* use the same domain for multiple separate shard mergers
				// but it's unlikely that would do us any good.
				return mig.target.domainNext()
			}

			if n.IsReader() {
				// readers always re-materialize, so sharing a domain doesn't help them much.
				// having them in their own domain also means that they get to aggregate reader
				// replay requests in their own little thread, and not interfere as much with other
				// internal traffic.
				return mig.target.domainNext()
			}

			if n.IsTable() {
				return mig.target.domainNext()
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
					if nn.IsRoot() {
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

			var assignment *dataflow.DomainIdx
			for _, parent := range parents {
				p := parent.p
				switch {
				case p.IsTable():
					// we don't want to be on the same domain as an external table
				case p.IsSharder():
					// we're a child of a sharder (which currently has to be unsharded). we
					// can't be in the same domain as the sharder (because we're starting a new
					// sharding)
				case p.IsRoot():
					// the source isn't a useful source of truth
				case assignment == nil:
					// the key may move to a different column, so we can't actually check for
					// ByColumn equality. this'll do for now.
					if dom := p.Domain(); dom != dataflow.InvalidDomainIdx {
						assignment = &dom
					}
				}

				if assignment != nil {
					candidate := *assignment
					if anyParents(
						func(p *flownode.Node) bool {
							return p.Domain() != dataflow.InvalidDomainIdx && p.Domain() != candidate
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
						if s.Domain() == dataflow.InvalidDomainIdx {
							continue
						}
						if s.Sharding().IsNone() != n.Sharding().IsNone() {
							continue
						}
						candidate := s.Domain()
						if anyParents(
							func(p *flownode.Node) bool {
								return p.Domain() != dataflow.InvalidDomainIdx && p.Domain() != candidate
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
			return mig.target.domainNext()
		}()

		g.Value(node).AddTo(assignment)
	}
}
