package controller

import (
	"fmt"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
)

func allShardingsAreNone(shardings map[graph.NodeIdx]boostpb.Sharding) bool {
	for _, sharding := range shardings {
		if !sharding.IsNone() {
			return false
		}
	}
	return true
}

func anyShardingIsNone(shardings map[graph.NodeIdx]boostpb.Sharding) bool {
	for _, sharding := range shardings {
		if sharding.Mode == boostpb.Sharding_ForcedNone {
			return true
		}
	}
	return false
}

func firstSharding(shardings map[graph.NodeIdx]boostpb.Sharding) boostpb.Sharding {
	for _, sh := range shardings {
		return sh
	}
	panic("firstSharding on empty map")
}

func (mig *migration) shard(new map[graph.NodeIdx]bool, topoList []graph.NodeIdx, shardingFactor uint) ([]graph.NodeIdx, map[graph.NodeIdxPair]graph.NodeIdx, error) {
	g := mig.target.ingredients
	swaps := make(map[graph.NodeIdxPair]graph.NodeIdx)

nodes:
	for _, node := range topoList {
		inputShardings := make(map[graph.NodeIdx]boostpb.Sharding)
		it := g.NeighborsDirected(node, graph.DirectionIncoming)
		for it.Next() {
			inputShardings[it.Current] = g.Value(it.Current).Sharding()
		}

		graphNode := g.Value(node)

		var needSharding map[graph.NodeIdx][]int
		switch {
		case graphNode.IsInternal() || graphNode.IsAnyBase():
			needSharding = graphNode.SuggestIndexes(node)

		case graphNode.IsReader():
			if len(inputShardings) != 1 {
				panic("len(inputShardings) != 1")
			}
			var ni graph.NodeIdx
			for n := range inputShardings {
				ni = n
			}
			if inputShardings[ni].IsNone() {
				continue
			}

			var s boostpb.Sharding
			ckey := graphNode.AsReader().Key()
			if len(ckey) == 1 {
				if graphNode.Fields()[ckey[0]] == "bogokey" {
					s = boostpb.NewShardingForcedNone()
				} else {
					s = boostpb.NewShardingByColumn(ckey[0], shardingFactor)
				}
			} else {
				s = boostpb.NewShardingForcedNone()
			}
			if s.IsNone() {
				mig.log.Debug("de-sharding prior to poorly keyed reader", node.Zap())
			} else {
				mig.log.Debug("sharding reader", node.Zap())
				// FIXME: this is a no-op
				graphNode.AsReader().Shard(shardingFactor)
			}

			if s != inputShardings[ni] {
				mig.reshard(new, swaps, ni, node, s)
			}
			graphNode.ShardBy(s)
			continue

		case graphNode.IsSource():
			continue

		default:
			// non-internal nodes are always passthrough
		}

		if len(needSharding) == 0 && (len(inputShardings) == 1 || allShardingsAreNone(inputShardings)) {
			var s boostpb.Sharding
			if anyShardingIsNone(inputShardings) {
				s = boostpb.NewShardingForcedNone()
			} else {
				s = firstSharding(inputShardings)
			}

			mig.log.Debug("preserving sharding for pass-through node", node.Zap())

			if graphNode.IsInternal() || graphNode.IsAnyBase() {
				if c, shards, ok := s.ByColumn(); ok {
					col, maxcol := 0, len(graphNode.Fields())
					for col = 0; col < maxcol; col++ {
						src := graphNode.ParentColumns(col)[0].Column
						if src != -1 && src == c {
							break
						}
					}

					if col < maxcol {
						s = boostpb.NewShardingByColumn(col, shards)
					} else {
						s = boostpb.NewShardingRandom(shards)
					}
				}
			}

			graphNode.ShardBy(s)
			continue
		}

		isComplex := false
		for _, lookupCol := range needSharding {
			if len(lookupCol) != 1 {
				isComplex = true
			}
		}
		if isComplex {
			if !graphNode.IsAnyBase() {
				// not supported yet -- force no sharding
				// TODO: if we're sharding by a two-part key and need sharding by the *first* part
				// of that key, we can probably re-use the existing sharding?
				mig.log.Error("de-sharding for lack of multi-key sharding support", node.Zap())
				for ni := range inputShardings {
					mig.reshard(new, swaps, ni, node, boostpb.NewShardingForcedNone())
				}
			}
			continue
		}

		// if a node does a lookup into itself by a given key, it must be sharded by that key (or
		// not at all). this *also* means that its inputs must be sharded by the column(s) that the
		// output column resolves to.
		if wantShardingVec, ok := needSharding[node]; ok {
			if len(wantShardingVec) != 1 {
				panic("len(wantSharding) != 1")
			}
			wantSharding := wantShardingVec[0]
			if graphNode.Fields()[wantSharding] == "bogokey" {
				mig.log.Debug("de-sharding node that operates on bogokey", node.Zap())
				for ni := range inputShardings {
					mig.reshard(new, swaps, ni, node, boostpb.NewShardingForcedNone())
					inputShardings[ni] = boostpb.NewShardingForcedNone()
				}
				continue
			}

			var resolved []flownode.NodeColumn
			switch {
			case graphNode.IsInternal():
				resolved = graphNode.Resolve(wantSharding)
			case graphNode.IsAnyBase():
				resolved = nil
			default:
				resolved = make([]flownode.NodeColumn, 0, 4)
				// non-internal nodes just pass through columns
				it := g.NeighborsDirected(node, graph.DirectionIncoming)
				for it.Next() {
					resolved = append(resolved, flownode.NodeColumn{Node: it.Current, Column: wantSharding})
				}
			}

			switch {
			case resolved == nil && !graphNode.IsAnyBase():
				// weird operator -- needs an index in its output, which it generates.
				// we need to have *no* sharding on our inputs!
				for ni := range inputShardings {
					mig.reshard(new, swaps, ni, node, boostpb.NewShardingForcedNone())
					inputShardings[ni] = boostpb.NewShardingForcedNone()
				}
				// ok to continue since standard shard_by is None
				continue

			case resolved == nil:
				graphNode.ShardBy(boostpb.NewShardingByColumn(wantSharding, shardingFactor))
				continue

			default:
				wantShardingInput := make(map[graph.NodeIdx]int)
				for _, nc := range resolved {
					wantShardingInput[nc.Node] = nc.Column
				}

				// we can shard by the ouput column `want_sharding` *only* if we don't do
				// lookups based on any *other* columns in any ancestor. if we do, we must
				// force no sharding :(
				canshard := true
				for ni, lookupColVec := range needSharding {
					lookupCol := lookupColVec[0]

					if inShardCol, ok := wantShardingInput[ni]; ok {
						// we do lookups on this input on a different column than the one
						// that produces the output shard column.
						if inShardCol != lookupCol {
							canshard = false
						}
					} else {
						// we do lookups on this input column, but it's not the one we're
						// sharding output on -- no unambigous sharding.
						canshard = false
					}
				}

				if canshard {
					s := boostpb.NewShardingByColumn(wantSharding, shardingFactor)
					for ni, col := range wantShardingInput {
						needShardingShard := boostpb.NewShardingByColumn(col, shardingFactor)
						if inputShardings[ni] != needShardingShard {
							mig.reshard(new, swaps, ni, node, needShardingShard)
							inputShardings[ni] = needShardingShard
						}
					}

					graphNode.ShardBy(s)
					continue
				}
			}
		} else {
			// if we get here, the node does no lookups into itself, but we still need to figure
			// out a "safe" sharding for it given that its inputs may be sharded. the safe thing to
			// do here is to simply force all our ancestors to be unsharded, but that would lead to
			// a very suboptimal graph. instead, we try to choose a sharding that is "harmonious"
			// with that of our inputs.
			mig.log.Debug("testing for harmonious sharding", node.Zap())

		outer:
			for col := range graphNode.Fields() {
				var srcs []flownode.NodeColumn

				if graphNode.IsAnyBase() {
					srcs = nil
				} else {
					for _, src := range graphNode.ParentColumns(col) {
						if src.Column != -1 {
							srcs = append(srcs, src)
						}
					}
				}

				if len(srcs) != len(inputShardings) {
					// column does not resolve to all inputs
					continue
				}

				if len(needSharding) == 0 {
					// if we don't ever do lookups into our ancestors, we just need to find _some_
					// good sharding for this node. a column that resolves to all ancestors makes
					// for a good candidate! if this single output column (which resolves to a
					// column in all our inputs) matches what each ancestor is individually sharded
					// by, then we know that the output of the node is also sharded by that key.
					// this is sufficiently common that we want to make sure we don't accidentally
					// shuffle in those cases.

					allSame := true
					for _, nc := range srcs {
						if inputShardings[nc.Node] != boostpb.NewShardingByColumn(nc.Column, shardingFactor) {
							allSame = false
							break
						}
					}

					if allSame {
						graphNode.ShardBy(boostpb.NewShardingByColumn(col, shardingFactor))
						continue nodes
					}
				} else {
					// if a single output column resolves to the lookup column we use for *every*
					// ancestor, we know that sharding by that column is safe, so we shard the node
					// by that key (and shuffle any inputs that are not already shareded by the
					// chosen column).

					for _, nc := range srcs {
						col, ok := needSharding[nc.Node]
						if !ok {
							// we're never looking up in this view. must mean that a given
							// column resolved to *two* columns in the *same* view?
							panic("unreachable")
						}

						if len(col) != 1 {
							// we're looking up by a compound key -- that's hard to shard
							break outer
						}
						if col[0] != nc.Column {
							// we're looking up by a different key. it's kind of weird that this
							// output column still resolved to a column in all our inputs...
							// let's hope another column works instead
							continue outer
						}
						// looking up by the same column -- that's fine
					}

					// `col` resolves to the same column we use to lookup in each ancestor
					// so it's safe for us to shard by `col`!
					s := boostpb.NewShardingByColumn(col, shardingFactor)

					// we have to ensure that each input is also sharded by that key
					// specifically, some inputs may _not_ be sharded previously
					for _, nc := range srcs {
						needShardingShard := boostpb.NewShardingByColumn(nc.Column, shardingFactor)
						if inputShardings[nc.Node] != needShardingShard {
							mig.reshard(new, swaps, nc.Node, node, needShardingShard)
							inputShardings[nc.Node] = needShardingShard
						}
					}

					graphNode.ShardBy(s)
					continue nodes
				}
			}

			/*
				if len(needSharding) == 0 {
					// if we get here, that means no one column resolves to matching shardings across
					// all ancestors. we have two options here, either force no sharding or force
					// sharding to the "most common" sharding of our ancestors. the latter is left as
					// TODO for now.
				} else {
					// if we get here, there is no way the node can be sharded such that all of its
					// lookups are satisfiable on one shard. this effectively means that the operator
					// is unshardeable (or would need to _always_ do remote lookups for some
					// ancestors).
				}
			*/
		}

		// force everything to be unsharded...
		sharding := boostpb.NewShardingForcedNone()
		for ni, inSharding := range inputShardings {
			if !inSharding.IsNone() {
				mig.reshard(new, swaps, ni, node, sharding)
				inputShardings[ni] = inSharding
			}
		}
	}

	// the code above can do some stupid things, such as adding a sharder after a new, unsharded
	// node. we want to "flatten" such cases so that we shard as early as we can.
	var newSharders []graph.NodeIdx
	for n := range new {
		if g.Value(n).IsSharder() {
			newSharders = append(newSharders, n)
		}
	}

	gone := make(map[graph.NodeIdx]struct{})
	for len(newSharders) > 0 {
		newSharderSplit := newSharders
		newSharders = newSharders[:0]

	sharders:
		for _, n := range newSharderSplit {
			mig.log.Debug("can we eliminate sharder?", n.Zap())
			if _, contains := gone[n]; contains {
				mig.log.Debug("no, parent is weird (already eliminated)")
				continue
			}

			p := g.NeighborsDirected(n, graph.DirectionIncoming).First()

			graphN := g.Value(n)
			graphP := g.Value(p)

			if graphP.IsSource() {
				panic("a sharder should never be placed right under the source")
			}

			// and that its children must be sharded somehow (otherwise what is the sharder doing?)
			col := graphN.AsSharder().ShardedBy()
			by := boostpb.NewShardingByColumn(col, shardingFactor)

			// we can only push sharding above newly created nodes that are not already sharded.
			if !new[p] || graphP.Sharding().Mode != boostpb.Sharding_None {
				continue
			}

			if graphP.IsAnyBase() {
				if k, ok := graphP.AsBase().Key(); ok {
					if len(k) != 1 {
						continue
					}
				}

				// if the base has other children, sharding it may have other effects
				if g.NeighborsDirected(p, graph.DirectionOutgoing).Count() != 1 {
					// TODO: technically we could still do this if the other children were sharded by the same column.
					continue
				}

				graphP.ShardBy(by)

				cs := g.NeighborsDirected(n, graph.DirectionOutgoing)
				for cs.Next() {
					c := cs.Current

					// undo the swap that inserting the sharder in the first place generated
					delete(swaps, graph.NodeIdxPair{One: c, Two: p})

					// unwire the child from the sharder and wire to the base directly
					e := g.FindEdge(n, c)
					g.RemoveEdge(e)
					g.AddEdge(p, c)
				}

				// also unwire the sharder from the base
				e := g.FindEdge(p, n)
				g.RemoveEdge(e)

				// NOTE: we can't remove nodes from the graph, because our graph implementation doesn't
				// guarantee that NodeIndexes are stable when nodes are removed from the graph.
				graphN.Remove()
				gone[n] = struct{}{}
				continue
			}

			srcCols := graphP.ParentColumns(col)
			if len(srcCols) != 1 {
				// TODO: technically we could push the sharder to all parents here
				continue
			}

			grandp, srcCol := srcCols[0].Node, srcCols[0].Column
			if srcCol == -1 {
				// we can't shard a node by a column it generates
				continue
			}

			// we now know that we have the following
			//
			//    grandp[src_col] -> p[col] -> n[col] ---> nchildren[][]
			//                       :
			//                       +----> pchildren[col][]
			//
			// we want to move the sharder to "before" p.
			// this requires us to:
			//
			//  - rewire all nchildren to refer to p instead of n
			//  - rewire p so that it refers to n instead of grandp
			//  - remove any pchildren that also shard p by the same key
			//  - mark p as sharded
			//
			// there are some cases we need to look out for though. in particular, if any of n's
			// siblings (i.e., pchildren) do *not* have a sharder, we can't lift n!
			var remove []graph.NodeIdx
			it := g.NeighborsDirected(p, graph.DirectionOutgoing)
			for it.Next() {
				c := it.Current
				shrd := g.Value(c).AsSharder()
				if shrd == nil {
					// TODO: we *could* insert a de-shard here
					continue sharders
				}
				csharding := boostpb.NewShardingByColumn(shrd.ShardedBy(), shardingFactor)
				if csharding == by {
					remove = append(remove, c)
				}
				// sharding by a different key, which is okay
				// TODO: we have two sharders for different keys below p which should we shard p by?
			}

			// it is now safe to hoist the sharder

			// first, remove any sharders that are now unnecessary. unfortunately, we can't fully
			// remove nodes from the graph, because petgraph doesn't guarantee that NodeIndexes are
			// stable when nodes are removed from the graph.
			for _, c := range remove {
				e := g.FindEdge(p, c)
				g.RemoveEdge(e)

				grandc := g.NeighborsDirected(c, graph.DirectionOutgoing)
				for grandc.Next() {
					gc := grandc.Current
					e := g.FindEdge(c, gc)
					g.RemoveEdge(e)

					delete(swaps, graph.NodeIdxPair{One: gc, Two: p})
					g.AddEdge(p, gc)
				}

				if c != n {
					g.Value(c).Remove()
					gone[c] = struct{}{}
				}
			}

			realGrandp := grandp
			if currentGrandp, ok := swaps[graph.NodeIdxPair{One: p, Two: grandp}]; ok {
				// so, this is interesting... the parent of p has *already* been swapped, most
				// likely by another (hoisted) sharder. it doesn't really matter to us here, but we
				// will want to remove the duplication of sharders (whcih we'll do below).
				grandp = currentGrandp
			}

			newnode := g.Value(grandp).Mirror(flownode.NewSharder(srcCol))
			*g.Value(n) = *newnode

			e := g.FindEdge(grandp, p)
			g.RemoveEdge(e)
			g.AddEdge(grandp, n)
			g.AddEdge(n, p)

			delete(swaps, graph.NodeIdxPair{One: p, Two: grandp})
			swaps[graph.NodeIdxPair{One: p, Two: realGrandp}] = n

			g.Value(p).ShardBy(by)
			newSharders = append(newSharders, n)
		}
	}

	// and finally, because we don't *currently* support sharded shuffles (i.e., going directly
	// from one sharding to another), we replace such patterns with a merge + a shuffle. the merge
	// will ensure that replays from the first sharding are turned into a single update before
	// arriving at the second sharding, and the merged sharder will ensure that nshards is set
	// correctly.
	var shardedSharders []graph.NodeIdx
	for n := range new {
		graphN := g.Value(n)
		if graphN.IsSharder() && !graphN.Sharding().IsNone() {
			shardedSharders = append(shardedSharders, n)
		}
	}

	for _, n := range shardedSharders {
		p := g.NeighborsDirected(n, graph.DirectionIncoming).First()
		mig.log.Error("preventing unsupported sharded shuffle", n.Zap())

		mig.reshard(new, swaps, p, n, boostpb.NewShardingForcedNone())
		g.Value(n).ShardBy(boostpb.NewShardingForcedNone())
	}

	var finalTopoList []graph.NodeIdx
	topo := graph.NewTopoVisitor(g)
	for topo.Next() {
		node := topo.Current
		graphNode := g.Value(node)
		if graphNode.IsSource() || graphNode.IsDropped() {
			continue
		}
		if !new[node] {
			continue
		}
		finalTopoList = append(finalTopoList, node)
	}
	if err := mig.validateSharding(finalTopoList, shardingFactor); err != nil {
		return nil, nil, err
	}
	return finalTopoList, swaps, nil
}

func (mig *migration) reshard(new map[graph.NodeIdx]bool, swaps map[graph.NodeIdxPair]graph.NodeIdx, src graph.NodeIdx, dst graph.NodeIdx, to boostpb.Sharding) {
	g := mig.target.ingredients
	graphSrc := g.Value(src)

	if graphSrc.Sharding().IsNone() && to.IsNone() {
		return
	}

	var node *flownode.Node
	if to.IsNone() {
		// NOTE: this *must* be a union so that we correctly buffer partial replays
		op := flownode.NewUnionDeshard(src, graphSrc.Sharding())
		node = graphSrc.Mirror(op)
		node.ShardBy(to)
	} else if c, _, ok := to.ByColumn(); ok {
		node = graphSrc.Mirror(flownode.NewSharder(c))
		node.ShardBy(graphSrc.Sharding())
	} else {
		panic("unexpected reshard node")
	}

	mig.log.Debug("told to shuffle", src.Zap(), dst.Zap())

	nodeIdx := g.AddNode(node)
	new[nodeIdx] = true

	// TODO: if there is already sharder child of src with the right sharding target,
	// just add us as a child of that node!

	// hook in node that does appropriate shuffle
	old := g.FindEdge(src, dst)
	g.RemoveEdge(old)
	g.AddEdge(src, nodeIdx)
	g.AddEdge(nodeIdx, dst)

	swaps[graph.NodeIdxPair{One: dst, Two: src}] = nodeIdx
}

func (mig *migration) validateSharding(topoList []graph.NodeIdx, shardingFactor uint) error {
	g := mig.target.ingredients

	// ensure that each node matches the sharding of each of its ancestors, unless the ancestor is
	// a sharder or a shard merger
	for _, node := range topoList {
		n := g.Value(node)
		if n.IsInternal() && n.IsShardMerger() {
			// shard mergers legitimately have a different sharding than their ancestors
			continue
		}

		var inputs []graph.NodeIdx
		it := g.NeighborsDirected(node, graph.DirectionIncoming)
		for it.Next() {
			if !g.Value(it.Current).IsSource() {
				inputs = append(inputs, it.Current)
			}
		}

		remap := func(nd *flownode.Node, pni graph.NodeIdx, ps boostpb.Sharding) boostpb.Sharding {
			if nd.IsInternal() || nd.IsAnyBase() {
				if c, shards, ok := ps.ByColumn(); ok {
					var maxcol = len(nd.Fields())
					for col := 0; col < maxcol; col++ {
						for _, pc := range nd.ParentColumns(col) {
							if pc.Column != -1 {
								if pc.Node == pni && pc.Column == c {
									return boostpb.NewShardingByColumn(col, shards)
								}
								if !g.Value(pni).IsInternal() {
									if graph.HasPathConnecting(g, pc.Node, pni) && pc.Column == c {
										return boostpb.NewShardingByColumn(col, shards)
									}
								}
							}
						}
					}
					return boostpb.NewShardingRandom(shards)
				}
			}
			return ps
		}

		for _, inni := range inputs {
			inNode := g.Value(inni)
			if sharder := inNode.AsSharder(); sharder != nil {
				inSharding := remap(n, inni, boostpb.NewShardingByColumn(sharder.ShardedBy(), shardingFactor))
				if inSharding != n.Sharding() {
					return fmt.Errorf("invalid sharding: %d shards to %s; %d shards to %s",
						inni, inSharding.DebugString(), node, n.Sharding().DebugString(),
					)
				}

			} else {
				inSharding := remap(n, inni, inNode.Sharding())
				outSharding := n.Sharding()
				var equal bool
				if inSharding.IsNone() {
					equal = outSharding.IsNone()
				} else {
					equal = inSharding == outSharding
				}
				if !equal {
					return fmt.Errorf("invalid sharding: %d shards to %s; %d shards to %s",
						inni, inSharding.DebugString(), node, n.Sharding().DebugString(),
					)
				}
			}
		}
	}

	return nil
}
