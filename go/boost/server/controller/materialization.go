package controller

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
)

type Materialization struct {
	have  indexmap
	added indexmap

	state            map[graph.NodeIdx]*boostpb.Packet_PrepareState
	partial          map[graph.NodeIdx]bool
	partialEnabled   bool
	frontierStrategy *boostpb.FrontierStrategy

	tagGenerator uint32 // atomic
}

func NewMaterialization() *Materialization {
	return &Materialization{
		have:           newIndexmap(),
		added:          newIndexmap(),
		state:          make(map[graph.NodeIdx]*boostpb.Packet_PrepareState),
		partial:        make(map[graph.NodeIdx]bool),
		partialEnabled: true,
		frontierStrategy: &boostpb.FrontierStrategy{
			Type:  boostpb.FrontierStrategyType_NONE,
			Match: "",
		},
	}
}

func (mat *Materialization) DisablePartial() {
	mat.partialEnabled = false
}

func (mat *Materialization) SetFrontierStrategy(strat *boostpb.FrontierStrategy) {
	mat.frontierStrategy = strat
}

func indicesEqual(a, b [][]int) bool {
	return slices.EqualFunc(a, b, func(a, b []int) bool { return slices.Equal(a, b) })
}

type indexmap struct {
	m map[graph.NodeIdx][][]int
}

func newIndexmap() indexmap {
	return indexmap{m: make(map[graph.NodeIdx][][]int)}
}

func (o *indexmap) insert(idx graph.NodeIdx, cols []int) bool {
	existing := o.m[idx]
	for _, cc := range existing {
		if slices.Equal(cc, cols) {
			return false
		}
	}
	existing = append(existing, cols)
	o.m[idx] = existing
	return true
}

func (o *indexmap) contains(idx graph.NodeIdx) bool {
	_, ok := o.m[idx]
	return ok
}

func (o *indexmap) get(idx graph.NodeIdx) (cols [][]int, ok bool) {
	cols, ok = o.m[idx]
	return
}

func (o *indexmap) clear() {
	o.m = make(map[graph.NodeIdx][][]int)
}

func (o *indexmap) remove(idx graph.NodeIdx) [][]int {
	cols, ok := o.m[idx]
	if ok {
		delete(o.m, idx)
	}
	return cols
}

func (o *indexmap) indexLen(idx graph.NodeIdx) int {
	return len(o.m[idx])
}

// Extend the current set of materializations with any additional materializations needed to
// satisfy indexing obligations in the given set of (new) nodes.
func (mat *Materialization) extend(g *graph.Graph[*flownode.Node], newnodes map[graph.NodeIdx]bool) error {
	// this code used to be a mess, and will likely be a mess this time around too.
	// but, let's try to start out in a principled way...
	//
	// we have a bunch of known existing materializations (self.have), and potentially a set of
	// newly added, but not yet constructed, materializations (self.added). Everything in
	// self.added is also in self.have. We're now being asked to compute any indexing
	// obligations created by the nodes in `nodes`, some of which may be new (iff the boolean
	// is true). `extend` will be called once per new domain, so it will be called several
	// times before `commit` is ultimately called to create the new materializations.
	//
	// There are multiple ways in which an indexing obligation can be created:
	//
	//  - a node can ask for its own state to be materialized
	//  - a node can indicate that it will perform lookups on its ancestors
	//  - a node can declare that it would benefit from an ancestor index for replays
	//
	// The last point is special, in that those indexes can be hoisted past *all* nodes,
	// including across domain boundaries. We call these "replay obligations". They are also
	// special in that they also need to be carried along all the way to the nearest *full*
	// materialization.
	//
	// In the first case, the materialization decision is easy: we materialize the node in
	// question. In the latter case, it is a bit more complex, since the parent may be in a
	// different domain, or may be a "query through" node that we want to avoid materializing.
	//
	// Computing indexing obligations is therefore a multi-stage process.
	//
	//  1. Compute what indexes each *new* operator requires.
	//  2. Add materializations for any lookup obligations, considering query-through.
	//  3. Recursively add indexes for replay obligations.
	//

	// Holds all lookup obligations. Keyed by the node that should be materialized.
	var lookupObligations = newIndexmap()

	// Holds all replay obligations. Keyed by the node whose *parent* should be materialized.
	var replayObligations = newIndexmap()

	// Find indices we need to add.
	for ni := range newnodes {
		n := g.Value(ni)

		type coltuple struct {
			cols   []int
			lookup bool
		}
		var indices = make(map[graph.NodeIdx]coltuple)

		if reader := n.AsReader(); reader != nil {
			key := reader.Key()
			if key == nil {
				// only streaming, no indexing needed
				continue
			}

			// for a reader that will get lookups, we'd like to have an index above us
			// somewhere on our key so that we can make the reader partial
			indices[ni] = coltuple{key, false}
		} else {
			for k, c := range n.SuggestIndexes(ni) {
				indices[k] = coltuple{c, true}
			}
		}

		if len(indices) == 0 && n.IsBase() {
			// we must *always* materialize base nodes
			// so, just make up some column to index on
			indices[ni] = coltuple{[]int{0}, true}
		}

		for ni, ct := range indices {
			if ct.lookup {
				lookupObligations.insert(ni, ct.cols)
			} else {
				replayObligations.insert(ni, ct.cols)
			}
		}
	}

	mapIndices := func(n *flownode.Node, parent graph.NodeIdx, indices [][]int) error {
		for _, index := range indices {
			for c, col := range index {
				if !n.IsInternal() {
					if n.IsAnyBase() {
						panic("???")
					}
					continue
				}

				var rewritten bool
				for _, pc := range n.ParentColumns(col) {
					if pc.Node == parent {
						index[c] = pc.Column
						rewritten = true
						break
					}
				}

				if !rewritten {
					return fmt.Errorf(
						"could not resolve obligation past operator; node: %v, ancestor: %v, column: %v",
						n, parent, col)
				}
			}
		}
		return nil
	}

	// lookup obligations are fairly rigid, in that they require a materialization, and can
	// only be pushed through query-through nodes, and never across domains. so, we deal with
	// those first.
	//
	// it's also *important* that we do these first, because these are the only ones that can
	// force non-materialized nodes to become materialized. if we didn't do this first, a
	// partial node may add indices to only a subset of the intermediate partial views between
	// it and the nearest full materialization (because the intermediate ones haven't been
	// marked as materialized yet).
	for ni, indices := range lookupObligations.m {
		// we want to find the closest materialization that allows lookups (i.e., counting
		// query-through operators).
		mi := ni
		m := g.Value(mi)

		for {
			if mat.have.contains(mi) {
				break
			}
			if !m.IsInternal() || !m.CanQueryThrough() {
				break
			}

			parents := g.NeighborsDirected(mi, graph.DirectionIncoming).Collect(nil)
			if len(parents) != 1 {
				panic("query_through had more than one ancestor")
			}

			// hoist index to parent
			mi = parents[0]
			if err := mapIndices(m, mi, indices); err != nil {
				return err
			}
			m = g.Value(mi)
		}

		for _, columns := range indices {
			if mat.have.insert(mi, columns) {
				replayObligations.insert(mi, columns)
				mat.added.insert(mi, columns)
			}
		}
	}

	// we need to compute which views can be partial, and which can not.
	// in addition, we need to figure out what indexes each view should have.
	// this is surprisingly difficult to get right.
	//
	// the approach we are going to take is to require walking the graph bottom-up:

	var ordered = make([]graph.NodeIdx, 0, g.NodeCount())
	var topo = graph.NewTopoVisitor(g)

	for topo.Next(g) {
		n := g.Value(topo.Current)
		if n.IsSource() {
			continue
		}
		if n.IsDropped() {
			continue
		}
		ordered = append(ordered, topo.Current)
	}

	// for each node, we will check if it has any *new* indexes (i.e., in self.added).
	// if it does, see if the indexed columns resolve into its nearest ancestor
	// materializations. if they do, we mark this view as partial. if not, we, well, don't.
	// if the view was marked as partial, we add the necessary indexes to self.added for the
	// parent views, and keep walking. this is the reason we need the reverse topological
	// order: if we didn't, a node could receive additional indexes after we've checked it!
	for i := len(ordered) - 1; i >= 0; i-- {
		ni := ordered[i]
		indexes, ok := replayObligations.get(ni)
		if !ok {
			continue
		}

		// we want to find out if it's possible to partially materialize this node. for that to
		// be the case, we need to keep moving up the ancestor tree of `ni`, and check at each
		// stage that we can trace the key column back into each of our nearest
		// materializations.
		var able = mat.partialEnabled
		var add = newIndexmap()

		nn := g.Value(ni)
		if nn.IsAnyBase() {
			able = false
		}
		if nn.IsInternal() && nn.RequiresFullMaterialization() {
			able = false
		}

		// we are already fully materialized, so can't be made partial
		if !newnodes[ni] && !mat.partial[ni] && mat.added.indexLen(ni) != mat.have.indexLen(ni) {
			able = false
		}

		// do we have a full materialization below us?
		var stack = g.NeighborsDirected(ni, graph.DirectionOutgoing).Collect(nil)
		for len(stack) > 0 {
			child := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			childN := g.Value(child)

			// allow views to force full
			if strings.HasPrefix(childN.Name, "FULL_") {
				stack = stack[:0]
				able = false
			}

			if mat.have.contains(child) {
				// materialized child -- don't need to keep walking along this path
				if !mat.partial[child] {
					stack = stack[:0]
					able = false
				}
			} else if childN.IsReader() && childN.AsReader().Key() != nil {
				// reader child (which is effectively materialized)
				if !mat.partial[child] {
					stack = stack[:0]
					able = false
				}
			} else {
				stack = g.NeighborsDirected(child, graph.DirectionOutgoing).Collect(stack)
			}
		}

	attempt:
		for _, index := range indexes {
			if !able {
				break
			}

			paths := ProvenanceOf(g, ni, index, materializationPlanOnJoin(g))
			for _, path := range paths {
				for _, pe := range path[1:] {
					if p := slices.Index(pe.Columns, -1); p >= 0 {
						able = false
						break attempt
					}

					if m, ok := mat.have.get(pe.Node); ok {
						if slices.IndexFunc(m, func(cc []int) bool { return slices.Equal(cc, pe.Columns) }) < 0 {
							add.insert(pe.Node, pe.Columns)
						}
						break
					}
				}
			}
		}

		if able {
			// we can do partial if we add all those indices
			mat.partial[ni] = true
			for mi, indices := range add.m {
				for _, index := range indices {
					replayObligations.insert(mi, index)
				}
			}
		} else {
			if g.Value(ni).Purge {
				panic("full materialization placed beyond materialization frontier")
			}
		}

		// no matter what happens, we're going to have to fulfill our replay obligations.
		if mat.have.contains(ni) {
			for _, index := range indexes {
				newIndex := mat.have.insert(ni, index)
				if newIndex || mat.partial[ni] {
					// we need to add to self.added even if we didn't explicitly add any new
					// indices if we're partial, because existing domains will need to be told
					// about new partial replay paths sourced from this node.
					mat.added.insert(ni, index)
				}
			}
		} else {
			if !g.Value(ni).IsReader() {
				panic("expected a Reader node")
			}
		}
	}

	return nil
}

func containsColumn(cols []int, col int) bool {
	for _, c := range cols {
		if c == col {
			return true
		}
	}
	return false
}

// Commit commits all materialization decisions since the last time `commit` was called.
// This includes setting up replay paths, adding new indices to existing materializations, and
// populating new materializations
func (mat *Materialization) Commit(
	ctx context.Context,
	g *graph.Graph[*flownode.Node],
	newnodes map[graph.NodeIdx]bool,
	domains map[boostpb.DomainIndex]*DomainHandle,
	workers map[WorkerID]*Worker,
) error {
	log := common.Logger(ctx)
	if err := mat.extend(g, newnodes); err != nil {
		return err
	}

	// check that we don't have fully materialized nodes downstream of partially materialized nodes.
	{
		var anyPartial func(ni graph.NodeIdx) graph.NodeIdx
		anyPartial = func(ni graph.NodeIdx) graph.NodeIdx {
			if mat.partial[ni] {
				return ni
			}
			neighbors := g.NeighborsDirected(ni, graph.DirectionIncoming)
			for neighbors.Next() {
				if ni := anyPartial(neighbors.Current); ni != graph.InvalidNode {
					return ni
				}
			}
			return graph.InvalidNode
		}

		for ni := range mat.added.m {
			if mat.partial[ni] {
				continue
			}
			if pi := anyPartial(ni); pi != graph.InvalidNode {
				panic("partial materialization above full materialization")
			}
		}
	}

	// Mark nodes as beyond the frontier as dictated by the strategy
	for ni := range newnodes {
		n := g.Value(ni)

		if (mat.have.contains(ni) || n.IsReader()) && !mat.partial[ni] {
			// full materializations cannot be beyond the frontier.
			continue
		}

		// Normally, we only mark things that are materialized as .purge, but when it comes to
		// name matching, we don't do that since MIR will sometimes place the name of identity
		// nodes and the like. It's up to the user to make sure they don't match node names
		// that are, say, above a full materialization.
		if mat.frontierStrategy.Type == boostpb.FrontierStrategyType_MATCH {
			n.Purge = n.Purge || strings.Contains(n.Name, mat.frontierStrategy.Match)
			continue
		}
		if strings.HasPrefix(n.Name, "SHALLOW_") {
			n.Purge = true
			continue
		}

		// For all other strategies, we only want to deal with partial indices
		if !mat.partial[ni] {
			continue
		}

		switch mat.frontierStrategy.Type {
		case boostpb.FrontierStrategyType_ALL_PARTIAL:
			n.Purge = true
		case boostpb.FrontierStrategyType_READERS:
			n.Purge = n.Purge || n.IsReader()
		}
	}

	// check that no node is partial over a subset of the indices in its parent
	{
		for ni, added := range mat.added.m {
			if !mat.partial[ni] {
				continue
			}

			for _, index := range added {
				paths := ProvenanceOf(g, ni, index, materializationPlanOnJoin(g))
				for _, path := range paths {
					for _, pe := range path {
						pni := pe.Node
						columns := pe.Columns

						if slices.Contains(columns, -1) {
							break
						} else if mat.partial[pni] {
							indexes, ok := mat.have.get(pni)
							if !ok {
								break
							}
							for _, index := range indexes {
								overlap := false
								for _, col := range index {
									if containsColumn(columns, col) {
										overlap = true
										break
									}
								}
								if !overlap {
									break
								}

								for _, col := range columns {
									if !containsColumn(index, col) {
										panic("unimplemented")
									}
								}
							}
						} else if mat.have.contains(ni) {
							break
						}
					}
				}
			}
		}
	}

	// check that we don't have any cases where a subgraph is sharded by one column, and then
	// has a replay path on a duplicated copy of that column. for example, a join with
	// [B(0, 0), R(0)] where the join's subgraph is sharded by .0, but a downstream replay path
	// looks up by .1. this causes terrible confusion where the target (correctly) queries only
	// one shard, but the shard merger expects to have to wait for all shards (since the replay
	// key and the sharding key do not match at the shard merger).
	{
		for node := range newnodes {
			n := g.Value(node)
			if !n.IsShardMerger() {
				continue
			}

			// we don't actually store replay paths anywhere in Materializations (perhaps we
			// should). however, we can check a proxy for the necessary property by making sure
			// that our parent's sharding key is never aliased. this will lead to some false
			// positives (all replay paths may use the same alias as we shard by), but we'll
			// deal with that.
			parent := g.NeighborsDirected(node, graph.DirectionIncoming).First()
			psharding := g.Value(parent).Sharding()
			if col, _, ok := psharding.ByColumn(); ok {
				// we want to resolve col all the way to its nearest materialized ancestor.
				// and then check whether any other cols of the parent alias that source column
				columns := make([]int, len(n.Fields()))
				for c := range columns {
					columns[c] = c
				}

				paths := ProvenanceOf(g, parent, columns, OnJoinNone)
				for _, path := range paths {
					m := 0
					for m < len(path) {
						if mat.have.contains(path[m].Node) {
							break
						}
						m++
					}
					cols := path[m].Columns
					src := cols[col]
					if src == -1 {
						continue
					}

					for c, res := range cols {
						if c != col && res == src {
							panic("attempting to merge sharding by aliased column")
						}
					}
				}
			}
		}
	}

	for ni := range newnodes {
		// any nodes marked as .purge should have their state be beyond the materialization
		// frontier. however, mir may have named an identity child instead of the node with a
		// materialization, so let's make sure the label gets correctly applied: specifically,
		// if a .prune node doesn't have state, we "move" that .prune to its ancestors.

		n := g.Value(ni)
		if n.Purge && !(mat.have.contains(ni) || n.IsReader()) {
			it := g.NeighborsDirected(ni, graph.DirectionIncoming)
			for it.Next() {
				if !newnodes[it.Current] {
					continue
				}
				if !mat.have.contains(it.Current) {
					log.Warn("no associated state with purged node", it.Current.Zap())
					continue
				}
				g.Value(it.Current).Purge = true
			}
		}
	}

	var nonPurge []graph.NodeIdx
	for ni := range newnodes {
		n := g.Value(ni)
		if (n.IsReader() || mat.have.contains(ni)) && !n.Purge {
			nonPurge = g.NeighborsDirected(ni, graph.DirectionIncoming).Collect(nonPurge)
		}
	}
	for len(nonPurge) > 0 {
		ni := nonPurge[len(nonPurge)-1]
		nonPurge = nonPurge[:len(nonPurge)-1]

		if g.Value(ni).Purge {
			panic("found purge node above non-purge node")
		}
		if mat.have.contains(ni) {
			// already shceduled to be checked
			// NOTE: no need to check for readers here, since they can't be parents
			continue
		}
		nonPurge = g.NeighborsDirected(ni, graph.DirectionIncoming).Collect(nonPurge)
	}

	var reindex = make([]graph.NodeIdx, 0, len(newnodes))
	var mknodes = make([]graph.NodeIdx, 0, len(newnodes))
	var topo = graph.NewTopoVisitor(g)

	for topo.Next(g) {
		n := g.Value(topo.Current)
		if n.IsSource() || n.IsDropped() {
			continue
		}
		if newnodes[topo.Current] {
			mknodes = append(mknodes, topo.Current)
		} else if mat.added.contains(topo.Current) {
			reindex = append(reindex, topo.Current)
		}
	}

	// first, we add any new indices to existing nodes
	for _, node := range reindex {
		indexOn := mat.added.remove(node)

		// are they trying to make a non-materialized node materialized?
		if indicesEqual(mat.have.m[node], indexOn) {
			if mat.partial[node] {
				// we can't make this node partial if any of its children are materialized, as
				// we might stop forwarding updates to them, which would make them very sad.
				//
				// the exception to this is for new children, or old children that are now
				// becoming materialized; those are necessarily empty, and so we won't be
				// violating key monotonicity.

				var stack = g.NeighborsDirected(node, graph.DirectionOutgoing).Collect(nil)
				for len(stack) > 0 {
					child := stack[len(stack)-1]
					stack = stack[:len(stack)-1]

					if newnodes[child] {
						// NOTE: no need to check its children either
						continue
					}

					if mat.added.indexLen(child) != mat.have.indexLen(child) {
						panic("node was previously materialized!")
					}

					stack = g.NeighborsDirected(child, graph.DirectionOutgoing).Collect(stack)
				}
			}
		}

		n := g.Value(node)
		if mat.partial[node] {
			if err := mat.setup(ctx, node, indexOn, g, domains, workers); err != nil {
				return err
			}
		} else {
			var indexProto []*boostpb.Packet_PrepareState_IndexedLocal_Index
			for _, idx := range indexOn {
				indexProto = append(indexProto, &boostpb.Packet_PrepareState_IndexedLocal_Index{Key: idx})
			}

			var pkt boostpb.Packet
			pkt.Inner = &boostpb.Packet_PrepareState_{
				PrepareState: &boostpb.Packet_PrepareState{
					Node: n.LocalAddr(),
					State: &boostpb.Packet_PrepareState_IndexedLocal_{
						IndexedLocal: &boostpb.Packet_PrepareState_IndexedLocal{
							Index: indexProto,
						},
					},
				},
			}
			if err := domains[n.Domain()].SendToHealthy(ctx, &pkt, workers); err != nil {
				return err
			}
		}
	}

	for _, ni := range mknodes {
		var err error

		n := g.Value(ni)
		indexOn := mat.added.remove(ni)
		indexOn, err = mat.readyOne(ctx, ni, indexOn, g, domains, workers)
		if err != nil {
			return err
		}

		// communicate to the domain in charge of a particular node that it should start
		// delivering updates to a given new node. note that we wait for the domain to
		// acknowledge the change. this is important so that we don't ready a child in a
		// different domain before the parent has been readied. it's also important to avoid us
		// returning before the graph is actually fully operational.

		var indexProto []*boostpb.SyncPacket_Ready_Index
		for _, idx := range indexOn {
			indexProto = append(indexProto, &boostpb.SyncPacket_Ready_Index{Key: idx})
		}

		var pkt boostpb.SyncPacket
		pkt.Inner = &boostpb.SyncPacket_Ready_{
			Ready: &boostpb.SyncPacket_Ready{
				Node:  n.LocalAddr(),
				Purge: n.Purge,
				Index: indexProto,
			},
		}

		domain := domains[n.Domain()]
		if err := domain.SendToHealthySync(ctx, &pkt, workers); err != nil {
			return nil
		}
	}

	mat.added.clear()
	return nil
}

func (mat *Materialization) readyOne(ctx context.Context, ni graph.NodeIdx, indexOn [][]int, g *graph.Graph[*flownode.Node], domains map[boostpb.DomainIndex]*DomainHandle, workers map[WorkerID]*Worker) ([][]int, error) {
	log := common.Logger(ctx)
	n := g.Value(ni)
	hasState := len(indexOn) > 0

	if hasState {
		if mat.partial[ni] {
			log.Debug("new partially materialized node", zap.Any("node", n))
		} else {
			log.Debug("new fully-materialized node", zap.Any("node", n))
		}
	} else {
		log.Debug("new stateless node", zap.Any("node", n))
	}

	if n.IsAnyBase() {
		log.Debug("no need to replay empty new base", ni.Zap())
		return indexOn, nil
	}

	if r := n.AsReader(); r != nil {
		if r.IsMaterialized() {
			hasState = true
		}
	}
	if !hasState {
		log.Debug("no need to replay non-materialized view", ni.Zap())
		return indexOn, nil
	}

	log.Debug("beginning node reconstruction", ni.Zap())
	// NOTE: the state has already been marked ready by the replay completing, but we want to
	// wait for the domain to finish replay, which the ready executed by the outer commit()
	// loop does.
	err := mat.setup(ctx, ni, indexOn, g, domains, workers)
	return nil, err
}

func (mat *Materialization) setup(ctx context.Context, ni graph.NodeIdx, indexOn [][]int, g *graph.Graph[*flownode.Node], domains map[boostpb.DomainIndex]*DomainHandle, workers map[WorkerID]*Worker) error {
	log := common.Logger(ctx)

	if len(indexOn) == 0 {
		// we must be reconstructing a Reader.
		// figure out what key that Reader is using
		reader := g.Value(ni).AsReader()
		if reader == nil || !reader.IsMaterialized() {
			panic("expected to have a materialized reader")
		}
		if rh := reader.Key(); rh != nil {
			indexOn = append(indexOn, rh)
		}
	}

	plan := newMaterializationPlan(mat, g, ni, domains, workers)
	for _, idx := range indexOn {
		if err := plan.add(ctx, idx); err != nil {
			return err
		}
	}
	state, pending, err := plan.finalize(ctx)
	if err != nil {
		return err
	}

	mat.state[ni] = state

	if len(pending) > 0 {
		for n, pending := range pending {
			var pkt boostpb.Packet
			pkt.Inner = &boostpb.Packet_StartReplay_{
				StartReplay: &boostpb.Packet_StartReplay{
					Tag:  pending.Tag,
					From: pending.Source,
				}}

			log.Debug("sending pending StartReplay", zap.Int("n", n), pending.Tag.Zap(), pending.Source.Zap(), pending.SourceDomain.Zap())
			if err := domains[pending.SourceDomain].SendToHealthy(ctx, &pkt, workers); err != nil {
				return err
			}
		}

		// FIXME@vmg: this is Noria's behavior, where it just asks the target domain for _a_ replay to finish.
		// 		ideally we would ask the target domain to wait for a _specific replay tag_ to finish; our domain
		//		is capable of waiting on specific tags to finish, but we don't really know what's going to be the
		//		final tag for our replay once it reaches the target domain so we cannot explicitly wait for it.
		var pkt = &boostpb.SyncPacket{Inner: &boostpb.SyncPacket_WaitForReplay{}}
		var finishTarget = g.Value(ni).Domain()
		log.Debug("waiting for WaitForReplay", finishTarget.Zap())
		if err := domains[finishTarget].SendToHealthySync(ctx, pkt, workers); err != nil {
			return err
		}
	}
	return nil
}

func (mat *Materialization) nextTag() boostpb.Tag {
	return boostpb.Tag(atomic.AddUint32(&mat.tagGenerator, 1) - 1)
}

func (mat *Materialization) GetStatus(node *flownode.Node) boostpb.MaterializationStatus {
	var idx = node.GlobalAddr()
	var isMaterialized bool
	switch {
	case mat.have.contains(idx):
		isMaterialized = true
	case node.IsReader():
		isMaterialized = node.AsReader().IsMaterialized()
	}

	if !isMaterialized {
		return boostpb.MaterializationNone
	} else if mat.partial[idx] {
		return boostpb.MaterializationPartial
	} else {
		return boostpb.MaterializationFull
	}
}
