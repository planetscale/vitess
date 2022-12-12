package materialization

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common/graphviz"
	"vitess.io/vitess/go/boost/common/xslice"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Migration interface {
	context.Context

	Log() *zap.Logger
	Graph() *graph.Graph[*flownode.Node]
	SendPacket(domain boostpb.DomainIndex, b *boostpb.Packet) error
	SendPacketSync(domain boostpb.DomainIndex, b *boostpb.SyncPacket) error
	DomainShards(domain boostpb.DomainIndex) uint
	PlannedUpquery(node graph.NodeIdx) (sqlparser.SelectStatement, bool)
}

type Materialization struct {
	have  graphIndexMap
	added graphIndexMap

	plans          map[graph.NodeIdx]*Plan
	partial        map[graph.NodeIdx]bool
	partialEnabled bool

	tagGenerator uint32 // atomic
}

func NewMaterialization() *Materialization {
	return &Materialization{
		have:           make(graphIndexMap),
		added:          make(graphIndexMap),
		plans:          make(map[graph.NodeIdx]*Plan),
		partial:        make(map[graph.NodeIdx]bool),
		partialEnabled: true,
	}
}

func (mat *Materialization) DisablePartial() {
	mat.partialEnabled = false
}

type indexSet [][]int

func (idx indexSet) equal(other indexSet) bool {
	return slices.EqualFunc(idx, other, func(a, b []int) bool {
		return slices.Equal(a, b)
	})
}
func (idx indexSet) contains(cols []int) bool {
	for _, cc := range idx {
		if slices.Equal(cc, cols) {
			return true
		}
	}
	return false
}

func (idx indexSet) insert(cols []int) (indexSet, bool) {
	if idx.contains(cols) {
		return idx, false
	}
	return append(idx, cols), true
}

type graphIndexMap map[graph.NodeIdx]indexSet

func (im graphIndexMap) insert(idx graph.NodeIdx, cols []int) (inserted bool) {
	im[idx], inserted = im[idx].insert(cols)
	return
}

func (im graphIndexMap) clear() {
	for k := range im {
		delete(im, k)
	}
}

func (im graphIndexMap) take(idx graph.NodeIdx) indexSet {
	cols, ok := im[idx]
	if ok {
		delete(im, idx)
	}
	return cols
}

func (im graphIndexMap) contains(n graph.NodeIdx) bool {
	_, found := im[n]
	return found
}

func mapIndices(n *flownode.Node, parent graph.NodeIdx, indices [][]int) error {
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
				return &ObligationResolveError{
					Node:       n.GlobalAddr(),
					ParentNode: parent,
					Column:     col,
				}
			}
		}
	}
	return nil
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
	var lookupObligations = make(graphIndexMap)

	// Holds all replay obligations. Keyed by the node whose *parent* should be materialized.
	var replayObligations = make(graphIndexMap)

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

	// lookup obligations are fairly rigid, in that they require a materialization, and can
	// only be pushed through query-through nodes, and never across domains. so, we deal with
	// those first.
	//
	// it's also *important* that we do these first, because these are the only ones that can
	// force non-materialized nodes to become materialized. if we didn't do this first, a
	// partial node may add indices to only a subset of the intermediate partial views between
	// it and the nearest full materialization (because the intermediate ones haven't been
	// marked as materialized yet).
	for ni, indices := range lookupObligations {
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

	for topo.Next() {
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
		indexes, ok := replayObligations[ni]
		if !ok {
			continue
		}

		// we want to find out if it's possible to partially materialize this node. for that to
		// be the case, we need to keep moving up the ancestor tree of `ni`, and check at each
		// stage that we can trace the key column back into each of our nearest
		// materializations.
		var able = mat.partialEnabled
		var add = make(graphIndexMap)

		nn := g.Value(ni)
		if nn.IsAnyBase() {
			able = false
		}
		if nn.IsInternal() && nn.RequiresFullMaterialization() {
			able = false
		}

		// we are already fully materialized, so can't be made partial
		if !newnodes[ni] && !mat.partial[ni] && len(mat.added[ni]) != len(mat.have[ni]) {
			able = false
		}

		// do we have a full materialization below us?
		var stack = g.NeighborsDirected(ni, graph.DirectionOutgoing).Collect(nil)
		for len(stack) > 0 {
			child := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			childN := g.Value(child)

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

					if m, ok := mat.have[pe.Node]; ok {
						if !m.contains(pe.Columns) {
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
			for mi, indices := range add {
				for _, index := range indices {
					replayObligations.insert(mi, index)
				}
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

// Commit commits all materialization decisions since the last time `commit` was called.
// This includes setting up replay paths, adding new indices to existing materializations, and
// populating new materializations
func (mat *Materialization) Commit(mig Migration, newnodes map[graph.NodeIdx]bool) error {
	g := mig.Graph()

	if err := mat.extend(g, newnodes); err != nil {
		return err
	}

	if err := mat.checkMaterializationOrdering(g); err != nil {
		return err
	}

	if err := mat.checkPartialOverIndices(g); err != nil {
		return err
	}

	if err := mat.checkDuplicateColumns(g, newnodes); err != nil {
		return err
	}

	var (
		toReindex = make([]graph.NodeIdx, 0, len(newnodes))
		toMake    = make([]graph.NodeIdx, 0, len(newnodes))
		topo      = graph.NewTopoVisitor(g)
	)

	for topo.Next() {
		n := g.Value(topo.Current)
		if n.IsSource() || n.IsDropped() {
			continue
		}
		if newnodes[topo.Current] {
			toMake = append(toMake, topo.Current)
		} else if mat.added.contains(topo.Current) {
			toReindex = append(toReindex, topo.Current)
		}
	}

	if err := mat.reindexNodes(mig, newnodes, toReindex); err != nil {
		return err
	}

	if err := mat.makeNewNodes(mig, toMake); err != nil {
		return err
	}

	mat.added.clear()
	return nil
}

func (mat *Materialization) makeNewNodes(mig Migration, toMake []graph.NodeIdx) error {
	g := mig.Graph()

	for _, ni := range toMake {
		var err error

		n := g.Value(ni)
		indexOn := mat.added.take(ni)
		indexOn, err = mat.readyOne(mig, ni, indexOn)
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
				Index: indexProto,
			},
		}

		if err := mig.SendPacketSync(n.Domain(), &pkt); err != nil {
			return err
		}
	}
	return nil
}

func (mat *Materialization) reindexNodes(mig Migration, newnodes map[graph.NodeIdx]bool, reindex []graph.NodeIdx) error {
	g := mig.Graph()

	// first, we add any new indices to existing nodes
	for _, node := range reindex {
		indexOn := mat.added.take(node)

		// are they trying to make a non-materialized node materialized?
		if mat.have[node].equal(indexOn) {
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

					if len(mat.added[child]) != len(mat.have[child]) {
						panic("node was previously materialized!")
					}

					stack = g.NeighborsDirected(child, graph.DirectionOutgoing).Collect(stack)
				}
			}
		}

		n := g.Value(node)
		if mat.partial[node] {
			if err := mat.setup(mig, node, indexOn); err != nil {
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
			if err := mig.SendPacket(n.Domain(), &pkt); err != nil {
				return err
			}
		}
	}
	return nil
}

// check that we don't have any cases where a subgraph is sharded by one column, and then
// has a replay path on a duplicated copy of that column. for example, a join with
// [B(0, 0), R(0)] where the join's subgraph is sharded by .0, but a downstream replay path
// looks up by .1. this causes terrible confusion where the target (correctly) queries only
// one shard, but the shard merger expects to have to wait for all shards (since the replay
// key and the sharding key do not match at the shard merger).
func (mat *Materialization) checkDuplicateColumns(g *graph.Graph[*flownode.Node], newnodes map[graph.NodeIdx]bool) error {
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
						return &MergeShardingByAliasedColumnError{
							Node:         node,
							ParentNode:   parent,
							SourceColumn: src,
						}
					}
				}
			}
		}
	}
	return nil
}

// check that no node is partial over a subset of the indices in its parent
func (mat *Materialization) checkPartialOverIndices(g *graph.Graph[*flownode.Node]) error {
	for ni, added := range mat.added {
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
						for _, index := range mat.have[pni] {
							// is this node partial over some of the child's partial
							// columns, but not others? if so, we run into really sad
							// situations where the parent could miss in its state despite
							// the child having state present for that key.

							if xslice.All(index, func(c int) bool { return !slices.Contains(columns, c) }) {
								continue
							}

							conflict, found := xslice.Find(index, func(c int) bool { return !slices.Contains(columns, c) })
							if !found {
								conflict, found = xslice.Find(columns, func(c int) bool { return !slices.Contains(index, c) })
							}
							if found {
								/*
									Check that we don't run into newfound bug

									Specifically, consider the case where you have an aggregation that
									groups by [0,1], and a downstream reader keyed by [0]. The reader
									will be partial, and add an index on [0] on the aggregation. When the
									reader misses, it will ask the aggregation for a replay of, say, ["7"].
									The aggregation will perform that replay, but when trying to process the
									response, it will miss when doing a self-lookup of ["7", x] for whatever
									value x happens to be in column 1 of the replay responses. This triggers
									a second round of (unnecessary) replays, which in and of itself causes a
									bunch of issues, but let's imagine that it works fine, and that both
									["7"] and ["7", x] is filled correctly in the aggregation. Now a write
									comes along for ["7", y]. The aggregation misses on its lookup, so the
									write is discarded. But the reader holds state for ["7"], so this
									violates key monotonicity!

									There are a couple of ways that this could be fixed. One idea is that
									the aggregation could evict ["7"] on miss, similar to what we currently
									do for joins. This would cause us to do a lot more replays, and will
									likely reveal a bunch of bugs with overlapping replays, but should be
									theoretically correct. An alternative, and more robust, idea is to to
									have Soup realize that [0,1] and [0] overlap, and that the replay of [0]
									actually fills *all* holes in [0,1].

									Separately, the *reason* why we run into this situation in the majority
									of cases is that Soup has to pull through columns for downstream views
									(e.g., consider the case where 0 = article id and 1 = title). In these
									cases, there is *really* just one group by key (article id), and the
									other column value is uniquely determined by the first. In this case,
									the aggregation operator (and other similar operators) could be modified
									to know about these kinds of "pull through" columns, and simply pick any
									1 value for a set of records sharing the same value for [0]. This is
									similar to what MySQL does when it is lax about not having to name all
									selected columns in a GROUP BY (PostgreSQL does not allow this).

									From: https://github.com/vmg/noria/commit/25e0be14c216b1289c15a5f731a2277736d5d12d
								*/
								return &PartialOverlappingPartialIndexesError{
									ParentNode: pni,
									Index:      index,
									ChildNode:  ni,
									Columns:    columns,
									Conflict:   conflict,
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
	return nil
}

// check that we don't have fully materialized nodes downstream of partially materialized nodes.
func (mat *Materialization) checkMaterializationOrdering(g *graph.Graph[*flownode.Node]) error {
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

	for ni := range mat.added {
		if mat.partial[ni] {
			continue
		}
		if pi := anyPartial(ni); pi != graph.InvalidNode {
			return &PartialMaterializationAboveFullMaterializationError{
				FullNode:    ni,
				PartialNode: pi,
			}
		}
	}
	return nil
}

func (mat *Materialization) readyOne(mig Migration, ni graph.NodeIdx, indexOn [][]int) ([][]int, error) {
	log := mig.Log().With(ni.Zap())
	n := mig.Graph().Value(ni)
	hasState := len(indexOn) > 0

	if hasState {
		if mat.partial[ni] {
			log.Debug("new partially materialized node")
		} else {
			log.Debug("new fully-materialized node")
		}
	} else {
		log.Debug("new stateless node")
	}

	if n.IsAnyBase() {
		log.Debug("no need to replay empty new base")
		return indexOn, nil
	}

	if r := n.AsReader(); r != nil {
		if r.IsMaterialized() {
			hasState = true
		}
	}
	if !hasState {
		log.Debug("no need to replay non-materialized view")
		return indexOn, nil
	}

	log.Debug("beginning node reconstruction")
	// NOTE: the state has already been marked ready by the replay completing, but we want to
	// wait for the domain to finish replay, which the ready executed by the outer commit()
	// loop does.
	err := mat.setup(mig, ni, indexOn)
	return nil, err
}

func (mat *Materialization) setup(mig Migration, ni graph.NodeIdx, indexOn [][]int) error {
	if len(indexOn) == 0 {
		// we must be reconstructing a Reader.
		// figure out what key that Reader is using
		reader := mig.Graph().Value(ni).AsReader()
		if reader == nil || !reader.IsMaterialized() {
			panic("expected to have a materialized reader")
		}
		if rh := reader.Key(); rh != nil {
			indexOn = append(indexOn, rh)
		}
	}

	plan := newMaterializationPlan(mat, ni)
	for _, idx := range indexOn {
		if err := plan.add(mig, idx); err != nil {
			return err
		}
	}
	pending, err := plan.finalize(mig)
	if err != nil {
		return err
	}

	mat.plans[ni] = plan

	if len(pending) > 0 {
		for n, pending := range pending {
			var pkt boostpb.Packet
			pkt.Inner = &boostpb.Packet_StartReplay_{
				StartReplay: &boostpb.Packet_StartReplay{
					Tag:  pending.tag,
					From: pending.source,
				}}

			mig.Log().Debug("sending pending StartReplay", zap.Int("n", n), pending.tag.Zap(), pending.source.Zap(), pending.sourceDomain.Zap())
			if err := mig.SendPacket(pending.sourceDomain, &pkt); err != nil {
				return err
			}
		}

		// FIXME@vmg: this is Noria's behavior, where it just asks the target domain for _a_ replay to finish.
		// 		ideally we would ask the target domain to wait for a _specific replay tag_ to finish; our domain
		//		is capable of waiting on specific tags to finish, but we don't really know what's going to be the
		//		final tag for our replay once it reaches the target domain so we cannot explicitly wait for it.
		var pkt = &boostpb.SyncPacket{Inner: &boostpb.SyncPacket_WaitForReplay{}}
		var finishTarget = mig.Graph().Value(ni).Domain()
		mig.Log().Debug("waiting for WaitForReplay", finishTarget.Zap())
		if err := mig.SendPacketSync(finishTarget, pkt); err != nil {
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

func (mat *Materialization) RenderGraphviz(gvz *graphviz.Graph[graph.NodeIdx]) {
	portLabels := []string{"w", "e"}
	portCount := 0

	for _, plan := range mat.plans {
		for tag, replayPath := range plan.paths {
			for _, seg := range replayPath {
				port := portLabels[portCount%2]
				portCount++

				setup := graphviz.JSON(seg.setup)
				color := fmt.Sprintf("/spectral11/%d", (tag%11)+1)

				addEdge := func(from, to PathElement) {
					edge := gvz.AddEdge(from.Node, to.Node)
					edge.Attr["xlabel"] = fmt.Sprintf("T%d (d%d, %v⇨%v)", tag, seg.domain, from.Columns, to.Columns)
					edge.Attr["color"] = color
					edge.Attr["fontcolor"] = color
					edge.Attr["headport"] = port
					edge.Attr["tailport"] = port
					edge.Attr["penwidth"] = "3"
					edge.Attr["labeltooltip"] = setup
				}

				if len(seg.path) == 1 {
					addEdge(seg.path[0], seg.path[0])
				}
				for e := 0; e < len(seg.path)-1; e++ {
					addEdge(seg.path[e], seg.path[e+1])
				}
			}
		}
	}
}
