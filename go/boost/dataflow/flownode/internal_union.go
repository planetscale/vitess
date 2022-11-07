package flownode

import (
	"math"
	"sort"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common/btree"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
)

var _ Internal = (*Union)(nil)
var _ ingredientInputRaw = (*Union)(nil)

type Union struct {
	emit         unionEmit
	replayKey    map[unionreplayKey][]int
	replayPieces *btree.BTreeG[*unionreplay] // *btree.[*unionreplay]
	required     int
	wait         *ongoingWait
	me           graph.NodeIdx
}

type unionreplayKey struct {
	tag  boostpb.Tag
	node boostpb.LocalNodeIndex
}

type unionreplay struct {
	// Key
	tag          boostpb.Tag
	replayingKey boostpb.Row
	shard        uint

	// Value
	buffered map[boostpb.LocalNodeIndex][]boostpb.Record
	evict    bool
}

func compareReplays(a, b *unionreplay) bool {
	if a.tag < b.tag {
		return true
	}
	if a.tag > b.tag {
		return false
	}
	if a.replayingKey < b.replayingKey {
		return true
	}
	if a.replayingKey > b.replayingKey {
		return false
	}
	return a.shard < b.shard
}

type ongoingWait struct {
	started  map[boostpb.LocalNodeIndex]bool
	finished int
	buffered []boostpb.Record
}

func (u *Union) OnInputRaw(ex processing.Executor, from boostpb.LocalNodeIndex, rs []boostpb.Record, rpl replay.Context, n *Map, s *state.Map) (processing.RawResult, error) {
	// NOTE: in the special case of us being a shard merge node (i.e., when
	// self.emit.is_empty()), `from` will *actually* hold the shard index of
	// the sharded egress that sent us this record. this should make everything
	// below just work out.

	switch {
	case rpl.Partial == nil && rpl.Full == nil:
		absorbForFull := false
		if ongoing := u.wait; ongoing != nil {
			// ongoing full replay. is this a record we need to not disappear (i.e.,
			// message 2 in the explanation)?
			if len(ongoing.started) != u.required && ongoing.started[from] {
				// yes! keep it.
				absorbForFull = true
			}
		}

		if absorbForFull {
			// we shouldn't be stepping on any partial materialization toes, but let's
			// make sure. i'm not 100% sure at this time if it's true.
			//
			// hello future self. clearly, i was correct that i might be incorrect. let me
			// help: the only reason this assert is here is because we consume rs so that
			// we can process it. the replay code below seems to require rs to be
			// *unprocessed* (not sure why), and so once we add it to our buffer, we can't
			// also execute the code below. if you fix that, you should be all good!
			//
			// TODO: why is this an || past self?

			rs, err := u.OnInput(nil, ex, from, rs, nil, n, s)
			if err != nil {
				return nil, err
			}
			u.wait.buffered = append(u.wait.buffered, rs.Records...)
			return &processing.Result{Records: rs.Records}, nil
		}

		if u.replayPieces.Len() == 0 {
			res, err := u.OnInput(nil, ex, from, rs, nil, n, s)
			if err != nil {
				return nil, err
			}
			return &res, nil
		}

		// partial replays are flowing through us, and at least one piece is being waited
		// for. we need to keep track of any records that succeed a replay piece (and thus
		// aren't included in it) before the other pieces come in. note that it's perfectly
		// safe for us to also forward them, since they'll just be dropped when they miss
		// in the downstream node. in fact, we *must* forward them, because there may be
		// *other* nodes downstream that do *not* have holes for the key in question.
		// TODO: is the *must* still true now that we take tags into account?
		//
		// unfortunately, finding out which things we need to merge is a bit of a pain,
		// since the buffered upquery responses may be for different upquery paths with
		// different key columns. in other words, for each record, we conceptually need to
		// check each buffered replay.
		//
		// we have two options here. either, we iterate over the records in an outer loop
		// and the buffered upquery responses in the inner loop, or the other way around.
		// since iterating over the buffered upquery responses includes a btree lookup, we
		// want to do fewer of those, so we do those in the outer loop.
		var (
			lastTag   = boostpb.TagInvalid
			replayKey []int
			rkeyFrom  boostpb.LocalNodeIndex
		)

		if _, all := u.emit.(*unionEmitAll); all {
			rkeyFrom = 0
		} else {
			rkeyFrom = from
		}

		u.replayPieces.Ascend(func(it *unionreplay) bool {
			// first, let's see if _any_ of the records in this batch even affect this
			// buffered upquery response.
			buffered, ok := it.buffered[from]
			if !ok {
				// we haven't received a replay piece for this key from this ancestor yet,
				// so we know that the eventual replay piece must include any records in
				// this batch.
				return true
			}

			// make sure we use the right key columns for this tag
			if lastTag == boostpb.TagInvalid || lastTag != it.tag {
				replayKey = u.replayKey[unionreplayKey{it.tag, rkeyFrom}]
			}
			k := replayKey
			lastTag = it.tag

			replayHit := func(k []int, r boostpb.Record, replayingKey []boostpb.Value) bool {
				keys := r.Row.ToValues()
				for ki, c := range k {
					if keys[c].Cmp(replayingKey[ki]) != 0 {
						return false
					}
				}
				return true
			}

			for _, r := range rs {
				if !replayHit(k, r, it.replayingKey.ToValues()) {
					continue
				}

				// we've received a replay piece from this ancestor already for this
				// key, and are waiting for replay pieces from other ancestors. we need
				// to incorporate this record into the replay piece so that it doesn't
				// end up getting lost.
				buffered = append(buffered, r)

				// it'd be nice if we could avoid doing this exact same key check multiple
				// times if the same key is being replayed by multiple `requesting_shard`s.
				// in theory, the btreemap could let us do this by walking forward in the
				// iterator until we hit the next key or tag, and the rewinding back to
				// where we were before continuing to the same record. but that won't work
				// because https://github.com/rust-lang/rfcs/pull/2896.
				//
				// we could emulate the same thing by changing `ReplayPieces` to
				// `RefCell<ReplayPieces>`, using an ref-only iterator that is `Clone`, and
				// then play some games from there, but it seems not worth it.
			}
			return true
		})

		// return a regular processing result
		res, err := u.OnInput(nil, ex, from, rs, nil, n, s)
		if err != nil {
			return nil, err
		}
		return &res, nil

	case rpl.Full != nil:
		// this part is actually surpringly straightforward, but the *reason* it is
		// straightforward is not. let's walk through what we know first:
		//
		//  - we know that there is only exactly one full replay going on
		//  - we know that the target domain buffers any messages not tagged as
		//    replays once it has seen the *first* replay
		//  - we know that the target domain will apply all bufferd messages after it sees
		//    last = true
		//
		// we therefore have two jobs to do:
		//
		//  1. ensure that we only send one message with last = true.
		//  2. ensure that all messages we forward after we allow the first replay message
		//     through logically follow the replay.
		//
		// step 1 is pretty easy -- we only set last = true when we've seen last = true
		// from all our ancestors. until that is the case, we just set last = false in all
		// our outgoing messages (even if they had last set).
		//
		// step 2 is trickier. consider the following in a union U
		// across two ancestors, L and R:
		//
		//  1. L sends first replay
		//  2. L sends a normal message
		//  3. R sends a normal message
		//  4. R sends first replay
		//  5. U receives L's replay
		//  6. U receives R's message
		//  7. U receives R's replay
		//
		// when should U emit the first replay? if it does it eagerly (i.e., at 1), then
		// R's normal message at 3 (which is also present in R's replay) will be buffered
		// and replayed at the target domain, since it comes after the first replay
		// message. instead, we must delay sending the first replay until we have seen the
		// first replay from *every* ancestor. in other words, 1 must be captured, and only
		// emitted at 5. unfortunately, 2 also wants to cause us pain. it must *not* be
		// sent until after 5 either, because otherwise it would be dropped by the target
		// domain, which is *not* okay since it is not included in L's replay.
		//
		// phew.
		//
		// first, how do we emit *two* replay messages at 5? it turns out that we're in
		// luck. because only one replay can be going on at a time, the target domain
		// doesn't actually care about which tag we use for the forward (well, as long as
		// it is *one* of the full replay tags). and since we're a union, we can simply
		// fold 1 and 4 into a single update, and then emit that!
		//
		// second, how do we ensure that 2 also gets sent *after* the replay has started.
		// again, we're in luck. we can simply absorb 2 into the replay when we detect that
		// there's a replay which hasn't started yet! we do that above (in the other match
		// arm). feel free to go check. interestingly enough, it's also fine for us to
		// still emit 2 (i.e., not capture it), since it'll just be dropped by the target
		// domain.
		last := *rpl.Full
		res, err := u.OnInput(nil, ex, from, rs, nil, n, s)
		if err != nil {
			return nil, err
		}
		rs := res.Records
		if u.wait == nil {
			if u.required == 1 {
				return &processing.FullReplay{Records: rs, Last: last}, nil
			}

			u.wait = &ongoingWait{
				started:  map[boostpb.LocalNodeIndex]bool{from: true},
				finished: 0,
				buffered: rs,
			}
			if last {
				u.wait.finished = 1
			}
			return &processing.CapturedFull{}, nil
		}

		wait := u.wait
		if last {
			wait.finished++
		}
		if wait.finished == u.required {
			// we can just send everything and we're done!
			// make sure to include what's in *this* replay.
			u.wait = nil
			return &processing.FullReplay{Records: append(wait.buffered, rs...), Last: true}, nil
		}
		if len(wait.started) != u.required {
			if _, exists := wait.started[from]; !exists {
				wait.started[from] = true
				if len(wait.started) == u.required {
					// we can release all buffered replays
					rs = append(wait.buffered, rs...)
					wait.buffered = nil
					return &processing.FullReplay{Records: rs, Last: false}, nil
				}
			}
		} else {
			if len(wait.buffered) != 0 {
				panic("no buffered records expected")
			}
			// common case: replay has started, and not yet finished
			// no need to buffer, nothing to see here, move along
			return &processing.FullReplay{Records: rs, Last: false}, nil
		}

		wait.buffered = append(wait.buffered, rs...)
		return &processing.CapturedFull{}, nil
	case rpl.Partial != nil:
		keyCols := rpl.Partial.KeyCols
		keys := rpl.Partial.Keys
		requestingShard := rpl.Partial.RequestingShard
		unishard := rpl.Partial.Unishard
		tag := rpl.Partial.Tag
		isShardMerger := false
		rkeyFrom := boostpb.LocalNodeIndex(0)

		if _, all := u.emit.(*unionEmitAll); all {
			if unishard {
				// No need to buffer since request should only be for one shard
				return &processing.ReplayPiece{
					Rows:     rs,
					Keys:     keys,
					Captured: nil,
				}, nil
			}
			isShardMerger = true
		} else {
			rkeyFrom = from
		}

		rk := unionreplayKey{tag, rkeyFrom}
		if _, ok := u.replayKey[rk]; !ok {
			switch emit := u.emit.(type) {
			case *unionEmitAll:
				u.replayKey[rk] = append(u.replayKey[rk], keyCols...)

			case *unionEmitProject:
				emission, ok := emit.emitLeft.Get(from)
				if !ok {
					panic("missing entry in emitLeft")
				}

				var extend []int
				for _, c := range keyCols {
					extend = append(extend, emission[c])
				}

				u.replayKey[rk] = append(u.replayKey[rk], extend...)

				emit.emitLeft.Scan(func(src boostpb.LocalNodeIndex, emission []int) bool {
					if src != from {
						var extend []int
						for _, c := range keyCols {
							extend = append(extend, emission[c])
						}
						u.replayKey[unionreplayKey{tag, src}] = extend
					}
					return true
				})
			}
			// otherwise we already know the meta info for this tag
		}

		rsByKey := make(map[boostpb.Row][]boostpb.Record)
		for _, r := range rs {
			rr := r.Row.IndexWith(keyCols)
			rsByKey[rr] = append(rsByKey[rr], r)
		}

		var released = make(map[boostpb.Row]bool)
		var captured = make(map[boostpb.Row]bool)
		var toprocess []*unionreplay
		var finalrecords []boostpb.Record

		for key := range keys {
			rs := rsByKey[key]
			delete(rsByKey, key)

			urp := &unionreplay{
				tag:          tag,
				replayingKey: key,
				shard:        requestingShard,
			}
			e, found := u.replayPieces.Get(urp)
			if found {
				if _, contains := e.buffered[from]; contains {
					// got two upquery responses for the same key for the same
					// downstream shard. waaaaaaat?
					// TODO: better panic message
					_ = isShardMerger
					panic("downstream shard double-requested a key")
				}
				if len(e.buffered) == u.required-1 {
					// release!
					u.replayPieces.Delete(e)
					e.buffered[from] = rs
					toprocess = append(toprocess, e)
				} else {
					e.buffered[from] = rs
					captured[key] = true
				}
			} else {
				urp.buffered = make(map[boostpb.LocalNodeIndex][]boostpb.Record)
				urp.buffered[from] = rs
				if u.required == 1 {
					toprocess = append(toprocess, urp)
				} else {
					u.replayPieces.ReplaceOrInsert(urp)
					captured[key] = true
				}
			}
		}

		for _, urp := range toprocess {
			if urp.evict {
				panic("!!! need to issue an eviction after replaying key")
			}
			released[urp.replayingKey] = true
			for from, rs := range urp.buffered {
				res, err := u.OnInput(nil, ex, from, rs, keyCols, n, s)
				if err != nil {
					return nil, err
				}
				finalrecords = append(finalrecords, res.Records...)
			}
		}

		// here's another bit that's a little subtle:
		//
		// remember how, above, we stripped out the upquery identifier from the replay's
		// tag? consider what happens if we buffer a replay with, say, tag 7.2 (so, upquery
		// 7, path 2). later, when some other replay comes along with, say, tag 7.1, we
		// decide that we're done buffering. we then release the buffered records from the
		// first replay alongside the ones that were in the 7.1 replay. but, we just
		// effectively _changed_ the tag for the records in that first replay! is that ok?
		// it turns out it _is_, and here is the argument for why:
		//
		// first, for a given upquery, let's consider what paths flow through us when there
		// is only a single union on the upquery's path. we know there is then one path for
		// each parent we have. an upquery from below must query each of our parents once
		// to get the complete results. those queries will all have the same upquery id,
		// but different path ids. since there is no union above us or below us on the
		// path, there are exactly as many paths as we have ancestors, and those paths only
		// branch _above_ us. below us, those different paths are the _same_. this means
		// that no matter what path discriminator we forward something with, it will follow
		// the right path.
		//
		// now, what happens if there is a exactly one union on the upquery's path, at or
		// above one of our parents. well, _that_ parent will have as many distinct path
		// identifiers as that union has parents. we know that it will only produce _one_
		// upquery response through (since the union will buffer), and that the repsonse
		// will have (an arbitrary chosen) one of those path identifiers. we know that path
		// discriminators are distinct, so whichever identifier our union ancestor chooses,
		// it will be distinct from the paths that go through our _other_ parents.
		// furthermore, we know that all those path identifiers ultimately share the same
		// path below us. and we also know that the path identifiers of the paths through
		// our other parents share that same path. so choosing any of them is fine.
		//
		// if we have unions above multiple of our parents, the same argument holds.
		//
		// if those unions again have ancestors that are unions, the same argument holds.
		//
		// the missing piece then is how a union that has a union as a _descendant_ knows
		// that any choice it makes for path identifier is fine for that descendant. this
		// is trickier to argue, but the argument goes like this:
		//
		// imagine you have a union immediately followed by a union, followed by some node
		// that wishes to make an upquery. imagine that each union has two incoming edges:
		// the bottom union has two edges to the top union (a "diamond"), and the top union
		// has two incoming edges from disjoint parts of the graph. i don't know why you'd
		// have that, but let's imagine. for the one upquery id here, there are four path
		// identifiers: bottom-left:top-left, bottom-left:top-right, bottom-right:top-left,
		// and bottom-right:top-right. as the top union, we will therefore receive two
		// NOPE
		return &processing.ReplayPiece{
			Rows:     finalrecords,
			Keys:     released,
			Captured: captured,
		}, nil
	}
	panic("unreachable")
}

func (u *Union) internal() {}

func (u *Union) dataflow() {}

func (u *Union) Ancestors() []graph.NodeIdx {
	return u.emit.Ancestors()
}

func (u *Union) SuggestIndexes(you graph.NodeIdx) map[graph.NodeIdx][]int {
	return nil
}

func (u *Union) Resolve(col int) []NodeColumn {
	return u.emit.Resolve(col)
}

func (u *Union) ParentColumns(col int) []NodeColumn {
	return u.emit.ParentColumns(col)
}

func (u *Union) ColumnType(g *graph.Graph[*Node], col int) boostpb.Type {
	return u.emit.ColumnType(g, col)
}

func (u *Union) Description(detailed bool) string {
	return u.emit.Description(detailed)
}

func (u *Union) OnConnected(graph *graph.Graph[*Node]) {
	u.emit.OnConnected(graph)
}

func (u *Union) OnCommit(you graph.NodeIdx, remap map[graph.NodeIdx]boostpb.IndexPair) {
	u.me = you
	u.emit.OnCommit(remap)
}

func (u *Union) OnInput(you *Node, ex processing.Executor, from boostpb.LocalNodeIndex, rs []boostpb.Record, replayKeyCol []int, domain *Map, states *state.Map) (processing.Result, error) {
	return u.emit.OnInput(from, rs)
}

func (u *Union) IsShardMerger() bool {
	_, emitAll := u.emit.(*unionEmitAll)
	return emitAll
}

func (u *Union) OnEviction(from boostpb.LocalNodeIndex, tag boostpb.Tag, keys []boostpb.Row) {
	for _, key := range keys {
		var startKey = &unionreplay{tag: tag, replayingKey: key, shard: 0}
		var endKey = &unionreplay{tag: tag, replayingKey: key, shard: math.MaxUint}

		u.replayPieces.AscendRange(startKey, endKey, func(e *unionreplay) bool {
			if _, found := e.buffered[from]; found {
				// we've already received something from left, but it has now been evicted.
				// we can't remove the buffered replay, since we'll then get confused when the
				// other parts of the replay arrive from the other sides. we also can't drop
				// the eviction, because the eviction might be there to, say, ensure key
				// monotonicity in the face of joins. instead, we have to *buffer* the eviction
				// and emit it immediately after releasing the replay for this key. we do need
				// to emit the replay, as downstream nodes are waiting for it.
				//
				// NOTE: e.evict may already be true here, as we have no guarantee that
				// upstream nodes won't send multiple evictions in a row (e.g., joins evictions
				// could cause this).
				e.evict = true
			}
			return true
		})
	}
}

type unionEmit interface {
	Ancestors() []graph.NodeIdx
	Resolve(col int) []NodeColumn
	Description(detailed bool) string
	ParentColumns(col int) []NodeColumn
	ColumnType(graph *graph.Graph[*Node], col int) boostpb.Type
	OnCommit(remap map[graph.NodeIdx]boostpb.IndexPair)
	OnConnected(graph *graph.Graph[*Node])
	OnInput(from boostpb.LocalNodeIndex, rs []boostpb.Record) (processing.Result, error)
}

func NewUnion(emit map[graph.NodeIdx][]int) *Union {
	var emitLocal = make(map[boostpb.IndexPair][]int, len(emit))
	for k, cols := range emit {
		if !sort.IntsAreSorted(cols) {
			panic("union does not support column reordering")
		}
		emitLocal[boostpb.NewIndexPair(k)] = cols
	}

	return &Union{
		emit: &unionEmitProject{
			emit: emitLocal,
			cols: make(map[boostpb.IndexPair]int),
		},
		required:     len(emit),
		me:           graph.InvalidNode,
		replayKey:    make(map[unionreplayKey][]int),
		replayPieces: btree.NewG[*unionreplay](2, compareReplays),
	}
}

func NewUnionDeshard(parent graph.NodeIdx, sharding boostpb.Sharding) *Union {
	shards := sharding.TryGetShards()
	if shards == nil {
		panic("cannot create Union for this sharding mode")
	}
	return &Union{
		emit: &unionEmitAll{
			from:     boostpb.NewIndexPair(parent),
			sharding: sharding,
		},
		required:     int(*shards),
		me:           graph.InvalidNode,
		replayKey:    make(map[unionreplayKey][]int),
		replayPieces: btree.NewG[*unionreplay](2, compareReplays),
	}
}

func (u *Union) ToProto() *boostpb.Node_InternalUnion {
	if len(u.replayKey) > 0 || u.replayPieces.Len() > 0 {
		panic("unsupported")
	}

	punion := &boostpb.Node_InternalUnion{
		Required: u.required,
		Me:       u.me,
	}
	switch e := u.emit.(type) {
	case *unionEmitAll:
		punion.Emit = &boostpb.Node_InternalUnion_All{All: e.ToProto()}
	case *unionEmitProject:
		punion.Emit = &boostpb.Node_InternalUnion_Project{Project: e.ToProto()}
	}
	return punion
}

func NewUnionFromProto(u *boostpb.Node_InternalUnion) *Union {
	var emit unionEmit
	switch e := u.Emit.(type) {
	case *boostpb.Node_InternalUnion_All:
		emit = newUnionEmitAllFromProto(e.All)
	case *boostpb.Node_InternalUnion_Project:
		emit = newUnionEmitProjectFromProto(e.Project)
	}
	return &Union{
		emit:         emit,
		replayKey:    make(map[unionreplayKey][]int),
		replayPieces: btree.NewG[*unionreplay](2, compareReplays),
		required:     u.Required,
		me:           u.Me,
	}
}
