package domain

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/common/deque"
	"vitess.io/vitess/go/boost/common/xslice"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/server/controller/boostplan/upquery"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vthash"
)

func (d *Domain) handleReplay(ctx context.Context, m *packet.ReplayPiece, executor processing.Executor) error {
	tag := m.Tag
	rp := d.replayPaths[tag]
	pathLast := rp.Path[len(rp.Path)-1]

	if d.nodes.Get(pathLast.Node).IsDropped() {
		return nil
	}

	if ext := m.External; ext != nil {
		if ext.Slot > 0 {
			select {
			case d.upquerySlots <- uint8(ext.Slot):
			default:
				panic("repeated upquery slot?")
			}

			if gt := d.gtidTrackers.Get(m.Link.Src); gt != nil {
				buffered := gt.FinishUpquery(ctx, m)
				for _, msg := range buffered {
					d.delayForSelf(msg)
				}
			} else {
				panic("cannot resolve ReplayPiece with UpquerySlot")
			}

			// clear the upquery slot now that the upquery has been resolved
			ext.Slot = 0
		}
		if ext.Failed {
			return nil
		}
	}

	type needreplay struct {
		node              dataflow.LocalNodeIdx
		whileReplayingKey sql.Row
		missCols          common.Columns
		unishard          bool
		waiters           bool
		requestingShard   uint
		tag               dataflow.Tag
	}
	type finished struct {
		tag     dataflow.Tag
		ni      dataflow.LocalNodeIdx
		forKeys map[sql.Row]bool
		gtid    string
	}

	needReplay := make(map[needreplay][]sql.Row)
	var finishedReplay *finished
	var finishedPartial int
	var notifyDone = rp.NotifyDone && m.GetReplace() == nil

	switch {
	case d.replaying == nil && notifyDone:
		// this is the first message we receive for this tagged replay path. only at
		// this point should we start buffering messages for the target node. since the
		// node is not yet marked ready, all previous messages for this node will
		// automatically be discarded by dispatch(). the reason we should ignore all
		// messages preceeding the first replay message is that those have already been
		// accounted for in the state we are being replayed. if we buffered them and
		// applied them after all the state has been replayed, we would double-apply
		// those changes, which is bad.
		d.replaying = &Replaying{
			To:       pathLast.Node,
			Buffered: deque.New[packet.ActiveFlowPacket](),
			Passes:   0,
		}
	case d.replaying == nil:
		// we're replaying to forward to another domain
	case d.replaying != nil:
		// another packet the local state we are constructing
	}

	// let's collect some information about the destination of this replay
	dst := pathLast.Node
	dstIsReader := d.nodes.Get(dst).IsReader()
	dstIsTarget := !d.nodes.Get(dst).IsSender()

	if dstIsTarget {
		if partial, ok := m.Context.(*packet.ReplayPiece_Partial); ok {
			partial := partial.Partial
			had := len(partial.ForKeys)
			partialKeys := common.ColumnsFrom(pathLast.PartialKey)

			if w, ok := d.waiting[dst]; ok {
				for r := range partial.ForKeys {
					_, found := w.redos[hole{partialKeys, r}]
					if !found {
						delete(partial.ForKeys, r)
					}
				}
			} else if prev, ok := d.readerTriggered[dst]; ok {
				for r := range partial.ForKeys {
					if !prev.Contains(r) {
						delete(partial.ForKeys, r)
					}
				}
			} else {
				d.log.Debug("ignored replay: not waiting nor reader triggered", tag.Zap(), dst.Zap())
				return nil
			}

			if len(partial.ForKeys) == 0 {
				return nil
			} else if len(partial.ForKeys) != had {
				// discard records in data associated with the keys we weren't
				// waiting for
				// note that we need to use the partial_keys column IDs from the
				// *start* of the path here, as the records haven't been processed
				// yet
				partialKeys := rp.Path[0].PartialKey

				var records = m.Records[:0]
				for _, r := range m.Records {
					var anyRowIsEqual bool

					for k := range partial.ForKeys {
						var rowIsEqual = true
						for i, c := range partialKeys {
							if r.Row.ValueAt(c).Cmp(k.ValueAt(i)) != 0 {
								rowIsEqual = false
								break
							}
						}
						if rowIsEqual {
							anyRowIsEqual = true
							break
						}
					}

					if anyRowIsEqual {
						records = append(records, r)
					}
				}

				m.Records = records
			}
		}
	}

	var input = flownode.FlowInput{
		Packet: packet.ActiveFlowPacket{
			Inner: &packet.ReplayPiece{
				Link:     m.Link,
				Tag:      m.Tag,
				Records:  m.Records,
				Context:  m.Context,
				External: m.External,
			},
		},
		KeyedBy:    nil,
		State:      d.state,
		Nodes:      d.nodes,
		OnShard:    d.shard,
		Swap:       false,
		ReplayPath: rp,
		Ex:         executor,
	}

	// forward the current message through all local nodes.
	for i, segment := range rp.Path {
		if segment.ForceTagTo != dataflow.TagNone {
			if rpiece := input.Packet.GetReplayPiece(); rpiece != nil {
				rpiece.Tag = segment.ForceTagTo
			}
		}
		n := d.nodes.Get(segment.Node)
		isReader := n.IsReader()

		// keep track of whether we're filling any partial holes
		partialKeyCols := segment.PartialKey

		// keep a copy of the partial keys from before we process
		// we need this because n.process may choose to reduce the set of keys
		// (e.g., because some of them missed), in which case we need to know what
		// keys to _undo_.
		var backfillKeys map[sql.Row]bool
		if partial := input.Packet.GetReplayPiece().GetPartial(); partial != nil {
			backfillKeys = maps.Clone(partial.ForKeys)
		}

		// figure out if we're the target of a partial replay.
		// this is the case either if the current node is waiting for a replay,
		// *or* if the target is a reader. the last case is special in that when a
		// client requests a replay, the Reader isn't marked as "waiting".
		target := backfillKeys != nil && i == len(rp.Path)-1 && (isReader || !n.IsSender())

		// are we about to fill a hole?
		if target {
			// mark the state for the key being replayed as *not* a hole otherwise
			// we'll just end up with the same "need replay" response that
			// triggered this replay initially.
			if st := d.state.Get(segment.Node); st != nil {
				for key := range backfillKeys {
					st.MarkFilled(key, m.Tag)
				}
			} else {
				r := n.AsReader()
				if wh := r.Writer(); wh != nil {
					for key := range backfillKeys {
						wh.WithKey(key).MarkFilled()
					}
				}
			}
		}

		input.KeyedBy = segment.PartialKey
		var output flownode.FlowOutput
		if err := n.Process(ctx, &input, &output); err != nil {
			return err
		}

		slices.SortFunc(output.Misses, func(a, b processing.Miss) int {
			return a.Compare(&b)
		})
		slices.CompactFunc(output.Misses, func(a, b processing.Miss) bool {
			return a.Compare(&b) == 0
		})

		rpiece := input.Packet.GetReplayPiece()
		if partial := rpiece.GetPartial(); partial != nil {
			output.Misses = xslice.FilterInPlace(output.Misses, func(miss processing.Miss) bool {
				if miss.ForceTag == dataflow.TagNone {
					return true
				}
				nr := needreplay{
					node:              miss.On,
					whileReplayingKey: miss.ReplayKey(),
					missCols:          common.ColumnsFrom(miss.LookupIdx),
					unishard:          partial.Unishard,
					requestingShard:   partial.RequestingShard,
					tag:               miss.ForceTag,
					waiters:           false,
				}

				needReplay[nr] = append(needReplay[nr], miss.LookupKey())
				return false
			})
		} else {
			if slices.ContainsFunc(output.Misses, func(miss processing.Miss) bool { return miss.ForceTag != dataflow.TagNone }) {
				panic("regular replay with forced tag")
			}
		}

		var missedOn *sql.RowSet
		if backfillKeys != nil {
			missedOn = sql.NewRowSet(n.Schema())
			for _, miss := range output.Misses {
				missedOn.Add(miss.ReplayKey())
			}
		}

		if target {
			if len(output.Misses) > 0 {
				// we missed while processing
				// it's important that we clear out any partially-filled holes.
				if st := d.state.Get(segment.Node); st != nil {
					missedOn.ForEach(func(miss sql.Row) {
						st.MarkHole(miss, tag)
					})
				} else {
					if w := n.AsReader().Writer(); w != nil {
						missedOn.ForEach(func(miss sql.Row) {
							w.WithKey(miss).MarkHole()
						})
					}
				}
			} else if isReader {
				r := n.AsReader()
				if wh := r.Writer(); wh != nil {
					wh.Swap()
				}
				if prev, ok := d.readerTriggered[segment.Node]; ok {
					for key := range backfillKeys {
						prev.Remove(key)
					}
				}
			}
		}

		if target && len(output.Captured) > 0 {
			// materialized union ate some of our keys,
			// so we didn't *actually* fill those keys after all!
			if st := d.state.Get(segment.Node); st != nil {
				for key := range output.Captured {
					st.MarkHole(key, m.Tag)
				}
			} else {
				r := n.AsReader()
				if wh := r.Writer(); wh != nil {
					for key := range output.Captured {
						wh.WithKey(key).MarkHole()
					}
				}
			}
		}

		if input.Packet.Inner == nil {
			// eaten full replay
			goto finished
		}

		// we need to track how many replays we completed, and we need to do so
		// *before* we prune keys that missed. these conditions are all important,
		// so let's walk through them
		//
		//  1. this applies only to partial backfills
		//  2. we should only set finished_partial if it hasn't already been set.
		//     this is important, as misses will cause backfill_keys to be pruned
		//     over time, which would cause finished_partial to hold the wrong
		//     value!
		//  3. if the last node on this path is a reader, or is a ::End (so we
		//     triggered the replay) then we need to decrement the concurrent
		//     replay count! note that it's *not* sufficient to check if the
		//     *current* node is a target/reader, because we could miss during a
		//     join along the path.
		if backfillKeys != nil && finishedPartial == 0 && (dstIsReader || dstIsTarget) {
			finishedPartial = len(backfillKeys)
		}

		if rpiece := input.Packet.GetReplayPiece(); rpiece != nil {
			if partial, ok := rpiece.Context.(*packet.ReplayPiece_Partial); ok {
				partial := partial.Partial
				for k := range backfillKeys {
					if !partial.ForKeys[k] {
						delete(backfillKeys, k)
					}
				}
			}
		}

		// if we missed during replay, we need to do another replay
		if backfillKeys != nil && len(output.Misses) > 0 {
			// so, in theory, unishard can be changed by n.process. however, it
			// will only ever be changed by a union, which can't cause misses.
			// since we only enter this branch in the cases where we have a miss,
			// it is okay to assume that unishard _hasn't_ changed, and therefore
			// we can use the value that's in m.

			partial := input.Packet.GetReplayPiece().Context.(*packet.ReplayPiece_Partial).Partial

			for _, miss := range output.Misses {
				nr := needreplay{
					node:              miss.On,
					whileReplayingKey: miss.ReplayKey(),
					missCols:          common.ColumnsFrom(miss.LookupIdx),
					unishard:          partial.Unishard,
					requestingShard:   partial.RequestingShard,
					tag:               m.Tag,
					waiters:           true,
				}
				needReplay[nr] = append(needReplay[nr], miss.LookupKey())
			}

			// we should only finish the replays for keys that *didn't* miss
			for k := range backfillKeys {
				if missedOn.Contains(k) {
					delete(backfillKeys, k)
				}
			}

			// prune all replayed records for keys where any replayed record for
			// that key missed.
			input.Packet.FilterRecords(func(r sql.Record) bool {
				partialRow := r.Row.IndexWith(partialKeyCols)
				return !missedOn.Contains(partialRow)
			})
		}

		if backfillKeys != nil && len(backfillKeys) == 0 {
			goto finished
		}

		// we successfully processed some upquery responses!
		//
		// at this point, we can discard the state that the replay used in n's
		// ancestors if they are beyond the materialization frontier (and thus
		// should not be allowed to amass significant state).
		//
		// we want to make sure we only remove state once it will no longer be
		// looked up into though. consider this dataflow graph:
		//
		//  (a)     (b)
		//   |       |
		//   |       |
		//  (q)      |
		//   |       |
		//   `--(j)--`
		//       |
		//
		// where j is a join, a and b are materialized, q is query-through. if we
		// removed state the moment a replay has passed through the next operator,
		// then the following could happen: a replay then comes from a, passes
		// through q, q then discards state from a and forwards to j. j misses in
		// b. replay happens to b, and re-triggers replay from a. however, state in
		// a is discarded, so replay to a needs to happen a second time. that's not
		// _wrong_, and we will eventually make progress, but it is pretty
		// inefficient.
		//
		// insted, we probably want the join to do the eviction. we achieve this by
		// only evicting from a after the replay has passed the join (or, more
		// generally, the operator that might perform lookups into a)
		if backfillKeys != nil {
			// next, evict any state that we had to look up to process this replay.
			var pnsFor = dataflow.InvalidLocalNode
			var evictTag = dataflow.TagNone
			var pns []dataflow.LocalNodeIdx
			var tmp []dataflow.LocalNodeIdx

			for _, lookup := range output.Lookups {
				// don't evict from our own state
				if lookup.On == segment.Node {
					continue
				}

				if pnsFor != lookup.On {
					pns = pns[:0]
					tmp = append(tmp, lookup.On)

					for len(tmp) > 0 {
						pn := tmp[len(tmp)-1]
						tmp = tmp[:len(tmp)-1]
						pnn := d.nodes.Get(pn)

						if d.state.ContainsKey(pn) {
							// Otherwise we should not evict from this
							continue
						}

						if !pnn.CanQueryThrough() {
							panic("lookup into non-materialized, non-query-through node")
						}
						tmp = append(tmp, pnn.Parents()...)
					}

					pnsFor = lookup.On
				}

				tagMatch := func(rp *replay.Path, pn dataflow.LocalNodeIdx) bool {
					lastp := rp.Path[len(rp.Path)-1]
					return lastp.Node == pn && slices.Equal(lastp.PartialKey, lookup.Cols)
				}

				for _, pn := range pns {
					st := d.state.Get(pn)

					// this is a node that we were doing lookups into as part of
					// the replay -- make sure we evict any state we may have added
					// there.
					if evictTag != dataflow.TagNone {
						if !tagMatch(d.replayPaths[evictTag], pn) {
							// can't reuse
							evictTag = dataflow.TagNone
						}
					}

					if evictTag == dataflow.TagNone {
						if cs, ok := d.replayPathsByDst[pn]; ok {
							if tags, ok := cs[common.ColumnsFrom(lookup.Cols)]; ok {
								// this is the tag we would have used to
								// fill a lookup hole in this ancestor, so
								// this is the tag we need to evict from.
								if len(tags) != 1 {
									panic("TODO: multiple tags on eviction")
								}
								evictTag = tags[0]
							}
						}
					}

					if evictTag != dataflow.TagNone {
						st.MarkHole(lookup.Key, evictTag)
					} else {
						panic("no tag found for lookup target")
					}
				}

			}
		}

		// we're all good -- continue propagating
		if rpiece := input.Packet.GetReplayPiece(); rpiece != nil {
			if len(rpiece.Records) == 0 {
				if regular := rpiece.GetRegular(); regular != nil && !regular.Last {
					// don't continue processing empty updates, *except* if this is the
					// last replay batch. in that case we need to send it so that the
					// next domain knows that we're done
					// TODO: we *could* skip ahead to path.last() here
					break
				}
			}
		}

		if i < len(rp.Path)-1 {
			// update link for next iteration
			if d.nodes.Get(rp.Path[i+1].Node).IsShardMerger() {
				// we need to preserve the egress src for shard mergers
				// (which includes shard identifier)
			} else {
				input.Packet.Link().Src = segment.Node
			}
			input.Packet.Link().Dst = rp.Path[i+1].Node
		}

		if rpiece := input.Packet.GetReplayPiece(); rpiece != nil {
			if partial := rpiece.GetPartial(); partial != nil {
				partial.ForKeys = backfillKeys
			}
		}
	}

	if message := input.Packet.GetMessage(); message != nil {
		me := input.Packet.Dst()

		for _, child := range d.nodes.Get(me).Children() {
			childIsMerger := d.nodes.Get(child).IsShardMerger()

			m := input.Packet.Clone()

			if childIsMerger {
				// we need to preserve the egress src (which includes shard identifier)
			} else {
				m.Link().Src = me
			}
			m.Link().Dst = child
			if err := d.dispatch(ctx, m, executor); err != nil {
				return err
			}
		}
		return nil
	}

	switch replayctx := input.Packet.GetReplayPiece().Context.(type) {
	case *packet.ReplayPiece_Regular:
		regular := replayctx.Regular
		if regular.Last {
			if notifyDone {
				finishedReplay = &finished{tag: m.Tag, ni: dst, forKeys: nil, gtid: m.External.GetGtid()}
			}
		}
	case *packet.ReplayPiece_Partial:
		partial := replayctx.Partial
		switch {
		case dstIsReader:
			// we're on the replay path
		case dstIsTarget:
			if finishedPartial == 0 {
				if len(partial.ForKeys) != 0 {
					panic("expected ForKeys to be empty")
				}
			}
			finishedReplay = &finished{tag: m.Tag, ni: dst, forKeys: partial.ForKeys, gtid: m.External.GetGtid()}
		}
	default:
		panic("unexpected replay piece context")
	}

finished:
	if finishedPartial != 0 {
		if err := d.finishedPartialReplay(ctx, tag, finishedPartial); err != nil {
			return err
		}
	}

	for nr, misses := range needReplay {
		if err := d.onReplayMiss(ctx, nr.node, nr.missCols.ToSlice(), nr.whileReplayingKey, misses, nr.unishard, nr.waiters, nr.requestingShard, nr.tag); err != nil {
			return err
		}
	}

	if finishedReplay != nil {
		d.log.Debug("partial replay finished", finishedReplay.ni.Zap())
		if waiting, ok := d.waiting[finishedReplay.ni]; ok {
			delete(d.waiting, finishedReplay.ni)

			rp := d.replayPaths[finishedReplay.tag]
			keyCols := common.ColumnsFrom(rp.Path[len(rp.Path)-1].PartialKey)

			// we got a partial replay result that we were waiting for. it's time we let any
			// downstream nodes that missed in us on that key know that they can (probably)
			// continue with their replays.
			for key := range finishedReplay.forKeys {
				h := hole{keyCols, key}
				repl, ok := waiting.redos[h]
				if !ok {
					panic("got backfill for unnecessary key")
				}
				delete(waiting.redos, h)

				var replayvec []redo
				for taggedReplayKey := range repl {
					waiting.holes[taggedReplayKey]--
					left := waiting.holes[taggedReplayKey]

					if left == 0 {
						d.log.Debug("filled last hole for key, triggering replay",
							zap.Any("key", taggedReplayKey))
						delete(waiting.holes, taggedReplayKey)
						if taggedReplayKey.waiters {
							replayvec = append(replayvec, taggedReplayKey)
						}
					} else {
						d.log.Debug("filled hole for key, not triggering replay",
							zap.Any("key", taggedReplayKey), zap.Int("left", left))
					}
				}

				for _, rd := range replayvec {
					request := &packet.PartialReplayRequest{
						Tag:             rd.tag,
						Unishard:        rd.unishard,
						RequestingShard: rd.requestingShard,
						Keys:            []sql.Row{rd.replayKey},
					}
					d.delayForSelf(request)
				}
			}

			if len(waiting.holes) > 0 {
				// there are still holes, so there must still be pending redos
				d.waiting[finishedReplay.ni] = waiting
			} else {
				if len(waiting.redos) > 0 {
					panic("there should be no redos left")
				}
			}
		} else if finishedReplay.forKeys != nil {
			panic("unreachable")
		} else {
			// must be a full replay
			// NOTE: node is now ready, in the sense that it shouldn't ignore all updates since
			// replaying_to is still set, "normal" dispatch calls will continue to be buffered, but
			// this allows finish_replay to dispatch into the node by overriding replaying_to.
			delete(d.notReady, finishedReplay.ni)

			pkt := &packet.FinishReplayRequest{
				Tag:        tag,
				Node:       finishedReplay.ni,
				Gtid:       finishedReplay.gtid,
				NotifyDone: notifyDone,
			}
			d.delayForSelf(pkt)
		}
	}

	return nil
}

func (d *Domain) handleReaderReplay(ctx context.Context, replay *packet.ReaderReplayRequest) error {
	// the reader could have raced with us filling in the key after some
	// *other* reader requested it, so let's double check that it indeed still
	// misses!

	node := replay.Node
	r := d.nodes.Get(node).AsReader()
	if r == nil {
		panic("reader replay requested for non-reader node")
	}

	w := r.Writer()
	w.Swap()

	keys := make([]sql.Row, 0, len(replay.Keys))
	for _, k := range replay.Keys {
		if !w.WithKey(k).Found() {
			keys = append(keys, k)
		}
	}

	missingKeys := keys[:0]
	for _, k := range keys {
		allrows, ok := d.readerTriggered[node]
		if !ok {
			allrows = sql.NewRowSet(r.KeySchema())
			d.readerTriggered[node] = allrows
		}
		if allrows.Add(k) {
			missingKeys = append(missingKeys, k)
		}
	}
	if len(missingKeys) > 0 {
		return d.findTagsAndReplay(ctx, missingKeys, replay.Cols, node)
	}
	return nil
}

func (d *Domain) findTagsAndReplay(ctx context.Context, missKeys []sql.Row, missCols []int, missIn dataflow.LocalNodeIdx) error {
	missColumns := common.ColumnsFrom(missCols)

	var tags []dataflow.Tag
	if candidates, ok := d.replayPathsByDst[missIn]; ok {
		if ts, ok := candidates[missColumns]; ok {
			tags = ts
		}
	}

	for _, tag := range tags {
		// send a message to the source domain(s) responsible
		// for the chosen tag so they'll start replay.
		if d.replayPaths[tag].Trigger.Kind == replay.TriggerLocal {
			// *in theory* we could just call self.seed_replay, and everything would be good.
			// however, then we start recursing, which could get us into sad situations where
			// we break invariants where some piece of code is assuming that it is the only
			// thing processing at the time (think, e.g., borrow_mut()).
			//
			// for example, consider the case where two misses occurred on the same key.
			// normally, those requests would be deduplicated so that we don't get two replay
			// responses for the same key later. however, the way we do that is by tracking
			// keys we have requested in self.waiting.redos (see `redundant` in
			// `on_replay_miss`). in particular, on_replay_miss is called while looping over
			// all the misses that need replays, and while the first miss of a given key will
			// trigger a replay, the second will not. if we call `seed_replay` directly here,
			// that might immediately fill in this key and remove the entry. when the next miss
			// (for the same key) is then hit in the outer iteration, it will *also* request a
			// replay of that same key, which gets us into trouble with `State::mark_filled`.
			//
			// so instead, we simply keep track of the fact that we have a replay to handle,
			// and then get back to it after all processing has finished (at the bottom of
			// `Self::handle()`)
			repl := &packet.PartialReplayRequest{
				Tag:             tag,
				Unishard:        true,
				Keys:            missKeys,
				RequestingShard: common.UnwrapOr(d.shard, 0),
			}
			d.delayForSelf(repl)
			continue
		}

		// NOTE: due to max_concurrent_replays, it may be that we only replay from *some* of
		// these ancestors now, and some later. this will cause more of the replay to be
		// buffered up at the union above us, but that's probably fine.
		if err := d.requestPartialReplay(ctx, tag, missKeys); err != nil {
			return err
		}
	}

	if len(tags) == 0 {
		panic("no tag found to fill missing value")
	}
	return nil
}

func (d *Domain) requestPartialReplay(ctx context.Context, tag dataflow.Tag, keys []sql.Row) error {
	if d.concurrentReplays < d.maxConcurrentReplays {
		return d.sendPartialReplayRequest(ctx, tag, keys)
	}
	d.replayRequestQueue.PushBack(replay.ReplayRequest{Tag: tag, Data: keys})
	return nil
}

func makePartialRequest(shard *uint, tag dataflow.Tag, keys []sql.Row, unishard bool) *packet.PartialReplayRequest {
	return &packet.PartialReplayRequest{
		Tag:             tag,
		Keys:            keys,
		Unishard:        unishard,
		RequestingShard: common.UnwrapOr(shard, 0),
	}
}

func (d *Domain) sendPartialReplayRequest(ctx context.Context, tag dataflow.Tag, keys []sql.Row) error {
	path, ok := d.replayPaths[tag]
	if !ok {
		panic("asked to replay along non-existing path")
	}

	switch path.Trigger.Kind {
	case replay.TriggerEnd:
		return d.sendPartialReplaySlow(ctx, tag, keys, path, path.Trigger)
	case replay.TriggerExternal:
		keyMap := make(map[sql.Row]bool, len(keys))
		for _, k := range keys {
			keyMap[k] = true
		}

		replayCtx := &packet.ReplayPiece_Partial{
			Partial: &packet.ReplayPiece_ContextPartial{
				ForKeys:         keyMap,
				RequestingShard: d.shardn(),
				Unishard:        false,
				Ignore:          false,
			}}
		link := &packet.Link{Src: path.Path[0].Node, Dst: path.Path[0].Node}
		return d.sendReplayUnified(ctx, tag, link, path.Upquery, replayCtx, keyMap)
	default:
		panic("unreachable")
	}
}

func (d *Domain) sendPartialReplaySlow(ctx context.Context, tag dataflow.Tag, keys []sql.Row, path *replay.Path, trigger *replay.TriggerEndpoint) error {
	var askShardByKeyI *int
	switch trigger.SourceSelection.Kind {
	case replay.SourceSelectionAllShards:
	case replay.SourceSelectionSameShard:
		// note that we "ask all" here because we're not indexing the vector by the
		// key's shard index. unipath will still be set to true though, since
		// options.len() == 1.
	case replay.SourceSelectionKeyShard:
		askShardByKeyI = &trigger.SourceSelection.KeyIdxToShard
	}

	if askShardByKeyI == nil && len(trigger.Clients) != 1 {
		d.concurrentReplays++
		request := makePartialRequest(d.shard, tag, keys, false)

		for _, tri := range trigger.Clients {
			if _, err := tri.StartPartialReplay(ctx, request); err != nil {
				// we're shutting down, it's fine
				d.log.Error("shard call failed", zap.Error(err))
				return nil
			}
		}
		return nil
	}

	d.concurrentReplays++

	if len(trigger.Clients) == 1 {
		request := makePartialRequest(d.shard, tag, keys, true)
		if _, err := trigger.Clients[0].StartPartialReplay(ctx, request); err != nil {
			// we're shutting down, it's fine
			d.log.Error("shard call failed", zap.Error(err))
			return nil
		}
	} else if askShardByKeyI != nil {
		var hasher vthash.Hasher
		schema := d.nodes.Get(path.Path[0].Node).Schema()
		shards := make(map[uint][]sql.Row)
		for _, key := range keys {
			shard := key.ShardValue(&hasher, *askShardByKeyI, schema[*askShardByKeyI], uint(len(trigger.Clients)))
			shards[shard] = append(shards[shard], key)
		}
		for shard, keys := range shards {
			request := makePartialRequest(d.shard, tag, keys, true)
			if _, err := trigger.Clients[shard].StartPartialReplay(ctx, request); err != nil {
				// we're shutting down, it's fine
				d.log.Error("shard call failed", zap.Error(err))
				return nil
			}
		}
	} else {
		panic("unreachable")
	}
	return nil
}

func (d *Domain) handlePartialReplay(request *packet.PartialReplayRequest) error {
	for _, key := range request.Keys {
		err := d.seedReplay(request.Tag, key, request.Unishard, request.RequestingShard)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Domain) seedReplay(tag dataflow.Tag, key sql.Row, unishard bool, requestingShard uint) error {
	rpath := d.replayPaths[tag]
	switch rpath.Trigger.Kind {
	// from Noria: we're always batching both local and global replays
	case replay.TriggerStart, replay.TriggerLocal:
		// TODO: maybe delay this seed request so that we can batch respond later?
		ts := tagshard{tag, requestingShard}
		buf, ok := d.bufferedReplayRequests[ts]
		if !ok {
			buf = &bufferedReplay{
				Keys:     make(map[sql.Row]bool),
				Unishard: unishard,
			}
			d.bufferedReplayRequests[ts] = buf

			time.AfterFunc(d.replayBatchTimeout, func() {
				d.chanElapsedReplays <- ts
			})
		}

		buf.Keys[key] = true
		return nil
	default:
		panic(fmt.Sprintf("unexpected trigger %s", rpath.Trigger.Kind))
	}
}

func (d *Domain) seedAll(ctx context.Context, tag dataflow.Tag, requestingShard uint, keys map[sql.Row]bool, unishard bool, executor processing.Executor) error {
	repl, ok := d.replayPaths[tag]
	if !ok {
		panic("missing replay path")
	}

	switch repl.Trigger.Kind {
	case replay.TriggerStart:
		if repl.Upquery != nil {
			replayCtx := &packet.ReplayPiece_Partial{
				Partial: &packet.ReplayPiece_ContextPartial{
					ForKeys:         keys,
					RequestingShard: requestingShard,
					Unishard:        unishard,
					Ignore:          false,
				},
			}
			link := &packet.Link{Src: repl.Source, Dst: repl.Path[0].Node}
			return d.sendReplayUnified(ctx, tag, link, repl.Upquery, replayCtx, keys)
		}
	case replay.TriggerLocal:
	default:
		panic("replay path should be Local or Start")
	}

	cols := repl.Trigger.Cols
	stat := d.state.Get(repl.Source)
	if stat == nil {
		panic("migration replay path started with non-materialized node")
	}

	var rs []sql.Record
	var misses = make(map[sql.Row]bool)

	if log := d.log.Check(zapcore.DebugLevel, "looking up in memory"); log != nil {
		log.Write(zap.Uint32("source", uint32(repl.Source)), sql.ZapRows("keys", maps.Keys(keys)))
	}

	for key := range keys {
		res, ok := stat.Lookup(cols, key)
		if !ok {
			if log := d.log.Check(zapcore.DebugLevel, "missed lookup"); log != nil {
				log.Write(zap.Uint32("source", uint32(repl.Source)), key.Zap("key"))
			}
			delete(keys, key)
			misses[key] = true
			continue
		}

		if log := d.log.Check(zapcore.DebugLevel, "succeeded lookup"); log != nil {
			log.Write(zap.Int("len", res.Len()), zap.Uint32("source", uint32(repl.Source)), key.Zap("key"))
		}

		res.ForEach(func(r sql.Row) {
			rs = append(rs, d.seedRow(repl.Source, r))
		})
	}
	if len(misses) == 0 {
		misses = nil
	}

	var m *packet.ReplayPiece
	if len(keys) > 0 {
		m = &packet.ReplayPiece{
			Link:    &packet.Link{Src: repl.Source, Dst: repl.Path[0].Node},
			Tag:     tag,
			Records: rs,
			Context: &packet.ReplayPiece_Partial{Partial: &packet.ReplayPiece_ContextPartial{
				ForKeys:         keys,
				RequestingShard: requestingShard,
				Unishard:        unishard,
				Ignore:          false,
			}},
		}
	}

	for key := range misses {
		// we have missed in our lookup, so we have a partial replay through a partial replay
		// trigger a replay to source node, and enqueue this request.
		if err := d.onReplayMiss(ctx, repl.Source, cols, key, []sql.Row{key}, unishard, true, requestingShard, tag); err != nil {
			return err
		}
	}

	if m != nil {
		return d.handleReplay(ctx, m, executor)
	}
	return nil
}

func (d *Domain) onReplayMiss(ctx context.Context, missIn dataflow.LocalNodeIdx, missColumns []int, replayKey sql.Row, missKeys []sql.Row, unishard, waiters bool, shard uint, neededFor dataflow.Tag) error {
	// when the replay eventually succeeds, we want to re-do the replay.
	w, ok := d.waiting[missIn]
	if ok {
		delete(d.waiting, missIn)
	} else {
		w = &waiting{
			holes: map[redo]int{},
			redos: map[hole]map[redo]struct{}{},
		}
	}

	redundant := false
	redoEntry := redo{
		tag:             neededFor,
		replayKey:       replayKey,
		requestingShard: shard,
		unishard:        unishard,
		waiters:         waiters,
	}

	for _, missKey := range missKeys {
		holeEntry := hole{common.ColumnsFrom(missColumns), missKey}
		e, found := w.redos[holeEntry]
		if found {
			// we have already requested backfill of this key
			// remember to notify this Redo when backfill completes
			if _, existing := e[redoEntry]; !existing {
				e[redoEntry] = struct{}{}
				// this Redo should wait for this backfill to complete before redoing
				w.holes[redoEntry]++
			}
			redundant = true
		} else {
			// we haven't already requested backfill of this key
			// remember to notify this Redo when backfill completes
			e = map[redo]struct{}{redoEntry: {}}
			w.redos[holeEntry] = e
			// this Redo should wait for this backfill to complete before redoing
			w.holes[redoEntry]++
		}
	}

	d.waiting[missIn] = w
	if redundant {
		return nil
	}

	return d.findTagsAndReplay(ctx, missKeys, missColumns, missIn)
}

func (d *Domain) seedRow(source dataflow.LocalNodeIdx, row sql.Row) sql.Record {
	if inj, ok := d.ingressInject[source]; ok {
		var v = make([]sql.Value, 0, inj.start+len(inj.defaults))
		v = append(v, row.ToValues()...)
		v = append(v, inj.defaults...)
		return sql.NewRecord(v, true)
	}
	return row.ToRecord(true)
}

func (d *Domain) finishedPartialReplay(ctx context.Context, tag dataflow.Tag, num int) error {
	rp := d.replayPaths[tag]
	switch rp.Trigger.Kind {
	case replay.TriggerEnd:
		// A backfill request we made to another domain was just satisfied!
		// We can now issue another request from the concurrent replay queue.
		// However, since unions require multiple backfill requests, but produce only one
		// backfill reply, we need to check how many requests we're now free to issue. If
		// we just naively release one slot here, a union with two parents would mean that
		// `self.concurrent_replays` constantly grows by +1 (+2 for the backfill requests,
		// -1 when satisfied), which would lead to a deadlock!
		requestsSatisfied := 0
		last := rp.Path[len(rp.Path)-1]

		if cs, ok := d.replayPathsByDst[last.Node]; ok {
			if tags, ok := cs[common.ColumnsFrom(last.PartialKey)]; ok {
				for _, tag := range tags {
					if d.replayPaths[tag].Trigger.Kind == replay.TriggerEnd {
						requestsSatisfied++
					}
				}
			}
		}

		// we also sent that many requests *per key*.
		requestsSatisfied = requestsSatisfied * num

		d.concurrentReplays -= requestsSatisfied
		if d.concurrentReplays < 0 {
			// TODO: figure out why this can underflow
			d.concurrentReplays = 0
		}

		var perTag = make(map[dataflow.Tag][]sql.Row)
		for d.concurrentReplays < d.maxConcurrentReplays {
			if d.replayRequestQueue.Len() == 0 {
				break
			}
			queued := d.replayRequestQueue.PopFront()
			perTag[queued.Tag] = append(perTag[queued.Tag], queued.Data...)
		}

		for tag, keys := range perTag {
			if err := d.sendPartialReplayRequest(ctx, tag, keys); err != nil {
				return err
			}
		}
	case replay.TriggerLocal:
		// didn't count against our quote, so we're not decrementing
	case replay.TriggerExternal:
		// this should have been handled already
	default:
		panic("unreachable")
	}
	return nil
}

func (d *Domain) handleReplayFromInternalNode(ctx context.Context, recordsToReplay []sql.Record, tx boostrpc.DomainClient, tag dataflow.Tag, link *packet.Link) {
	const BatchSize = 256

	for ctx.Err() == nil && len(recordsToReplay) > 0 {
		var last bool
		var records []sql.Record

		if len(recordsToReplay) >= BatchSize {
			records = slices.Clone(recordsToReplay[:BatchSize])
			recordsToReplay = recordsToReplay[BatchSize:]
		} else {
			last = true
			records = slices.Clone(recordsToReplay)
			recordsToReplay = nil
		}

		pkt := &packet.ReplayPiece{
			Link:    link.Clone(),
			Tag:     tag,
			Records: records,
			Context: &packet.ReplayPiece_Regular{
				Regular: &packet.ReplayPiece_ContextRegular{
					Last: last,
				},
			},
		}

		if _, err := tx.SendReplayPiece(ctx, pkt); err != nil {
			d.log.Error("failed to perform replay from internal node", zap.Error(err), tag.Zap())
			return
		}
	}
}

func (d *Domain) handleReplayFromExternal(ctx context.Context, up *upquery.Upquery, tx boostrpc.DomainClient, tag dataflow.Tag, link *packet.Link) {
	if up == nil {
		panic("handleReplayFromExternal: no upquery?")
	}

	query, bvars := up.For(nil)

	var mergedGtid mysql.Mysql56GTIDSet
	err := d.performUpquery(ctx, query, bvars, func(gtid string, records []sql.Record) error {
		gtidSet, err := mysql.ParseMysql56GTIDSet(gtid)
		if err != nil {
			return err
		}
		mergedGtid = mergedGtid.Union(gtidSet).(mysql.Mysql56GTIDSet)

		if len(records) == 0 {
			return nil
		}
		pkt := &packet.ReplayPiece{
			Link:    link.Clone(),
			Tag:     tag,
			Records: slices.Clone(records),
			Context: &packet.ReplayPiece_Regular{
				Regular: &packet.ReplayPiece_ContextRegular{
					Last: false,
				},
			},
			External: &packet.ReplayPiece_External{
				Gtid:         gtid,
				Intermediate: up.IsIntermediate(),
			},
		}
		_, err = tx.SendReplayPiece(ctx, pkt)
		return err
	})

	if err == nil {
		pkt := &packet.ReplayPiece{
			Link: link.Clone(),
			Tag:  tag,
			Context: &packet.ReplayPiece_Regular{
				Regular: &packet.ReplayPiece_ContextRegular{
					Last: true,
				},
			},
			External: &packet.ReplayPiece_External{
				Gtid:         mergedGtid.String(),
				Intermediate: up.IsIntermediate(),
			},
		}
		_, err = tx.SendReplayPiece(ctx, pkt)
	}
	if err != nil {
		d.log.Error("failed to perform replay from external table", zap.Error(err), tag.Zap())
	}
}

func (d *Domain) handleStartReplay(ctx context.Context, startReplay *packet.StartReplayRequest, ex processing.Executor) error {
	path := d.replayPaths[startReplay.Tag]

	var recordsToReplay []sql.Record
	var shouldReplay bool

	n := d.nodes.Get(startReplay.From)

	switch {
	case n.IsTable():
		shouldReplay = true
	case n.IsInternal() || n.IsIngress():
		if path.Trigger.Kind == replay.TriggerExternal {
			shouldReplay = true
		} else {
			// we know that the node is materialized, as the migration coordinator
			// picks path that originate with materialized nodes. if this weren't the
			// case, we wouldn't be able to do the replay, and the entire migration
			// would fail.
			//
			// we clone the entire state so that we can continue to occasionally
			// process incoming updates to the domain without disturbing the state that
			// is being replayed.
			recordsToReplay = d.state.Get(startReplay.From).ClonedState()
			shouldReplay = len(recordsToReplay) > 0
		}
	case n.IsReader():
		shouldReplay = true
		if path.Trigger.Kind != replay.TriggerExternal {
			panic("replay from reader without external trigger")
		}
	default:
		panic("starting replay from unexpected node type")
	}

	link := &packet.Link{Src: startReplay.From, Dst: d.replayPaths[startReplay.Tag].Path[0].Node}

	// we're been given an entire state snapshot, but we need to digest it
	// piece by piece spawn off a thread to do that chunking. however, before
	// we spin off that thread, we need to send a single Replay message to tell
	// the target domain to start buffering everything that follows. we can't
	// do that inside the thread, because by the time that thread is scheduled,
	// we may already have processed some other messages that are not yet a
	// part of state.
	m := &packet.ReplayPiece{
		Link:    link,
		Tag:     startReplay.Tag,
		Records: nil,
		Context: &packet.ReplayPiece_Regular{
			Regular: &packet.ReplayPiece_ContextRegular{
				Last: !shouldReplay,
			},
		},
		External: &packet.ReplayPiece_External{
			Intermediate: path.Upquery.IsIntermediate(),
		},
	}

	if err := d.handleReplay(ctx, m, ex); err != nil {
		return err
	}

	if shouldReplay {
		replayTx, err := d.coordinator.GetClient(d.index, d.shardn())
		if err != nil {
			return err
		}

		if path.Upquery != nil {
			go d.handleReplayFromExternal(ctx, path.Upquery, replayTx, startReplay.Tag, link)
		} else {
			go d.handleReplayFromInternalNode(ctx, recordsToReplay, replayTx, startReplay.Tag, link)
		}
	}

	return nil
}

func (d *Domain) handleFinishReplay(ctx context.Context, finishReplay *packet.FinishReplayRequest, ex processing.Executor) error {
	var finished bool
	if d.replaying != nil {
		d.replaying.Passes++

		handle := d.replaying.Buffered.Len()
		if handle > 100 {
			handle /= 2
		}

		gtid, err := mysql.ParseMysql56GTIDSet(finishReplay.Gtid)
		if err != nil {
			return err
		}

		for d.replaying.Buffered.Len() > 0 {
			m := d.replaying.Buffered.PopFront()

			switch m := m.Inner.(type) {
			case *packet.Message:
				g, err := mysql.ParseMysql56GTIDSet(m.Gtid)
				if err != nil {
					return err
				}
				if gtid.Contains(g) {
					continue
				}
			}

			// some updates were propagated to this node during the migration. we need to
			// replay them before we take even newer updates. however, we don't want to
			// completely block the domain data channel, so we only process a few backlogged
			// updates before yielding to the main loop (which might buffer more things).
			if err := d.dispatch(ctx, m, ex); err != nil {
				return err
			}

			handle--
			if handle == 0 {
				// we want to make sure we actually drain the backlog we've accumulated
				// but at the same time we don't want to completely stall the system
				// therefore we only handle half the backlog at a time
				break
			}
		}

		finished = d.replaying.Buffered.Len() == 0
	} else {
		panic("received FinishReplay but nothing was being replayed")
	}

	if finished {
		d.log.Debug("node is fully up to date", finishReplay.Node.Zap(), zap.Int("passes", d.replaying.Passes), finishReplay.Tag.Zap())
		d.replaying = nil

		if finishReplay.NotifyDone {
			d.finishedReplays <- finishReplay.Tag
		}
	} else {
		d.delayForSelf(finishReplay)
	}
	return nil
}

func (d *Domain) sendReplayUnified(ctx context.Context, tag dataflow.Tag, link *packet.Link, up *upquery.Upquery, replayCtx packet.ReplayContext, keys map[sql.Row]bool) error {
	var slot uint8

	select {
	case slot = <-d.upquerySlots:
	case <-ctx.Done():
		return ctx.Err()
	}

	gt := d.gtidTrackers.Get(link.Src)
	if gt == nil {
		panic(fmt.Sprintf("no gtid tracker for node %d", link.Src))
	}

	gt.BeginUpquery(ctx, tag, slot, keys)

	go func() {
		replayPiece := &packet.ReplayPiece{
			Link:    link,
			Tag:     tag,
			Records: nil,
			Context: replayCtx,
			External: &packet.ReplayPiece_External{
				Gtid:         "",
				Slot:         uint32(slot),
				Intermediate: up.IsIntermediate(),
				Failed:       false,
			},
		}

		query, bvars := up.For(keys)
		err := d.performUpquery(ctx, query, bvars, func(gtid string, records []sql.Record) error {
			replayPiece.External.Gtid = gtid
			replayPiece.Records = append(replayPiece.Records, records...)
			return nil
		})
		if err != nil {
			replayPiece.External.Failed = true
			d.log.Error("upquery failed", zap.Error(err))
		}

		d.stats.onUpqueryFinished(err, len(replayPiece.Records))

		d.delayForSelf(replayPiece)
	}()

	return nil
}
