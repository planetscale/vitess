package flownode

import (
	"context"
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
)

type FlowInput struct {
	Packet     packet.ActiveFlowPacket
	KeyedBy    []int
	State      *state.Map
	Nodes      *Map
	OnShard    *uint
	Swap       bool
	ReplayPath *replay.Path
	Ex         processing.Executor
}

type FlowOutput struct {
	Misses   []processing.Miss
	Lookups  []processing.Lookup
	Captured map[sql.Row]bool
}

func (n *Node) processMidflow(input *FlowInput, output *FlowOutput, repl replay.Context) error {
	addr := n.LocalAddr()

	switch impl := n.impl.(type) {
	case ingredientEmptyState:
		input.Packet.ReplaceRecords(func(rs []sql.Record) []sql.Record {
			return impl.InitEmptyState(rs, repl, input.State.Get(addr))
		})
	}

	var tag = dataflow.TagNone
	if partial := repl.Partial; partial != nil {
		tag = partial.Tag
	}

	Materialize(input, output, addr, tag)
	return nil
}

func (n *Node) processReplacement(input *FlowInput, replace *packet.ReplayPiece_ContextReplace) error {
	st := input.State.Get(n.LocalAddr())

	original := input.Packet.Inner.(*packet.ReplayPiece)
	newRecords := st.Replace(original.Records)

	input.Packet.Inner = &packet.Message{
		Link:    replace.OriginalLink,
		Records: newRecords,
		Gtid:    original.External.GetGtid(),
	}
	return nil
}

func (n *Node) processInternal(ctx context.Context, input *FlowInput, output *FlowOutput, i Internal) error {
	var (
		addr         = n.LocalAddr()
		capturedFull bool
		captured     = make(map[sql.Row]bool)
		misses       []processing.Miss
		lookups      []processing.Lookup
		data         *[]sql.Record
		repl         replay.Context
		m            = input.Packet
		from         = m.Src()
	)

	switch m := m.Inner.(type) {
	case *packet.ReplayPiece:
		switch rctx := m.Context.(type) {
		case *packet.ReplayPiece_Partial:
			data = &m.Records
			repl.Partial = &replay.Partial{
				KeyCols:         input.KeyedBy,
				Keys:            rctx.Partial.ForKeys,
				RequestingShard: rctx.Partial.RequestingShard,
				Unishard:        rctx.Partial.Unishard,
				Tag:             m.Tag,
			}

		case *packet.ReplayPiece_Regular:
			data = &m.Records
			repl.Regular = &replay.Regular{Last: rctx.Regular.Last}

		case *packet.ReplayPiece_Replace:
			return n.processReplacement(input, rctx.Replace)

		default:
			panic(fmt.Sprintf("unexpected replay context %T", m.Context))
		}
		if m.External != nil && m.External.Intermediate {
			return n.processMidflow(input, output, repl)
		}
	case *packet.Message:
		data = &m.Records
	default:
		panic("unexpected packet type for Internal processing")
	}

	var setReplayLast *bool
	oldData := *data

	if raw, ok := i.(ingredientInputRaw); ok {
		res, err := raw.OnInputRaw(input.Ex, from, oldData, repl, input.Nodes, input.State)
		if err != nil {
			return err
		}
		switch res := res.(type) {
		case *processing.Result:
			*data = res.Records
			lookups = res.Lookups
			misses = res.Misses
		case *processing.ReplayPiece:
			// we already know that m must be a ReplayPiece since only a
			// ReplayPiece can release a ReplayPiece.
			// NOTE: no misses or lookups here since this is a union
			*data = res.Rows
			captured = res.Captured

			if rpiece := m.GetReplayPiece(); rpiece != nil {
				rpiece.GetPartial().ForKeys = res.Keys
			} else {
				panic("?? should be ReplayPiece")
			}
		case *processing.CapturedFull:
			capturedFull = true
		case *processing.FullReplay:
			*data = res.Records
			setReplayLast = &res.Last
		default:
			panic("unsupported")
		}
	} else {
		res, err := i.OnInput(n, input.Ex, from, oldData, repl, input.Nodes, input.State)
		if err != nil {
			return err
		}
		*data = res.Records
		lookups = res.Lookups
		misses = res.Misses
	}

	if setReplayLast != nil {
		m.GetReplayPiece().GetRegular().Last = *setReplayLast
	}

	if rpiece := m.GetReplayPiece(); rpiece != nil {
		if partial := rpiece.GetPartial(); partial != nil {
			// hello, it's me again.
			//
			// on every replay path, there are some number of shard mergers, and
			// some number of sharders.
			//
			// if the source of a replay is sharded, and the upquery key matches
			// the sharding key, then only the matching shard of the source will be
			// queried. in that case, the next shard merger (if there is one)
			// shouldn't wait for replays from other shards, since none will
			// arrive. the same is not true for any _subsequent_ shard mergers
			// though, since sharders along a replay path send to _all_ shards
			// (modulo the last one if the destination is sharded, but then there
			// is no shard merger after it).
			//
			// to ensure that this is in fact what happens, we need to _unset_
			// unishard once we've passed the first shard merger, so that it is not
			// propagated to subsequent unions.
			if u, ok := i.(*Union); ok {
				if u.IsShardMerger() {
					partial.Unishard = false
				}
			}
		}
	}

	if capturedFull {
		input.Packet.Clear()
		return nil
	}

	var tag dataflow.Tag = dataflow.TagNone
	if rpiece := input.Packet.GetReplayPiece(); rpiece != nil {
		// NOTE: non-partial replays shouldn't be materialized only for a
		// particular index, and so the tag shouldn't be forwarded to the
		// materialization code. this allows us to keep some asserts deeper in
		// the code to check that we don't do partial replays to non-partial
		// indices, or for unknown tags.
		if rpiece.GetPartial() != nil {
			tag = input.Packet.Tag()
		}
	}

	Materialize(input, output, addr, tag)

	for i, miss := range misses {
		if miss.On != addr {
			rerouteMiss(input.Nodes, &misses[i])
		}
	}

	output.Misses = misses
	output.Lookups = lookups
	output.Captured = captured
	return nil
}

func rerouteMiss(nodes *Map, miss *processing.Miss) {
	node := nodes.Get(miss.On)
	if node.IsInternal() && node.CanQueryThrough() {
		var newparent *dataflow.IndexPair

		for colI, col := range miss.LookupIdx {
			parents := node.Resolve(col)
			if len(parents) != 1 {
				panic("query-through with more than one parent")
			}

			parentGlobal := parents[0].Node
			parentCol := parents[0].Column

			if newparent != nil {

			} else {
				pair := dataflow.NewIndexPair(parentGlobal)
				nodes.ForEach(func(_ dataflow.LocalNodeIdx, n *Node) bool {
					if n.GlobalAddr() == parentGlobal {
						pair.SetLocal(n.LocalAddr())
						return false
					}
					return true
				})
				if !pair.HasLocal() {
					panic("failed to find global parent")
				}
				newparent = &pair
			}

			miss.LookupIdx[colI] = parentCol
		}

		miss.On = newparent.AsLocal()
		rerouteMiss(nodes, miss)
	}
}

func (n *Node) Process(ctx context.Context, input *FlowInput, output *FlowOutput) error {
	defer func() {
		n.Stats.Processed++
	}()

	var addr = n.LocalAddr()
	var gaddr = n.index.AsGlobal()

	switch inner := n.impl.(type) {
	case *Ingress:
		tag := input.Packet.Tag()
		Materialize(input, output, addr, tag)
	case *Table:
		// external tables do no processing
		return nil
	case *Reader:
		return inner.Process(ctx, &input.Packet, input.Swap)
	case *Egress:
		shard := uint(0)
		if input.OnShard != nil {
			shard = *input.OnShard
		}
		return inner.Process(ctx, &input.Packet, shard, input.Ex)
	case *Sharder:
		var isLastSharderForTag *bool
		if replayPath := input.ReplayPath; replayPath != nil {
			if replayPath.PartialUnicastSharder != graph.InvalidNode {
				match := replayPath.PartialUnicastSharder == gaddr
				isLastSharderForTag = &match
			}
		}
		return inner.Process(ctx, &input.Packet, addr, n.Schema(), input.OnShard != nil, isLastSharderForTag, input.Ex)
	case Internal:
		return n.processInternal(ctx, input, output, inner)
	case *Dropped:
		input.Packet.Clear()
	default:
		panic("unexpected node type")
	}
	return nil
}

func (n *Node) ProcessEviction(ctx context.Context, from dataflow.LocalNodeIdx, keyColumns []int, keys *[]sql.Row, tag dataflow.Tag, onShard *uint, ex processing.Executor) error {
	addr := n.LocalAddr()

	switch inner := n.impl.(type) {
	case *Egress:
		pkt := packet.ActiveFlowPacket{
			Inner: &packet.EvictKeysRequest{
				Link: &packet.Link{Src: addr, Dst: addr},
				Tag:  tag,
				Keys: slices.Clone(*keys),
			},
		}
		return inner.Process(ctx, &pkt, common.UnwrapOr(onShard, 0), ex)
	case *Sharder:
		inner.ProcessEviction(keyColumns, keys, addr, onShard != nil, ex)
		return nil
	case Internal:
		if union, ok := inner.(*Union); ok {
			union.OnEviction(from, tag, *keys)
		}
		return nil
	case *Reader:
		inner.OnEviction(*keys)
		return nil
	case *Ingress, *Dropped:
		return nil
	// noop
	default:
		panic("cannot evict on this kind of node")
	}
}

func Materialize(input *FlowInput, output *FlowOutput, addr dataflow.LocalNodeIdx, partial dataflow.Tag) {
	st := input.State.Get(addr)
	if st == nil {
		return
	}

	onMiss := func(tag dataflow.Tag, key []int, r sql.Record) {
		output.Misses = append(output.Misses, processing.Miss{
			On:         addr,
			LookupIdx:  key,
			LookupCols: key,
			ReplayCols: input.KeyedBy,
			Record:     sql.HashedRecord{Record: r},
			ForceTag:   tag,
		})
	}

	input.Packet.ReplaceRecords(func(rs []sql.Record) []sql.Record {
		return st.ProcessRecords(rs, partial, onMiss)
	})
}
