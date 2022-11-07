package flownode

import (
	"context"
	"fmt"
	"sync/atomic"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/dataflow/trace"
	"vitess.io/vitess/go/boost/graph"
)

type FlowInput struct {
	Packet     *boostpb.Packet
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
	Captured map[boostpb.Row]bool
}

func (n *Node) processInternal(input *FlowInput, output *FlowOutput, i Internal) error {
	var addr = n.LocalAddr()
	var capturedFull bool
	var captured = make(map[boostpb.Row]bool)
	var misses []processing.Miss
	var lookups []processing.Lookup

	{
		m := input.Packet
		from := m.Src()

		var data *[]boostpb.Record
		var repl replay.Context

		switch m := m.Inner.(type) {
		case *boostpb.Packet_ReplayPiece_:
			rpiece := m.ReplayPiece
			if partial := rpiece.GetPartial(); partial != nil {
				data = &rpiece.Records
				repl.Partial = &replay.Partial{
					KeyCols:         input.KeyedBy,
					Keys:            partial.ForKeys,
					RequestingShard: partial.RequestingShard,
					Unishard:        partial.Unishard,
					Tag:             m.ReplayPiece.Tag,
				}
			}
			if regular := rpiece.GetRegular(); regular != nil {
				data = &rpiece.Records
				repl.Full = &regular.Last
			}
		case *boostpb.Packet_Message_:
			data = &m.Message.Records
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
			res, err := i.OnInput(n, input.Ex, from, oldData, repl.Key(), input.Nodes, input.State)
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
	}

	if capturedFull {
		input.Packet = nil
		return nil
	}

	var tag boostpb.Tag = boostpb.TagInvalid
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

	input.Packet.MapData(func(records *[]boostpb.Record) {
		Materialize(records, tag, input.State.Get(addr))
	})

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
		var newparent *boostpb.IndexPair

		for colI, col := range miss.LookupIdx {
			parents := node.Resolve(col)
			if len(parents) != 1 {
				panic("query-through with more than one parent")
			}

			parentGlobal := parents[0].Node
			parentCol := parents[0].Column

			if newparent != nil {

			} else {
				pair := boostpb.NewIndexPair(parentGlobal)
				nodes.ForEach(func(n *Node) bool {
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
	if trace.T {
		var span *trace.Span
		ctx, span = trace.WithSpan(ctx, "Node.Process", input.Packet)

		span.Arg("processor", fmt.Sprintf("%T", n.impl))

		defer func() {
			span.Arg("output", input.Packet).Close()
		}()
	}

	defer atomic.AddUint64(&n.Stats.Processed, 1)

	var addr = n.LocalAddr()
	var gaddr = n.index.AsGlobal()
	var ignore bool

	if msg := input.Packet.GetMessage(); msg != nil {
		ignore = msg.Ignored
		if ignore {
			if st := input.State.Get(addr); st != nil && st.IsPartial() {
				return nil
			}
		}
	}

	switch inner := n.impl.(type) {
	case *Ingress:
		tag := input.Packet.Tag()
		input.Packet.MapData(func(records *[]boostpb.Record) {
			Materialize(records, tag, input.State.Get(addr))
		})

	case *Base:
		if inputpkt := input.Packet.GetInput(); inputpkt != nil {
			dst := inputpkt.Inner.Dst
			data := inputpkt.Inner.Data

			rs, err := inner.Process(addr, data, input.State)
			if err != nil {
				return err
			}

			// When a replay originates at a base node, we replay the data *through* that
			// same base node because its column set may have changed. However, this replay
			// through the base node itself should *NOT* update the materialization,
			// because otherwise it would duplicate each record in the base table every
			// time a replay happens!
			//
			// So: only materialize if the message we're processing is not a replay!
			if input.KeyedBy == nil {
				Materialize(&rs, boostpb.TagInvalid, input.State.Get(addr))
			}

			// TODO: sent write-ACKs to all the clients
			input.Packet.Inner = &boostpb.Packet_Message_{
				Message: &boostpb.Packet_Message{
					Link:    &boostpb.Link{Src: dst, Dst: dst},
					Records: rs,
				},
			}
		}
	case *ExternalBase:
		rs, err := inner.Process(ctx, &input.Packet)
		if err != nil {
			return err
		}

		input.Packet.Inner = rs
	case *Reader:
		if ignore && inner.IsPartial() {
			return nil
		}
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
		return n.processInternal(input, output, inner)
	case *Dropped:
		input.Packet = nil
	default:
		panic("unexpected node type")
	}
	return nil
}

func (n *Node) ProcessEviction(ctx context.Context, from boostpb.LocalNodeIndex, keyColumns []int, keys *[]boostpb.Row, tag boostpb.Tag, onShard *uint, ex processing.Executor) error {
	addr := n.LocalAddr()

	switch inner := n.impl.(type) {
	case *Egress:
		var pkt = &boostpb.Packet{
			Inner: &boostpb.Packet_EvictKeys_{
				EvictKeys: &boostpb.Packet_EvictKeys{
					Link: &boostpb.Link{Src: addr, Dst: addr},
					Tag:  tag,
					Keys: slices.Clone(*keys),
				},
			},
		}
		var shard = uint(0)
		if onShard != nil {
			shard = *onShard
		}
		return inner.Process(ctx, &pkt, shard, ex)
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
	case *Base, *Ingress, *Dropped:
		return nil
	// noop
	default:
		panic("cannot evict on this kind of node")
	}
}

func Materialize(rs *[]boostpb.Record, partial boostpb.Tag, st *state.Memory) {
	if st == nil {
		return
	}
	st.ProcessRecords(rs, partial)
}
