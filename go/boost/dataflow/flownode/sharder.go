package flownode

import (
	"context"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/vt/vthash"
)

var _ NodeImpl = (*Sharder)(nil)

type SharderTx = packet.UpdateSharderRequest_Tx

type Sharder struct {
	txs     []SharderTx
	sharded map[uint]packet.ActiveFlowPacket
	shardBy int
}

func NewSharder(col int) *Sharder {
	return &Sharder{
		sharded: make(map[uint]packet.ActiveFlowPacket),
		shardBy: col,
	}
}

func NewSharderFromProto(psharder *flownodepb.Node_Sharder) *Sharder {
	sharder := NewSharder(psharder.ShardBy)
	return sharder
}

func (s *Sharder) ToProto() *flownodepb.Node_Sharder {
	if len(s.sharded) > 0 || len(s.txs) > 0 {
		panic("unsupported")
	}
	return &flownodepb.Node_Sharder{
		ShardBy: s.shardBy,
	}
}

func (s *Sharder) dataflow() {}

func (s *Sharder) ShardedBy() int {
	return s.shardBy
}

func (s *Sharder) Process(ctx context.Context, pkt *packet.ActiveFlowPacket, index dataflow.LocalNodeIdx, schema []sql.Type, isSharded bool, isLastSharderForTag *bool, output processing.Executor) error {
	var (
		hasher      vthash.Hasher
		m           = pkt.Take()
		shardBy     = s.shardBy
		shardByType = schema[s.shardBy]
		shardCount  = uint(len(s.txs))
	)

	for _, record := range m.TakeRecords() {
		shard := record.Row.ShardValue(&hasher, shardBy, shardByType, shardCount)
		p, ok := s.sharded[shard]
		if !ok {
			p = m.Clone()
			s.sharded[shard] = p
		}
		p.ReplaceRecords(func(rs []sql.Record) []sql.Record {
			return append(rs, record)
		})
	}

	type destination byte
	const (
		destinationAll destination = iota
		destinationOne
		destinationAny
	)

	dest := destinationAny
	var destOne uint

	replayPiece := m.GetReplayPiece()
	if replayPiece != nil && replayPiece.GetRegular() != nil && replayPiece.GetRegular().Last {
		dest = destinationAll
	} else if replayPiece != nil && replayPiece.GetPartial() != nil {
		if isLastSharderForTag != nil && *isLastSharderForTag {
			dest = destinationOne
			destOne = replayPiece.GetPartial().RequestingShard
		} else {
			dest = destinationAll
		}
	} else {
		if isLastSharderForTag != nil {
			panic("isLastSharderForTag should be missing")
		}
	}

	switch dest {
	case destinationAll:
		for shard := uint(0); shard < uint(len(s.txs)); shard++ {
			if _, exists := s.sharded[shard]; !exists {
				s.sharded[shard] = m.Clone()
			}
		}
	case destinationOne:
		if _, exists := s.sharded[destOne]; !exists {
			s.sharded[destOne] = m.Clone()
		}
		for k := range s.sharded {
			if k != destOne {
				delete(s.sharded, k)
			}
		}
	case destinationAny:
		// noop
	}

	if isSharded {
		// FIXME: we don't know how many shards in the destination domain our sibling Sharders
		// sent to, so we don't know what to put here. we *could* put self.txs.len() and send
		// empty messages to all other shards, which is probably pretty sensible, but that only
		// solves half the problem. the destination shard domains will then recieve *multiple*
		// replay pieces for each incoming replay piece, and needs to combine them somehow.
		// it's unclear how we do that.
		panic("unimplemented")
	}

	for i, tx := range s.txs {
		if shard, ok := s.sharded[uint(i)]; ok {
			delete(s.sharded, uint(i))
			shard.Link().Src = index
			shard.Link().Dst = tx.Local

			if err := output.Send(ctx, *tx.Domain, shard.Inner); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Sharder) AddShardedTx(tx *SharderTx) {
	s.txs = append(s.txs, *tx)
}

func (s *Sharder) ProcessEviction(columns []int, keys *[]sql.Row, addr dataflow.LocalNodeIdx, b bool, ex processing.Executor) {
	panic("unimplemented")
}
