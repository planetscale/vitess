package worker

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vthash"
)

type Table struct {
	desc   *boostpb.TableDescriptor
	shards []boostrpc.DomainClient
}

func NewTableClientFromProto(pb *boostpb.TableDescriptor, rpcs *common.SyncMap[string, boostrpc.DomainClient]) (*Table, error) {
	var shards = make([]boostrpc.DomainClient, 0, len(pb.Txs))

	for _, dom := range pb.Txs {
		conn, err := rpcs.GetOrSet(dom.Addr, func() (boostrpc.DomainClient, error) {
			return boostrpc.NewRemoteDomainClient(dom.Id, dom.Shard, dom.Addr)
		})
		if err != nil {
			return nil, err
		}

		shards = append(shards, conn)
	}

	return &Table{
		desc:   pb,
		shards: shards,
	}, nil
}

func (t *Table) Insert(ctx context.Context, row sqltypes.Row) error {
	return t.input(ctx, []*boostpb.TableOperation{
		{
			Tag: boostpb.TableOperation_Insert,
			Key: boostpb.RowFromVitess(row),
		},
	})
}

func (t *Table) BatchInsert(ctx context.Context, rows []sqltypes.Row) error {
	var batch = make([]*boostpb.TableOperation, 0, len(rows))
	for _, row := range rows {
		batch = append(batch, &boostpb.TableOperation{
			Tag: boostpb.TableOperation_Insert,
			Key: boostpb.RowFromVitess(row),
		})
	}
	return t.input(ctx, batch)
}

func (t *Table) Delete(ctx context.Context, key []sqltypes.Value) error {
	return t.input(ctx, []*boostpb.TableOperation{
		{
			Tag: boostpb.TableOperation_Delete,
			Key: boostpb.RowFromVitess(key),
		},
	})
}

func (t *Table) TableName() string {
	return t.desc.TableName
}

func (t *Table) Columns() []string {
	return t.desc.Columns
}

func (t *Table) input(ctx context.Context, in []*boostpb.TableOperation) error {
	ncols := len(t.desc.Columns) + len(t.desc.Dropped)
	for _, op := range in {
		switch op.Tag {
		case boostpb.TableOperation_Insert:
			if op.Key.Len() != ncols {
				return fmt.Errorf("wrong column count: expected %d, got %d", ncols, len(op.Key))
			}
		case boostpb.TableOperation_Delete:
			if op.Key.Len() != len(t.desc.Key) {
				return fmt.Errorf("wrong column count: expected %d, got %d", len(t.desc.Key), len(op.Key))
			}
		default:
			panic("unimplemented")
		}
	}

	if len(t.shards) == 1 {
		return t.shards[0].ProcessAsync(ctx, &boostpb.Packet{
			Inner: &boostpb.Packet_Input_{Input: &boostpb.Packet_Input{Inner: &boostpb.FlowInput{
				Dst:  t.desc.Addr,
				Data: in,
			}}},
		})
	}

	if t.desc.Key == nil {
		panic("sharded base without a key?")
	}
	if len(t.desc.Key) != 1 {
		panic("base sharded by complex key??")
	}

	var (
		hasher      vthash.Hasher
		keyCol      = t.desc.Key[0]
		keyType     = t.desc.Schema[keyCol]
		shardWrites = make([][]*boostpb.TableOperation, len(t.shards))
	)

	for _, r := range in {
		var col int
		switch r.Tag {
		case boostpb.TableOperation_Insert:
			col = keyCol
		case boostpb.TableOperation_Delete:
			col = 0
		default:
			panic("unimplemented")
		}

		shardn := r.Key.ShardValue(&hasher, col, keyType, uint(len(t.shards)))
		shardWrites[shardn] = append(shardWrites[shardn], r)
	}

	for s, rs := range shardWrites {
		if len(rs) == 0 {
			continue
		}
		err := t.shards[s].ProcessAsync(ctx, &boostpb.Packet{
			Inner: &boostpb.Packet_Input_{Input: &boostpb.Packet_Input{Inner: &boostpb.FlowInput{
				Dst:  t.desc.Addr,
				Data: rs,
			}}},
		})
		if err != nil {
			return err
		}
	}
	return nil

}
