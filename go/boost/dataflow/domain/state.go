package domain

import (
	"context"

	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/dataflow/view"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/vt/vthash"
)

func (d *Domain) memstate(node dataflow.LocalNodeIdx) *state.Memory {
	var memstate *state.Memory
	if !d.state.ContainsKey(node) {
		memstate = state.NewMemoryState()
		d.state.Insert(node, memstate)
		d.memstats.Register(d.index, d.shard, d.nodes.Get(node).GlobalAddr(), memstate.StateSizeAtomic())
	} else {
		memstate = d.state.Get(node)
	}
	return memstate
}

func partialGlobalState(
	ctx context.Context,
	coord *boostrpc.ChannelCoordinator,
	node *flownode.Node,
	schema []sql.Type,
	st *packet.PrepareStateRequest_PartialGlobal) (
	view.Reader, *view.Writer, error,
) {
	triggerDomain := st.TriggerDomain.Domain
	shards := st.TriggerDomain.Shards
	k := st.Key
	localAddr := node.LocalAddr()

	var txs []chan []sql.Row
	for shard := uint(0); shard < shards; shard++ {
		tx := make(chan []sql.Row, 8) // TODO: size
		sender, err := coord.GetClient(triggerDomain, shard)
		if err != nil {
			return nil, nil, err
		}

		log := common.Logger(ctx).With(
			zap.Uint("upquery_target_shard", shard),
			triggerDomain.ZapField("upquery_target_domain"))

		go func() {
			defer sender.Close()

			for {
				select {
				case <-ctx.Done():
					return
				case miss := <-tx:
					pkt := &packet.ReaderReplayRequest{
						Node: localAddr,
						Cols: k,
						Keys: miss,
					}
					if _, err := sender.StartReaderReplay(ctx, pkt); err != nil {
						log.Error("failed to process upquery", zap.Error(err))
					}
				}
			}
		}()

		txs = append(txs, tx)
	}

	var (
		shardCount = uint(len(txs))
		onMiss     func(misses []sql.Row) bool
	)

	if shardCount == 1 {
		onMiss = func(misses []sql.Row) bool {
			if len(misses) == 0 {
				return true
			}
			select {
			case txs[0] <- misses:
				return true
			default:
				return false
			}
		}
	} else {
		var hasher vthash.Hasher
		var shardKeyType = schema[k[0]]

		onMiss = func(misses []sql.Row) bool {
			perShard := make(map[uint][]sql.Row)
			for _, miss := range misses {
				if miss.Len() != 1 {
					panic("miss of wrong size")
				}
				shard := miss.ShardValue(&hasher, 0, shardKeyType, shardCount)
				perShard[shard] = append(perShard[shard], miss)
			}
			if len(perShard) == 0 {
				return true
			}
			for shard, m := range perShard {
				select {
				case txs[shard] <- m:
				default:
					return false
				}
			}
			return true
		}
	}

	r, w := view.NewMapView(k, schema, node.AsReader().ViewPlan(), onMiss)
	return r, w, nil
}

func (d *Domain) handlePrepareState(ctx context.Context, pkt *packet.PrepareStateRequest) error {
	node := pkt.Node

	switch st := pkt.State.(type) {
	case *packet.PrepareStateRequest_PartialLocal_:
		memstate := d.memstate(node)
		for _, idx := range st.PartialLocal.Index {
			memstate.AddKey(idx.Key, d.nodes.Get(node).Schema(), idx.Tags, idx.IsPrimary)
		}

	case *packet.PrepareStateRequest_IndexedLocal_:
		memstate := d.memstate(node)
		for _, idx := range st.IndexedLocal.Index {
			memstate.AddKey(idx.Key, d.nodes.Get(node).Schema(), nil, idx.IsPrimary)
		}

	case *packet.PrepareStateRequest_PartialGlobal_:
		n := d.nodes.Get(node)
		r, w, err := partialGlobalState(ctx, d.coordinator, n, n.Schema(), st.PartialGlobal)
		if err != nil {
			return err
		}
		d.readers.Set(ReaderID{st.PartialGlobal.Gid, d.shardn()}, r)
		d.memstats.Register(d.index, d.shard, n.GlobalAddr(), w.StateSizeAtomic())
		n.AsReader().SetWriteHandle(w)

	case *packet.PrepareStateRequest_Global_:
		n := d.nodes.Get(node)
		reader := n.AsReader()
		desc := reader.ViewPlan()

		var r view.Reader
		var w *view.Writer
		if desc.TreeKey != nil {
			r, w = view.NewTreeView(st.Global.Key, n.Schema(), desc)
		} else {
			r, w = view.NewMapView(st.Global.Key, n.Schema(), desc, nil)
		}

		d.readers.Set(ReaderID{st.Global.Gid, d.shardn()}, r)
		d.memstats.Register(d.index, d.shard, n.GlobalAddr(), w.StateSizeAtomic())
		reader.SetWriteHandle(w)
	}

	return nil
}
