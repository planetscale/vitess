package watcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"storj.io/drpc"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/common/metrics"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/proto/query"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/vthash"
)

var (
	StatViewReads      = stats.NewCountersWithSingleLabel("BoostClientViewReads", "The total number of remote view reads calls executed", "BoostQueryPublicId")
	StatViewHits       = stats.NewCountersWithSingleLabel("BoostClientViewHits", "The total number of cache hits from view reads", "BoostQueryPublicId")
	StatViewReadTiming = stats.NewTimings("BoostClientViewReadMs", "Duration for each individual read in the view client", "BoostQueryPublicId")
)

type View struct {
	name      string
	node      graph.NodeIdx
	schema    []*querypb.Field
	keySchema []*querypb.Field
	addrs     []string
	shards    []boostpb.DRPCReaderClient

	topkOrder []boostpb.OrderedColumn
	topkLimit int

	shardKeyType boostpb.Type
	metrics      *scopedMetrics
}

func (v *View) addShards(shards []string, rpcs *common.SyncMap[string, drpc.Conn]) error {
	for _, addr := range shards {
		conn, err := rpcs.GetOrSet(addr, func() (drpc.Conn, error) {
			return boostrpc.NewClientConn(addr)
		})
		if err != nil {
			return err
		}

		v.addrs = append(v.addrs, addr)
		v.shards = append(v.shards, boostpb.NewDRPCReaderClient(conn))
	}
	return nil
}

func NewViewClientFromProto(pb *vtboostpb.Materialization_ViewDescriptor, rpcs *common.SyncMap[string, drpc.Conn]) (*View, error) {
	v := &View{
		name:      pb.Name,
		node:      graph.NodeIdx(pb.Node),
		schema:    pb.Schema,
		keySchema: pb.KeySchema,
		topkLimit: int(pb.TopkLimit),
	}
	if len(pb.Shards) > 1 {
		if len(pb.KeySchema) != 1 {
			return nil, fmt.Errorf("sharded view with composite primary key is unsupported")
		}
		v.shardKeyType = boostpb.TypeFromField(pb.KeySchema[0])
	}
	for i, col := range pb.TopkOrderCols {
		v.topkOrder = append(v.topkOrder, boostpb.OrderedColumn{
			Col:  int(col),
			Desc: pb.TopkOrderDesc[i],
		})
	}
	if err := v.addShards(pb.Shards, rpcs); err != nil {
		return nil, err
	}
	return v, nil
}

func (v *View) PublicQueryID() string {
	if v.metrics == nil {
		return ""
	}
	return v.metrics.publicQueryID
}

func (v *View) CollectMetrics(publicID string, hitrate *metrics.RateCounter) {
	v.metrics = &scopedMetrics{
		publicQueryID: publicID,
		hitrate:       hitrate,
	}
}

func (v *View) LookupByBindVar(ctx context.Context, key []*querypb.BindVariable, block bool) (*sqltypes.Result, error) {
	for i, k := range key {
		if k.Type == sqltypes.Tuple {
			return v.multiLookupForTuple(ctx, i, key, block)
		}
	}

	for i, k := range key {
		if err := v.canCoerce(k.Type, i); err != nil {
			return nil, err
		}
	}

	keypb := boostpb.NewRowBuilder(len(key))
	for _, bvar := range key {
		v, _ := sqltypes.BindVariableToValue(bvar)
		keypb.AddVitess(v)
	}
	return v.lookup(ctx, keypb.Finish(), block)
}

func (v *View) multiLookupForTuple(ctx context.Context, tuplePosition int, key []*querypb.BindVariable, block bool) (*sqltypes.Result, error) {
	queryCount := len(key[tuplePosition].Values)
	keypbs := make([]boostpb.Row, 0, queryCount)

	for q := 0; q < queryCount; q++ {
		keypb := boostpb.NewRowBuilder(len(key))

		for i, bvar := range key {
			var val sqltypes.Value
			if i == tuplePosition {
				val = sqltypes.ProtoToValue(bvar.Values[q])
			} else {
				var err error
				val, err = sqltypes.BindVariableToValue(bvar)
				if err != nil {
					return nil, err
				}
			}
			if err := v.canCoerce(val.Type(), i); err != nil {
				return nil, err
			}
			keypb.AddVitess(val)
		}

		keypbs = append(keypbs, keypb.Finish())
	}

	return v.lookupManyAndMerge(ctx, keypbs, block)
}

func (v *View) Lookup(ctx context.Context, key []sqltypes.Value, block bool) (*sqltypes.Result, error) {
	for i, k := range key {
		if err := v.canCoerce(k.Type(), i); err != nil {
			return nil, err
		}
	}
	return v.lookup(ctx, boostpb.RowFromVitess(key), block)
}

func (v *View) canCoerce(from querypb.Type, fieldPos int) error {
	to := v.keySchema[fieldPos].Type

	switch {
	case sqltypes.IsQuoted(to):
		if !sqltypes.IsQuoted(from) {
			return fmt.Errorf("cannot query field %q with an %s (textual fields can only be queried textually)", v.keySchema[fieldPos].Name, from.String())
		}
	}
	return nil
}

func (v *View) lookup(ctx context.Context, key boostpb.Row, block bool) (*sqltypes.Result, error) {
	request := &boostpb.ViewReadRequest{
		TargetNode:  v.node,
		TargetShard: 0,
		Block:       block,
		Key:         key,
	}

	if shardCount := len(v.shards); shardCount > 1 {
		var hasher vthash.Hasher
		request.TargetShard = request.Key.ShardValue(&hasher, 0, v.shardKeyType, uint(shardCount))
	}

	start := time.Now()
	resp, err := v.shards[request.TargetShard].ViewRead(ctx, request)
	if err != nil {
		return nil, err
	}
	v.metrics.onViewDuration(start)
	v.metrics.onViewRead(1, int(resp.Hits))
	return v.fixResult(resp.Rows), nil
}

func (v *View) LookupByFields(ctx context.Context, keyByName map[string]sqltypes.Value, block bool) (*sqltypes.Result, error) {
	var plainkey = make([]sqltypes.Value, 0, len(v.keySchema))
	for _, key := range v.keySchema {
		v, ok := keyByName[key.Name]
		if !ok {
			return nil, fmt.Errorf("missing key for field %q", key)
		}
		plainkey = append(plainkey, v)
	}
	return v.Lookup(ctx, plainkey, block)
}

func (v *View) lookupManyAndMerge(ctx context.Context, keys []boostpb.Row, block bool) (*sqltypes.Result, error) {
	if len(v.shards) == 1 {
		request := &boostpb.ViewReadManyRequest{
			TargetNode:  v.node,
			TargetShard: 0,
			Block:       block,
			Keys:        keys,
		}

		start := time.Now()
		resp, err := v.shards[0].ViewReadMany(ctx, request)
		if err != nil {
			return nil, err
		}
		v.metrics.onViewDuration(start)
		v.metrics.onViewRead(len(keys), int(resp.Hits))
		return v.fixResult(resp.Rows), nil
	}

	shardKeys := make([][]boostpb.Row, len(v.shards))
	var hasher vthash.Hasher
	for _, key := range keys {
		shardn := key.ShardValue(&hasher, 0, v.shardKeyType, uint(len(v.shards)))
		shardKeys[shardn] = append(shardKeys[shardn], key)
	}

	var mergedResults []boostpb.Row
	var wg errgroup.Group
	var mu sync.Mutex

	for shardi, shardKey := range shardKeys {
		if len(shardKey) == 0 {
			continue
		}

		request := &boostpb.ViewReadManyRequest{
			TargetNode:  v.node,
			TargetShard: uint(shardi),
			Block:       block,
			Keys:        shardKey,
		}

		wg.Go(func() error {
			start := time.Now()
			resp, err := v.shards[request.TargetShard].ViewReadMany(ctx, request)
			if err != nil {
				return err
			}
			v.metrics.onViewDuration(start)
			v.metrics.onViewRead(len(keys), int(resp.Hits))

			mu.Lock()
			mergedResults = append(mergedResults, resp.Rows...)
			mu.Unlock()
			return nil
		})
	}

	if err := wg.Wait(); err != nil {
		return nil, err
	}
	return v.fixResult(mergedResults), nil
}

func (v *View) fixResult(rows []boostpb.Row) *sqltypes.Result {
	rr := &sqltypes.Result{
		Fields: v.schema,
	}
	if v.topkOrder != nil {
		order := flownode.Order(v.topkOrder)
		slices.SortFunc(rows, func(a, b boostpb.Row) bool {
			return order.Cmp(a, b) < 0
		})
		for _, r := range rows[:v.topkLimit] {
			rr.Rows = append(rr.Rows, r.ToVitessTruncate(len(v.schema)))
		}
	} else {
		for _, r := range rows {
			rr.Rows = append(rr.Rows, r.ToVitess())
		}
	}
	return rr
}

func (v *View) TableName() string {
	return v.name
}

func (v *View) Fields() []*query.Field {
	return v.schema
}

type scopedMetrics struct {
	publicQueryID string
	hitrate       *metrics.RateCounter
}

func (m *scopedMetrics) onViewDuration(start time.Time) {
	if m == nil {
		return
	}
	StatViewReadTiming.Record(m.publicQueryID, start)
}

func (m *scopedMetrics) onViewRead(reads, hits int) {
	if m == nil {
		return
	}
	StatViewReads.Add(m.publicQueryID, int64(reads))
	if hits > 0 {
		StatViewHits.Add(m.publicQueryID, int64(hits))
	}

	m.hitrate.Register(hits > 0)
}
