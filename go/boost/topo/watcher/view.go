package watcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"storj.io/drpc"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/common/metrics"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/proto/query"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/vthash"
)

var (
	StatViewReads      = stats.NewCountersWithMultiLabels("BoostClientViewReads", "The total number of remote view reads calls executed", []string{"PublicId"})
	StatViewHits       = stats.NewCountersWithMultiLabels("BoostClientViewHits", "The total number of cache hits from view reads", []string{"PublicId"})
	StatViewReadTiming = stats.NewMultiTimings("BoostClientViewReadMs", "Duration for each individual read in the view client", []string{"PublicId"})
)

type View struct {
	name      string
	node      graph.NodeIdx
	schema    []*querypb.Field
	keySchema []*querypb.Field
	addrs     []string
	shards    []boostpb.DRPCReaderClient

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
	}
	if len(pb.Shards) > 1 {
		if len(pb.KeySchema) != 1 {
			return nil, fmt.Errorf("sharded view with composite primary key is unsupported")
		}
		v.shardKeyType = boostpb.TypeFromField(pb.KeySchema[0])
	}
	if err := v.addShards(pb.Shards, rpcs); err != nil {
		return nil, err
	}
	return v, nil
}

func (v *View) CollectMetrics(publicID string, hitrate *metrics.RateCounter) {
	v.metrics = &scopedMetrics{
		labels:  []string{publicID},
		hitrate: hitrate,
	}
}

func (v *View) Lookup(ctx context.Context, key []sqltypes.Value, block bool) (*sqltypes.Result, error) {
	request := &boostpb.ViewReadRequest{
		TargetNode:  v.node,
		TargetShard: 0,
		Block:       block,
		Key:         boostpb.RowFromVitess(key),
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
	return v.fixResult(resp), nil
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

func (v *View) LookupMany(ctx context.Context, keys [][]sqltypes.Value, block bool) ([]*sqltypes.Result, error) {
	if len(v.shards) == 1 {
		request := &boostpb.ViewReadManyRequest{
			TargetNode:  v.node,
			TargetShard: 0,
			Block:       block,
		}

		for _, k := range keys {
			request.Keys = append(request.Keys, boostpb.RowFromVitess(k))
		}

		start := time.Now()
		resp, err := v.shards[0].ViewReadMany(ctx, request)
		if err != nil {
			return nil, err
		}
		v.metrics.onViewDuration(start)

		var results = make([]*sqltypes.Result, 0, len(resp.Results))
		for _, qr := range resp.Results {
			results = append(results, v.fixResult(qr))
		}
		return results, nil
	}

	shardKeys := make([][]boostpb.Row, len(v.shards))
	var hasher vthash.Hasher
	for _, key := range keys {
		bkey := boostpb.RowFromVitess(key)
		shardn := bkey.ShardValue(&hasher, 0, v.shardKeyType, uint(len(v.shards)))
		shardKeys[shardn] = append(shardKeys[shardn], bkey)
	}

	var mergedResults []*sqltypes.Result
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

			mu.Lock()
			for _, qr := range resp.Results {
				mergedResults = append(mergedResults, v.fixResult(qr))
			}
			mu.Unlock()
			return nil
		})
	}

	err := wg.Wait()
	return mergedResults, err
}

func (v *View) fixResult(qr *boostpb.ViewReadResponse) *sqltypes.Result {
	v.metrics.onViewRead(qr.Hit)
	rr := &sqltypes.Result{
		Fields: v.schema,
	}
	for _, r := range qr.Rows {
		rr.Rows = append(rr.Rows, r.ToVitess())
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
	labels  []string
	hitrate *metrics.RateCounter
}

func (m *scopedMetrics) onViewDuration(start time.Time) {
	if m == nil {
		return
	}
	StatViewReadTiming.Record(m.labels, start)
}

func (m *scopedMetrics) onViewRead(hit bool) {
	if m == nil {
		return
	}
	StatViewReads.Add(m.labels, 1)
	if hit {
		StatViewHits.Add(m.labels, 1)
	}

	m.hitrate.Register(hit)
}
