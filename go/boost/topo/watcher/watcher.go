/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package watcher

import (
	"context"
	"flag"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"google.golang.org/protobuf/proto"
	"storj.io/drpc"

	"vitess.io/vitess/go/netutil"

	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/common/metrics"
	boosttopo "vitess.io/vitess/go/boost/topo/internal/topo"
	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vthash"
)

var (
	EnableBoostIntegration = flag.Bool("enable-boost", false, "Enable boost integration; requires a valid topo connection. This Vitess process will find the local Boost cluster via topo")
)

type ControllerDialer func(string) (vtboostpb.DRPCControllerServiceClient, error)

// Watcher polls tablet from a configurable set of tablets
// periodically. When tablets are added / removed, it calls
// the LegacyTabletRecorder AddTablet / RemoveTablet interface appropriately.
type Watcher struct {
	Dial ControllerDialer

	// set at construction time
	name           string
	ts             *topo.Server
	wg             sync.WaitGroup
	cancel         context.CancelFunc
	failoverCancel context.CancelFunc

	clusterClients *common.SyncMap[string, *clusterClient]
	clusterState   unsafe.Pointer
	primary        unsafe.Pointer
}

// NewWatcher returns a TopologyWatcher that monitors all
// the tablets in a cell, and starts refreshing.
func NewWatcher(ts *topo.Server) *Watcher {
	nw := &Watcher{
		Dial:           boostrpc.NewControllerClient,
		name:           netutil.FullyQualifiedHostnameOrPanic(),
		ts:             ts,
		clusterClients: common.NewSyncMap[string, *clusterClient](),
	}

	return nw
}

func (nw *Watcher) loadClusterState() *globalState {
	return (*globalState)(atomic.LoadPointer(&nw.clusterState))
}

type DebugClusterState struct {
	UUID             string
	State            vtboostpb.ClusterState_State
	Materializations []*DebugMaterialization
}

type DebugMaterialization struct {
	SQL  string
	Rate float64
}

func (nw *Watcher) DebugState() map[string]*DebugClusterState {
	cstate := nw.loadClusterState()
	if cstate == nil {
		return nil
	}

	clusters := make(map[string]*DebugClusterState)
	for _, cluster := range cstate.clusters {
		cs := &DebugClusterState{
			UUID:  cluster.client.uuid,
			State: cluster.state,
		}

		cluster.client.mats.ForEach(func(_ vthash.Hash, materialization *cachedMaterialization) {
			cs.Materializations = append(cs.Materializations, &DebugMaterialization{
				SQL:  materialization.original,
				Rate: materialization.view.metrics.hitrate.Rate(),
			})
		})

		clusters[cluster.client.uuid] = cs
	}
	return clusters
}

func (nw *Watcher) Version() string {
	cstate := nw.loadClusterState()
	if cstate == nil {
		return ""
	}
	return cstate.version
}

// Start starts the topology watcher
func (nw *Watcher) Start() {
	// If the cell doesn't exist, this will return ErrNoNode.
	cellConn, err := nw.ts.ConnForCell(context.Background(), topo.GlobalCell)
	if err != nil {
		panic("boost.Watcher: failed to find GlobalCell in topology")
	}

	var ctx context.Context
	ctx, nw.cancel = context.WithCancel(context.Background())

	nw.wg.Add(1)
	go func() {
		defer nw.wg.Done()

		for ctx.Err() == nil {
			currentClusters, nextClusters, err := cellConn.Watch(ctx, boosttopo.PathClusterState)
			if err != nil {
				if topo.IsErrType(err, topo.NoNode) {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				log.Warningf("boost.Watcher: cannot find an active Boost cluster (%v); retrying...", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if err := nw.updateAvailableClusters(ctx, currentClusters, cellConn); err != nil {
				log.Warningf("boost.Watcher: cannot update cluster state (%v); retrying...", err)
			}
			for w := range nextClusters {
				if err := nw.updateAvailableClusters(ctx, w, cellConn); err != nil {
					log.Warningf("boost.Watcher: cannot update cluster state (%v); retrying...", err)
				}
			}
		}
	}()

	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				nw.clusterClients.ForEach(func(_ string, cluster *clusterClient) {
					cluster.hitrate.Tick()
				})
			}
		}
	}()
}

type clusterState struct {
	client *clusterClient
	state  vtboostpb.ClusterState_State
	drain  time.Time
}

type globalState struct {
	clusters []clusterState
	primary  *clusterClient
	version  string
}

func (nw *Watcher) watchForFailover(ctx context.Context, oldState, newState *globalState) {
	const FailoverRateDelta = 0.05
	const FailoverTimeout = 1 * time.Hour

	if nw.failoverCancel != nil {
		nw.failoverCancel()
	}

	var failoverCtx context.Context
	failoverCtx, nw.failoverCancel = context.WithCancel(ctx)

	go func(ctx context.Context) {
		timeoutDuration := FailoverTimeout
		for _, c := range newState.clusters {
			if c.client == oldState.primary && !c.drain.IsZero() {
				timeoutDuration = time.Until(c.drain)
				break
			}
		}

		tick := time.NewTicker(1 * time.Second)
		defer tick.Stop()

		timeout := time.NewTimer(timeoutDuration)
		defer timeout.Stop()

		start := time.Now()

		log.Infof("boost.Watcher: watching for failover from %s to %s (timeout=%v)...", oldState.primary.uuid, newState.primary.uuid, timeoutDuration)

		for {
			select {
			case <-ctx.Done():
				log.Infof("boost.Watcher: interrupted failover")
				return

			case <-tick.C:
				oldRate := oldState.primary.hitrate.Rate()
				newRate := newState.primary.hitrate.Rate()
				if math.Abs(oldRate-newRate) <= FailoverRateDelta {
					log.Infof("boost.Watcher: failing over! from %s (rate=%0.5f) to %s (rate=%0.5f), total duration %v",
						oldState.primary.uuid, oldRate, newState.primary.uuid, newRate, time.Since(start))
					nw.updatePrimary(ctx, &nw.primary, newState.primary)
					return
				}

			case <-timeout.C:
				oldRate := oldState.primary.hitrate.Rate()
				newRate := newState.primary.hitrate.Rate()
				log.Infof("boost.Watcher: failing over! from %s (rate=%0.5f) to %s (rate=%0.5f), forced failover after %v",
					oldState.primary.uuid, oldRate, newState.primary.uuid, newRate, time.Since(start))
				nw.updatePrimary(ctx, &nw.primary, newState.primary)
				return
			}
		}
	}(failoverCtx)
}

func (nw *Watcher) updatePrimary(ctx context.Context, ptr *unsafe.Pointer, primary *clusterClient) {
	atomic.StorePointer(ptr, unsafe.Pointer(primary))
	if err := nw.updateTopoVtgates(ctx, primary.uuid); err != nil {
		log.Warningf("failed to update topo with vtgates using cluster: %v", err)
	}
}

func (nw *Watcher) updateTopoVtgates(ctx context.Context, uuid string) error {
	updatePrimary := func(state *vtboostpb.ClusterStates) error {
		for _, cluster := range state.Clusters {
			if cluster.Uuid == uuid {
				if cluster.Vtgates == nil {
					cluster.Vtgates = map[string]bool{}
				}
				cluster.Vtgates[nw.name] = true
			} else {
				delete(cluster.Vtgates, nw.name)
			}
		}
		return nil
	}
	_, err := boosttopo.Update(ctx, nw.ts, boosttopo.PathClusterState, updatePrimary)
	return err
}

func (nw *Watcher) updateAvailableClusters(ctx context.Context, w *topo.WatchData, conn topo.Conn) error {
	if w.Err != nil {
		if topo.IsErrType(w.Err, topo.Interrupted) {
			// No need to log anything if we're asked to stop.
			return nil
		}
		return w.Err
	}

	var cstate vtboostpb.ClusterStates
	err := proto.Unmarshal(w.Contents, &cstate)
	if err != nil {
		return err
	}

	newState := &globalState{
		version: w.Version.String(),
	}

	log.Infof("boost.Watcher: updateAvailableClusters (%d clusters in state v%d)", len(cstate.Clusters), newState.version)

	for _, cluster := range cstate.Clusters {
		client, ok := nw.clusterClients.Get(cluster.Uuid)
		if !ok {
			client = &clusterClient{
				dial:    nw.Dial,
				uuid:    cluster.Uuid,
				rpcs:    common.NewSyncMap[string, drpc.Conn](),
				mats:    common.NewSyncMap[vthash.Hash, *cachedMaterialization](),
				hitrate: metrics.NewRateCounter(1 * time.Minute),
			}

			var clusterCtx context.Context
			clusterCtx, client.cancel = context.WithCancel(ctx)

			nw.clusterClients.Set(cluster.Uuid, client)
			nw.wg.Add(1)

			go client.start(clusterCtx, conn, &nw.wg)
		}

		if cluster.State == vtboostpb.ClusterState_PRIMARY {
			newState.primary = client
		}
		newState.clusters = append(newState.clusters, clusterState{
			client: client,
			state:  cluster.State,
			drain:  protoutil.TimeFromProto(cluster.DrainedAt),
		})
	}

	oldState := (*globalState)(atomic.SwapPointer(&nw.clusterState, unsafe.Pointer(newState)))
	switch {
	case newState.primary == nil:
		log.Warningf("boost.Watcher: cluster state has no active primary")

	case oldState == nil || oldState.primary == nil:
		log.Infof("boost.Watcher: forced failover from <nil> to %s", newState.primary.uuid)
		nw.updatePrimary(ctx, &nw.primary, newState.primary)

	case oldState.primary != newState.primary:
		nw.watchForFailover(ctx, oldState, newState)
	}
	return nil
}

const topoCleanupTimeout = 500 * time.Millisecond

// Stop stops the watcher.
func (nw *Watcher) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), topoCleanupTimeout)
	defer cancel()
	// Using an empty UUID ensures we remove ourselves from
	// any cluster we're listed in since it never matches.
	if err := nw.updateTopoVtgates(ctx, ""); err != nil {
		log.Warningf("boost.Watcher: failed to clean up from topo during shutdown: %v", err)
	}

	if nw.cancel == nil {
		return
	}
	nw.cancel()
	nw.wg.Wait()
}

type MaterializedQuery struct {
	View       *View
	Normalized string
	Args       []*querypb.BindVariable
	hash       vthash.Hash
}

func hashMaterializedQuery(keyspace, query string) vthash.Hash {
	var h vthash.Hasher
	_, _ = h.Write([]byte("mat1:"))
	_, _ = h.Write(hack.StringBytes(keyspace))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write(hack.StringBytes(query))
	return h.Sum128()
}

var defaultBogokey = []*querypb.BindVariable{
	{
		Type:  sqltypes.Int64,
		Value: []byte("0"),
	},
}

func (nw *Watcher) GetCachedQuery(keyspace string, query sqlparser.Statement, bvars map[string]*querypb.BindVariable) (*MaterializedQuery, bool) {
	res := &MaterializedQuery{
		Normalized: ParametrizeQuery(query),
	}

	res.hash = hashMaterializedQuery(keyspace, res.Normalized)

	activeCluster := (*clusterClient)(atomic.LoadPointer(&nw.primary))
	if activeCluster == nil {
		return nil, false
	}

	for cached, _ := activeCluster.mats.Get(res.hash); cached != nil; cached = cached.next {
		var args []*querypb.BindVariable
		if cached.fullyMaterialized {
			args = defaultBogokey
		} else {
			args = make([]*querypb.BindVariable, len(cached.view.keySchema))
		}

		if matchParametrizedQuery(args, query, bvars, cached.bounds) {
			res.Args = args
			res.View = cached.view
			return res, true
		}
	}

	return nil, false
}

func (nw *Watcher) Warmup(query *MaterializedQuery, warmup func(*View)) {
	state := nw.loadClusterState()
	for _, cluster := range state.clusters {
		if cluster.state != vtboostpb.ClusterState_WARMING {
			continue
		}
		cached, ok := cluster.client.mats.Get(query.hash)
		if !ok {
			continue
		}
		warmup(cached.view)
	}
}

type cachedMaterialization struct {
	view              *View
	bounds            []*vtboostpb.Materialization_Bound
	fullyMaterialized bool
	original          string
	next              *cachedMaterialization
}

type clusterClient struct {
	dial   ControllerDialer
	uuid   string
	cancel context.CancelFunc

	mu           sync.Mutex
	leaderAddr   string
	leaderConn   vtboostpb.DRPCControllerServiceClient
	pendingState bool

	rpcs          *common.SyncMap[string, drpc.Conn]
	mats          *common.SyncMap[vthash.Hash, *cachedMaterialization]
	hitrate       *metrics.RateCounter
	recipeVersion int64
}

func (ac *clusterClient) start(ctx context.Context, conn topo.Conn, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Infof("boost.Watcher: started watcher for cluster %s", ac.uuid)

	var err error

	election, err := conn.NewLeaderParticipation(boosttopo.PathLeader(ac.uuid), "")
	if err != nil {
		panic("failed to create LeaderParticipation")
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for ctx.Err() == nil {
			lead, err := election.WaitForNewLeader(ctx)
			if err != nil {
				log.Errorf("failed to WaitForNewLeader: %v (retrying in 1s)", err)
				time.Sleep(1 * time.Second)
				continue
			}
			for l := range lead {
				if err := ac.updateLeader(l); err != nil {
					log.Errorf("failed to update leader connection: %v", err)
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		var current *topo.WatchData
		var changes <-chan *topo.WatchData
		var err error

		for ctx.Err() == nil {
			current, changes, err = conn.Watch(ctx, boosttopo.PathControllerState(ac.uuid))
			if err != nil || changes == nil {
				time.Sleep(1 * time.Second)
				continue
			}

			err := ac.processControllerState(current)
			if err != nil {
				log.Errorf("failed to process controller state: %v", err)
			}
			for data := range changes {
				err := ac.processControllerState(data)
				if err != nil {
					log.Errorf("failed to process controller state: %v", err)
				}
			}
		}
	}()
}

func (ac *clusterClient) updateLeader(currentLeader string) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if ac.leaderAddr != currentLeader {
		log.Infof("boost.Watcher: new leader found for cluster %s: %s (was %s)", ac.uuid, currentLeader, ac.leaderAddr)

		if ac.leaderConn != nil {
			if conn := ac.leaderConn.DRPCConn(); conn != nil {
				_ = conn.Close()
			}
			ac.leaderConn = nil
		}
		var err error
		ac.leaderConn, err = ac.dial(currentLeader)
		if err != nil {
			return err
		}
		ac.leaderAddr = currentLeader
	}

	if ac.pendingState {
		ac.pendingState = false
		return ac.updateMaterializations(ac.leaderConn)
	}

	return nil
}

func (ac *clusterClient) updateMaterializations(leader vtboostpb.DRPCControllerServiceClient) error {
	resp, err := leader.GetMaterializations(context.Background(), &vtboostpb.MaterializationsRequest{})
	if err != nil {
		return err
	}

	log.Infof("boost.Watcher: new materializations loaded (%d in total)", len(resp.Materializations))
	res := make(map[vthash.Hash]*cachedMaterialization)
	for _, m := range resp.Materializations {
		view, err := NewViewClientFromProto(m.View, ac.rpcs)
		if err != nil {
			continue
		}
		view.CollectMetrics(m.Query.PublicId, ac.hitrate)
		hashedQuery := hashMaterializedQuery(m.Query.Keyspace, m.NormalizedSql)
		res[hashedQuery] = &cachedMaterialization{
			view:              view,
			bounds:            m.Bounds,
			original:          m.NormalizedSql,
			fullyMaterialized: m.FullyMaterialized,
			next:              res[hashedQuery],
		}
	}
	ac.mats.Replace(res)
	return nil
}

func (ac *clusterClient) processControllerState(data *topo.WatchData) error {
	if data.Err != nil {
		return data.Err
	}

	var state vtboostpb.ControllerState
	if err := proto.Unmarshal(data.Contents, &state); err != nil {
		return err
	}
	if ac.recipeVersion == state.RecipeVersion {
		return nil
	}
	ac.recipeVersion = state.RecipeVersion

	ac.mu.Lock()
	defer ac.mu.Unlock()

	if leader := ac.leaderConn; leader != nil {
		return ac.updateMaterializations(leader)
	}

	ac.pendingState = true
	return nil
}

func (ac *clusterClient) Close() {
	ac.cancel()
	ac.rpcs.ForEach(func(_ string, conn drpc.Conn) {
		conn.Close()
	})
}
