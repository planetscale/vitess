package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/boostrpc/service"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/boostplan/viewplan"
	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/boost/server/controller/domainrpc"
	"vitess.io/vitess/go/boost/server/controller/materialization"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	topowatcher "vitess.io/vitess/go/boost/topo/watcher"
	"vitess.io/vitess/go/maps2"
	"vitess.io/vitess/go/slice"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
)

type Controller struct {
	g *graph.Graph[*flownode.Node]

	uuid               uuid.UUID
	nDomains           uint
	shardCount         *uint
	epoch              service.Epoch
	quorum             uint
	heartbeatEvery     time.Duration
	healthcheckEvery   time.Duration
	lastCheckedWorkers time.Time

	domainConfig    *config.Domain
	mat             *materialization.Materialization
	recipe          *boostplan.VersionedRecipe
	domains         map[dataflow.DomainIdx]*domainrpc.Handle
	domainNodeMap   map[dataflow.DomainIdx][]graph.NodeIdx
	remap           map[dataflow.DomainIdx]map[graph.NodeIdx]dataflow.IndexPair
	chanCoordinator *boostrpc.ChannelCoordinator

	workersReady        bool
	expectedWorkerCount int
	workers             map[domainrpc.WorkerID]*domainrpc.Worker

	topo *toposerver.Server
	log  *zap.Logger

	BuildDomain domain.BuilderFn
}

var _ MigrationTarget = (*Controller)(nil)

func NewController(log *zap.Logger, uuid uuid.UUID, cfg *config.Config, state *vtboostpb.ControllerState, ts *toposerver.Server) (*Controller, error) {
	g := new(graph.Graph[*flownode.Node])

	// the Root node has no relevant fields; it just acts as a root for the whole DAG
	if !g.AddNode(flownode.New("root", []string{"void"}, &flownode.Root{})).IsRoot() {
		return nil, errors.New("failed to add initial root node")
	}

	mat := materialization.NewMaterialization(cfg.Materialization)
	recipe := boostplan.BlankRecipe()
	recipe.EnableReuse(cfg.Reuse)

	var shardCount *uint
	if cfg.Shards > 0 {
		shardCount = &cfg.Shards
	}

	return &Controller{
		g:                  g,
		uuid:               uuid,
		shardCount:         shardCount,
		epoch:              service.Epoch(state.Epoch),
		quorum:             cfg.Quorum,
		heartbeatEvery:     cfg.HeartbeatEvery,
		healthcheckEvery:   cfg.HealthcheckEvery,
		lastCheckedWorkers: time.Now(),
		domainConfig:       cfg.Domain,
		mat:                mat,
		recipe:             recipe,
		domains:            make(map[dataflow.DomainIdx]*domainrpc.Handle),
		domainNodeMap:      make(map[dataflow.DomainIdx][]graph.NodeIdx),
		remap:              make(map[dataflow.DomainIdx]map[graph.NodeIdx]dataflow.IndexPair),
		chanCoordinator:    boostrpc.NewChannelCoordinator(),
		workers:            make(map[domainrpc.WorkerID]*domainrpc.Worker),
		topo:               ts,
		log:                log.With(zap.Int64("epoch", state.Epoch)),
		BuildDomain:        domain.ToProto,
	}, nil
}

func (ctrl *Controller) IsReady() bool {
	return ctrl.expectedWorkerCount > 0 && len(ctrl.workers) >= ctrl.expectedWorkerCount
}

func (ctrl *Controller) Close() {
	for _, w := range ctrl.workers {
		if err := w.Close(); err != nil {
			ctrl.log.Warn("failed to close Worker connection", zap.Error(err))
		}
	}
	ctrl.workers = make(map[domainrpc.WorkerID]*domainrpc.Worker)
}

func (ctrl *Controller) updateWorker(ctx context.Context, req *vtboostpb.TopoWorkerEntry) {
	var err error
	workerid := domainrpc.WorkerID(req.Uuid)
	w, exists := ctrl.workers[workerid]
	if exists {
		err = w.Update(req)
		if err != nil {
			ctrl.log.Error("failed to update worker", zap.Error(err))
			return
		}
	} else {
		w, err = domainrpc.NewWorker(workerid, req)
		if err != nil {
			ctrl.log.Error("failed to register worker", zap.Error(err))
			return
		}
		ctrl.workers[workerid] = w
	}

	var removed []string
	for _, query := range req.UnhealthyQueries {
		switch query.Status {
		case vtboostpb.TopoWorkerEntry_SCHEMA_CHANGE_CONFLICT:
			ctrl.log.Warn("schema change conflict", zap.String("query", query.QueryId))
			removed = append(removed, query.QueryId)
		}
	}
	if len(removed) > 0 {
		_, err := ctrl.ModifyRecipe(ctx, nil, func(recipe *vtboostpb.Recipe) error {
			recipe.Queries = slice.DeleteFunc(recipe.Queries, func(q *vtboostpb.CachedQuery) bool {
				return slices.Contains(removed, q.PublicId)
			})
			return nil
		})
		if err != nil {
			ctrl.log.Error("failed to modify recipe", zap.Error(err))
		}
	}
}

func (ctrl *Controller) domainPlace(ctx context.Context, idx dataflow.DomainIdx, maybeShardNumber *uint, innodes []nodeWithAge) error {
	nodes := new(flownode.Map)

	for _, n := range innodes {
		node, err := ctrl.g.Value(n.Idx).Finalize(ctrl.g)
		if err != nil {
			return err
		}
		nodes.Insert(node.LocalAddr(), node)
	}

	var (
		shards       = common.UnwrapOr(maybeShardNumber, 1)
		domainshards = make([]domainrpc.ShardHandle, shards)
		announce     = make([]*service.DomainDescriptor, shards)
		workers      = maps2.Values(ctrl.workers)
		wg           errgroup.Group
	)

	for s := uint(0); s < shards; s++ {
		var shardN = s
		wg.Go(func() error {
			dombuilder, err := ctrl.BuildDomain(idx, shardN, shards, nodes, ctrl.domainConfig)
			if err != nil {
				return err
			}

			var wrk *domainrpc.Worker
			for i := range workers {
				wrk = workers[(int(shardN)+i)%len(workers)]
				if wrk.Healthy {
					break
				}
			}
			if wrk == nil || !wrk.Healthy {
				return fmt.Errorf("no healthy workers available")
			}

			var request = &service.AssignDomainRequest{
				From: &service.WorkerSource{
					Epoch:      ctrl.epoch,
					SourceUuid: ctrl.uuid.String(),
				},
				Domain: dombuilder,
			}

			resp, err := wrk.Client.AssignDomain(ctx, request)
			if err != nil {
				return err
			}

			ctrl.chanCoordinator.InsertRemote(idx, resp.Shard, resp.Addr)
			shardClient, err := ctrl.chanCoordinator.GetClient(idx, resp.Shard)
			if err != nil {
				return err
			}

			announce[shardN] = &service.DomainDescriptor{
				Id:    idx,
				Shard: shardN,
				Addr:  resp.Addr,
			}
			domainshards[shardN] = domainrpc.NewShardHandle(wrk.UUID, shardClient)
			return nil
		})
	}

	if err := wg.Wait(); err != nil {
		return err
	}

	for _, endpoint := range ctrl.workers {
		for _, dd := range announce {
			if _, err := endpoint.Client.DomainBooted(ctx, &service.DomainBootedRequest{
				From: &service.WorkerSource{
					SourceUuid: ctrl.uuid.String(),
					Epoch:      ctrl.epoch,
				},
				Domain: dd,
			}); err != nil {
				return err
			}
		}
	}

	ctrl.domains[idx] = domainrpc.NewHandle(idx, domainshards)
	return nil
}

func (ctrl *Controller) viewDescriptor(node *flownode.Node) *vtboostpb.Materialization_ViewDescriptor {
	reader := node.AsReader()
	domain := ctrl.domains[node.Domain()]
	if domain == nil {
		panic("unknown domain?")
	}

	var shards []string
	for s := uint(0); s < domain.Shards(); s++ {
		w, ok := ctrl.workers[domain.Assignment(s)]
		if !ok {
			ctrl.log.Warn("missing worker in domain assignment", node.Domain().Zap(), zap.Uint("shard", s))
			continue
		}

		shards = append(shards, w.ReaderAddr())
	}

	var desc = reader.ViewPlan()
	var nodeSchema = node.Schema()
	var keySchema []*querypb.Field
	var queryMode = vtboostpb.Materialization_ViewDescriptor_QUERY_SINGLE
	for _, param := range desc.Parameters {
		if param.Kind == viewplan.Param_MULTI_EQUALITY {
			queryMode = vtboostpb.Materialization_ViewDescriptor_QUERY_MULTI
		}
		tt := nodeSchema[param.Col]
		keySchema = append(keySchema, &querypb.Field{
			Name:    param.Name,
			Type:    tt.T,
			Charset: uint32(tt.Collation),
		})
	}

	var fields = node.Fields()
	var schema []*querypb.Field
	for col := 0; col < reader.PublicColumnLength(); col++ {
		tt := node.Schema()[col]
		schema = append(schema, &querypb.Field{
			Name:    fields[col],
			Type:    tt.T,
			Charset: uint32(tt.Collation),
		})
	}

	orderCols, orderColsDesc, orderLimit := reader.Order()

	return &vtboostpb.Materialization_ViewDescriptor{
		PublicId:      reader.PublicID(),
		Node:          uint32(node.GlobalAddr()),
		Schema:        schema,
		KeySchema:     keySchema,
		Shards:        shards,
		TopkOrderCols: orderCols,
		TopkOrderDesc: orderColsDesc,
		TopkLimit:     int64(orderLimit),
		QueryMode:     queryMode,
	}
}

func (ctrl *Controller) viewDescriptorForPublicID(id string) (*vtboostpb.Materialization_ViewDescriptor, error) {
	ext := ctrl.g.Externals(graph.DirectionOutgoing)
	for ext.Next() {
		nn := ctrl.g.Value(ext.Current)
		if r := nn.AsReader(); r != nil && r.PublicID() == id {
			return ctrl.viewDescriptor(nn), nil
		}
	}
	return nil, fmt.Errorf("no such view: %s", id)
}

func (ctrl *Controller) GetMaterializations() ([]*vtboostpb.Materialization, error) {
	viewsByPublicID := make(map[string]*boostplan.CachedQuery)
	var res []*vtboostpb.Materialization
	for _, v := range ctrl.recipe.GetAllPublicViews() {
		viewsByPublicID[v.PublicId] = v
	}

	var err error
	ctrl.g.ForEachValue(func(n *flownode.Node) bool {
		if r := n.AsReader(); r != nil {
			view, ok := viewsByPublicID[r.PublicID()]
			if !ok {
				return true
			}

			normalizedSQL := topowatcher.ParametrizeQuery(view.Statement)
			var binds []*vtboostpb.Materialization_Bind
			var fullyMaterialized bool
			binds, fullyMaterialized, err = topowatcher.GenerateBoundsForQuery(view.Statement, r.ViewPlan())
			if err != nil {
				return false
			}

			res = append(res, &vtboostpb.Materialization{
				Query:             view.CachedQuery,
				NormalizedSql:     normalizedSQL,
				Binds:             binds,
				FullyMaterialized: fullyMaterialized,
				View:              ctrl.viewDescriptor(n),
			})
		}
		return true
	})

	return res, err
}

func (ctrl *Controller) PutRecipe(ctx context.Context, recipepb *vtboostpb.Recipe, si *boostplan.SchemaInformation) (*boostplan.ActivationResult, error) {
	if recipepb.Version <= ctrl.recipe.Version() {
		return &boostplan.ActivationResult{}, nil
	}

	newrecipe, err := boostplan.NewVersionedRecipe(ctrl.recipe, recipepb)
	if err != nil {
		return nil, err
	}

	return ctrl.applyRecipe(ctx, newrecipe, si)
}

func (ctrl *Controller) ModifyRecipe(ctx context.Context, si *boostplan.SchemaInformation, modify func(recipe *vtboostpb.Recipe) error) (*boostplan.ActivationResult, error) {
	recipepb := &vtboostpb.Recipe{
		Version: ctrl.recipe.Version() + 1,
		Queries: ctrl.recipe.ToProto(),
	}

	if err := modify(recipepb); err != nil {
		return nil, err
	}

	return ctrl.PutRecipe(ctx, recipepb, si)
}

func (ctrl *Controller) cleanupRecipe(ctx context.Context, activation *boostplan.ActivationResult) error {
	var removedTables []graph.NodeIdx
	var removedOther []graph.NodeIdx
	for _, ni := range activation.NodesRemoved {
		if ctrl.g.Value(ni).IsTable() {
			removedTables = append(removedTables, ni)
		} else {
			removedOther = append(removedOther, ni)
		}
	}

	var topoRemovals []graph.NodeIdx
	var topo = graph.NewTopoVisitor(ctrl.g)
	for topo.Next() {
		if slices.Contains(removedOther, topo.Current) {
			topoRemovals = append(topoRemovals, topo.Current)
		}
	}

	var removalErrors []error

	for n := len(topoRemovals) - 1; n >= 0; n-- {
		if err := ctrl.removeLeaf(ctx, topoRemovals[n]); err != nil {
			removalErrors = append(removalErrors, err)
		}
	}

	for _, table := range removedTables {
		children := ctrl.g.NeighborsDirected(table, graph.DirectionOutgoing).Count()
		if children != 0 {
			return fmt.Errorf("trying to remove table with children: %v", table)
		}

		if err := ctrl.removeNodes(ctx, []graph.NodeIdx{table}); err != nil {
			removalErrors = append(removalErrors, err)
		}
	}

	return multierr.Combine(removalErrors...)
}

func (ctrl *Controller) applyRecipe(ctx context.Context, newrecipe *boostplan.VersionedRecipe, si *boostplan.SchemaInformation) (*boostplan.ActivationResult, error) {
	ctrl.log.Debug("applying new recipe", zap.Int64("version", newrecipe.Version()))

	var activation *boostplan.ActivationResult

	mig := NewMigration(ctx, ctrl.log, ctrl)
	mig = NewTrackedMigration(mig, ctrl)
	err := safeMigration(mig, func(mig Migration) error {
		var err error
		activation, err = mig.Activate(newrecipe, si)
		return err
	})

	if err != nil {
		return nil, err
	}

	ctrl.recipe = newrecipe

	// cleanupRecipe only removes old nodes that are no longer in use; we do not fail recipe application
	// if it fails because it's not a required cluster change
	return activation, ctrl.cleanupRecipe(ctx, activation)
}

func (ctrl *Controller) removeNodes(ctx context.Context, removals []graph.NodeIdx) error {
	var domainRemovals = make(map[dataflow.DomainIdx][]dataflow.LocalNodeIdx)

	for _, ni := range removals {
		ctrl.log.Debug("removed node", ni.Zap())

		node := ctrl.g.Value(ni)
		node.Remove()

		dom := node.Domain()
		domainRemovals[dom] = append(domainRemovals[dom], node.LocalAddr())
	}

	for dom, nodes := range domainRemovals {
		ctrl.log.Debug("notifying domain of node removals", dom.Zap())

		pkt := &packet.RemoveNodesRequest{
			Nodes: nodes,
		}
		if err := ctrl.domains[dom].Client(ctx, ctrl.workers).RemoveNodes(pkt); err != nil {
			return err
		}
	}
	return nil
}

func (ctrl *Controller) removeLeaf(ctx context.Context, leaf graph.NodeIdx) error {
	var removals []graph.NodeIdx
	var start = leaf

	if ctrl.g.Value(leaf).IsRoot() {
		panic("trying to remove source")
	}

	nchildren := ctrl.g.NeighborsDirected(leaf, graph.DirectionOutgoing).Count()
	if nchildren > 0 {
		// This query leaf node has children -- typically, these are readers, but they can also
		// include egress nodes or other, dependent queries. We need to find the actual reader,
		// and remove that.

		if nchildren != 1 {
			panic("trying to remove node with multiple children")
		}

		var readers []graph.NodeIdx
		var bfs = graph.NewBFSVisitor(ctrl.g, leaf)
		for bfs.Next() {
			n := ctrl.g.Value(bfs.Current)
			if r := n.AsReader(); r != nil && r.IsFor() == leaf {
				readers = append(readers, bfs.Current)
			}
		}

		if len(readers) != 1 {
			panic("nodes can only have one reader attached")
		}
		removals = append(removals, readers[0])
		leaf = readers[0]

		ctrl.log.Debug("removing query leaf", leaf.ZapField("node"), readers[0].ZapField("really"))
	}

	if ctrl.g.NeighborsDirected(leaf, graph.DirectionOutgoing).Count() != 0 {
		panic("node should have no children left")
	}

	var nodes = []graph.NodeIdx{leaf}
	for len(nodes) > 0 {
		node := nodes[len(nodes)-1]
		nodes = nodes[:len(nodes)-1]

		parents := ctrl.g.NeighborsDirected(node, graph.DirectionIncoming)
		for parents.Next() {
			parent := parents.Current
			edge := ctrl.g.FindEdge(parent, node)
			ctrl.g.RemoveEdge(edge)

			pp := ctrl.g.Value(parent)
			if !pp.IsRoot() &&
				!pp.IsTable() &&
				(parent == start || !ctrl.recipe.IsLeafAddress(parent)) &&
				ctrl.g.NeighborsDirected(parent, graph.DirectionOutgoing).Count() == 0 {
				nodes = append(nodes, parent)
			}
		}

		removals = append(removals, node)
	}

	return ctrl.removeNodes(ctx, removals)
}

func (ctrl *Controller) PrepareEvictionPlan(ctx context.Context) (*materialization.EvictionPlan, error) {
	var mu sync.Mutex
	var wg errgroup.Group
	var ep = materialization.NewEvictionPlan()

	ep.LoadRecipe(ctrl.g, ctrl.mat, ctrl.recipe.Recipe)

	for _, wrk := range ctrl.workers {
		client := wrk.Client
		wg.Go(func() error {
			resp, err := client.MemoryStats(ctx, &service.MemoryStatsRequest{})
			if err != nil {
				return err
			}

			mu.Lock()
			defer mu.Unlock()
			return ep.LoadMemoryStats(resp)
		})
	}

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	return ep, nil
}

func (ctrl *Controller) PerformDistributedEviction(ctx context.Context, forceLimits map[string]int64) (*materialization.EvictionPlan, error) {
	ctrl.log.Debug("preparing eviction plan for distributed eviction")

	plan, err := ctrl.PrepareEvictionPlan(ctx)
	if err != nil {
		return nil, err
	}

	plan.SetCustomLimits(forceLimits)
	evictions := plan.Evictions()

	ctrl.log.Debug("distributed eviction plan ready", zap.Int("evictions", len(evictions)))

	var errs []error
	for _, ev := range plan.Evictions() {
		dom, err := ctrl.chanCoordinator.GetClient(ev.Domain.Domain, ev.Domain.Shard)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		_, err = dom.Evict(ctx, &packet.EvictRequest{
			Node:     ev.Local,
			NumBytes: ev.Evict,
		})
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return nil, multierr.Combine(errs...)
	}
	return plan, nil
}

func (ctrl *Controller) graph() *graph.Graph[*flownode.Node] {
	return ctrl.g
}

func (ctrl *Controller) sharding() *uint {
	return ctrl.shardCount
}

func (ctrl *Controller) materialization() *materialization.Materialization {
	return ctrl.mat
}

func (ctrl *Controller) domainMapping(dom dataflow.DomainIdx) map[graph.NodeIdx]dataflow.IndexPair {
	rm, ok := ctrl.remap[dom]
	if !ok {
		rm = make(map[graph.NodeIdx]dataflow.IndexPair)
		ctrl.remap[dom] = rm
	}
	return rm
}

func (ctrl *Controller) domainNodes() map[dataflow.DomainIdx][]graph.NodeIdx {
	return ctrl.domainNodeMap
}

func (ctrl *Controller) domainNext() dataflow.DomainIdx {
	next := dataflow.DomainIdx(ctrl.nDomains)
	ctrl.nDomains++
	return next
}

func (ctrl *Controller) domainExists(idx dataflow.DomainIdx) bool {
	_, ok := ctrl.domains[idx]
	return ok
}

func (ctrl *Controller) domainShards(idx dataflow.DomainIdx) uint {
	return ctrl.domains[idx].Shards()
}

func (ctrl *Controller) domainAssignStream(ctx context.Context, req *service.AssignStreamRequest) error {
	// TODO; do not pick a worker at random
	for _, worker := range ctrl.workers {
		if _, err := worker.Client.AssignStream(ctx, req); err != nil {
			return err
		}
		break
	}
	return nil
}

func (ctrl *Controller) domainClient(ctx context.Context, idx dataflow.DomainIdx) domainrpc.Client {
	return ctrl.domains[idx].Client(ctx, ctrl.workers)
}

func (ctrl *Controller) domainShardClient(ctx context.Context, idx dataflow.DomainIdx, shard uint) domainrpc.Client {
	return ctrl.domains[idx].ShardClient(ctx, shard, ctrl.workers)
}
