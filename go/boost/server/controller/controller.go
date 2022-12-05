package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow/domain"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/domainrpc"
	"vitess.io/vitess/go/boost/server/controller/materialization"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	topowatcher "vitess.io/vitess/go/boost/topo/watcher"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
)

type Controller struct {
	ingredients *graph.Graph[*flownode.Node]

	uuid               uuid.UUID
	nDomains           uint
	sharding           *uint
	epoch              boostpb.Epoch
	quorum             uint
	heartbeatEvery     time.Duration
	healthcheckEvery   time.Duration
	lastCheckedWorkers time.Time

	domainConfig    *boostpb.DomainConfig
	materialization *materialization.Materialization
	recipe          *boostplan.VersionedRecipe
	domains         map[boostpb.DomainIndex]*domainrpc.Handle
	domainNodes     map[boostpb.DomainIndex][]graph.NodeIdx
	remap           map[boostpb.DomainIndex]map[graph.NodeIdx]boostpb.IndexPair
	chanCoordinator *boostrpc.ChannelCoordinator

	workersReady        bool
	expectedWorkerCount int
	workers             map[domainrpc.WorkerID]*domainrpc.Worker
	readAddrs           map[domainrpc.WorkerID]string

	topo *toposerver.Server
	log  *zap.Logger

	BuildDomain domain.BuilderFn
}

func NewController(log *zap.Logger, uuid uuid.UUID, config *boostpb.Config, state *vtboostpb.ControllerState, ts *toposerver.Server) *Controller {
	g := new(graph.Graph[*flownode.Node])

	// the Source node has no relevant fields; it just acts as a root for the whole DAG
	if !g.AddNode(flownode.New("source", []string{"void"}, &flownode.Source{})).IsSource() {
		panic("expected initial node to be Source")
	}

	mat := materialization.NewMaterialization()
	if !config.PartialEnabled {
		mat.DisablePartial()
	}

	recipe := boostplan.BlankRecipe()
	recipe.EnableReuse(config.Reuse)

	var sharding *uint
	if config.Shards > 0 {
		sharding = &config.Shards
	}

	return &Controller{
		ingredients:        g,
		uuid:               uuid,
		sharding:           sharding,
		epoch:              boostpb.Epoch(state.Epoch),
		quorum:             config.Quorum,
		heartbeatEvery:     config.HeartbeatEvery,
		healthcheckEvery:   config.HealthcheckEvery,
		lastCheckedWorkers: time.Now(),
		domainConfig:       config.DomainConfig,
		materialization:    mat,
		recipe:             recipe,
		domains:            make(map[boostpb.DomainIndex]*domainrpc.Handle),
		domainNodes:        make(map[boostpb.DomainIndex][]graph.NodeIdx),
		remap:              make(map[boostpb.DomainIndex]map[graph.NodeIdx]boostpb.IndexPair),
		chanCoordinator:    boostrpc.NewChannelCoordinator(),
		workers:            make(map[domainrpc.WorkerID]*domainrpc.Worker),
		readAddrs:          make(map[domainrpc.WorkerID]string),
		topo:               ts,
		log:                log.With(zap.Int64("epoch", state.Epoch)),
		BuildDomain:        domain.ToProto,
	}
}

func (ctrl *Controller) IsReady() bool {
	return ctrl.expectedWorkerCount > 0 && len(ctrl.workers) >= ctrl.expectedWorkerCount
}

func (ctrl *Controller) registerWorker(req *vtboostpb.TopoWorkerEntry) {
	workerid := domainrpc.WorkerID(req.Uuid)
	ws, err := domainrpc.NewWorker(workerid, req.AdminAddr)
	if err != nil {
		ctrl.log.Error("failed to register worker", zap.Error(err))
		return
	}

	ctrl.workers[workerid] = ws
	ctrl.readAddrs[workerid] = req.ReaderAddr
}

func (ctrl *Controller) placeDomain(ctx context.Context, idx boostpb.DomainIndex, maybeShardNumber *uint, innodes []nodeWithAge) (*domainrpc.Handle, error) {
	nodes := new(flownode.Map)

	for _, n := range innodes {
		node := ctrl.ingredients.Value(n.Idx).Finalize(ctrl.ingredients)
		nodes.Insert(node.LocalAddr(), node)
	}

	var (
		shards       = common.UnwrapOr(maybeShardNumber, 1)
		domainshards = make([]domainrpc.ShardHandle, shards)
		announce     = make([]*boostpb.DomainDescriptor, shards)
		workers      = maps.Values(ctrl.workers)
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

			var request = &boostpb.AssignDomainRequest{
				From: &boostpb.WorkerSource{
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

			announce[shardN] = &boostpb.DomainDescriptor{
				Id:    idx,
				Shard: shardN,
				Addr:  resp.Addr,
			}
			domainshards[shardN] = domainrpc.NewShardHandle(wrk.UUID, shardClient)
			return nil
		})
	}

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	ctrl.log.Info("placed new domain", idx.Zap(), zap.Int("shard_count", len(domainshards)), zap.Any("shards", domainshards))

	for _, endpoint := range ctrl.workers {
		for _, dd := range announce {
			if _, err := endpoint.Client.DomainBooted(ctx, &boostpb.DomainBootedRequest{
				From: &boostpb.WorkerSource{
					SourceUuid: ctrl.uuid.String(),
					Epoch:      ctrl.epoch,
				},
				Domain: dd,
			}); err != nil {
				return nil, err
			}
		}
	}

	return domainrpc.NewHandle(idx, domainshards), nil
}

func (ctrl *Controller) outputs() map[string]graph.NodeIdx {
	var outputs = make(map[string]graph.NodeIdx)

	ext := ctrl.ingredients.Externals(graph.DirectionOutgoing)
	for ext.Next() {
		node := ctrl.ingredients.Value(ext.Current)
		if r := node.AsReader(); r != nil {
			outputs[node.Name] = r.IsFor()
		}
	}
	return outputs
}

func (ctrl *Controller) inputs() map[string]graph.NodeIdx {
	var inputs = make(map[string]graph.NodeIdx)

	neigh := ctrl.ingredients.NeighborsDirected(graph.Source, graph.DirectionOutgoing)
	for neigh.Next() {
		base := ctrl.ingredients.Value(neigh.Current)
		if !base.IsAnyBase() {
			panic("root in the graph it not a base ???")
		}
		inputs[base.Name] = neigh.Current
	}
	return inputs
}

func (ctrl *Controller) tableDescriptor(name string) *boostpb.TableDescriptor {
	node, ok := ctrl.recipe.NodeAddrFor(name)
	if !ok {
		inputs := ctrl.inputs()
		node, ok = inputs[name]
		if !ok {
			return nil
		}
	}

	base := ctrl.ingredients.Value(node)
	dom := ctrl.domains[base.Domain()]
	baseOperator := base.AsBase()
	dropped := baseOperator.GetDropped()

	tbl := &boostpb.TableDescriptor{
		Txs:          nil,
		Node:         base.GlobalAddr(),
		Addr:         base.LocalAddr(),
		KeyIsPrimary: false,
		Key:          nil,
		Dropped:      dropped,
		TableName:    base.Name,
		Columns:      nil,
		Schema:       slices.Clone(base.Schema()),
	}

	key := base.SuggestIndexes(node)[node]
	if key == nil {
		if col, _, ok := base.Sharding().ByColumn(); ok {
			key = []int{col}
		}
	} else {
		tbl.KeyIsPrimary = true
	}
	tbl.Key = key

	for s := uint(0); s < dom.Shards(); s++ {
		tbl.Txs = append(tbl.Txs, &boostpb.DomainDescriptor{
			Id:    base.Domain(),
			Shard: s,
			Addr:  ctrl.chanCoordinator.GetAddr(base.Domain(), s),
		})
	}
	for n, f := range base.Fields() {
		if _, ok := dropped[n]; !ok {
			tbl.Columns = append(tbl.Columns, f)
		}
	}

	return tbl
}

func (ctrl *Controller) viewDescriptor(nodeIdx graph.NodeIdx) *vtboostpb.Materialization_ViewDescriptor {
	node := ctrl.ingredients.Value(nodeIdx)
	reader := node.AsReader()
	domain := ctrl.domains[node.Domain()]
	if domain == nil {
		panic("unknown domain?")
	}

	var shards []string
	for s := uint(0); s < domain.Shards(); s++ {
		shards = append(shards, ctrl.readAddrs[domain.Assignment(s)])
	}

	var params = reader.Parameters()
	var keySchema []*querypb.Field
	for i, col := range reader.Key() {
		flags := uint32(0)
		if params[i].Multi {
			flags |= uint32(querypb.MySqlFlag_MULTIPLE_KEY_FLAG)
		}

		tt := node.ColumnType(ctrl.ingredients, col)
		keySchema = append(keySchema, &querypb.Field{
			Name:    params[i].Name,
			Type:    tt.T,
			Charset: uint32(tt.Collation),
			Flags:   flags,
		})
	}

	var fields = node.Fields()
	var schema []*querypb.Field
	for col := 0; col < reader.PublicColumnLength(); col++ {
		tt := node.ColumnType(ctrl.ingredients, col)
		schema = append(schema, &querypb.Field{
			Name:    fields[col],
			Type:    tt.T,
			Charset: uint32(tt.Collation),
		})
	}

	orderCols, orderColsDesc, orderLimit := reader.Order()

	return &vtboostpb.Materialization_ViewDescriptor{
		Name:          node.Name,
		Node:          uint32(nodeIdx),
		Schema:        schema,
		KeySchema:     keySchema,
		Shards:        shards,
		TopkOrderCols: orderCols,
		TopkOrderDesc: orderColsDesc,
		TopkLimit:     orderLimit,
	}
}

func (ctrl *Controller) viewDescriptorForName(name string) *vtboostpb.Materialization_ViewDescriptor {
	node, ok := ctrl.recipe.NodeAddrFor(name)
	if !ok {
		// if the recipe doesn't know about this query, traverse the graph.
		// we need this do deal with manually constructed graphs (e.g., in tests).
		outputs := ctrl.outputs()
		node, ok = outputs[name]
		if !ok {
			return nil
		}
	}

	if alias, ok := ctrl.recipe.ResolveAlias(name); ok {
		name = alias
	}

	r, ok := ctrl.findViewFor(node, name)
	if !ok {
		return nil
	}

	return ctrl.viewDescriptor(r)
}

func (ctrl *Controller) findViewFor(node graph.NodeIdx, name string) (graph.NodeIdx, bool) {
	bfs := graph.NewBFSVisitor(ctrl.ingredients, node)
	for bfs.Next() {
		n := ctrl.ingredients.Value(bfs.Current)
		if r := n.AsReader(); r != nil {
			if r.IsFor() == node && n.Name == name {
				return bfs.Current, true
			}
		}
	}
	return graph.InvalidNode, false
}

func (ctrl *Controller) GetMaterializations() ([]*vtboostpb.Materialization, error) {
	viewsByName := make(map[string]*boostplan.CachedQuery)
	var res []*vtboostpb.Materialization
	for _, v := range ctrl.recipe.GetAllPublicViews() {
		viewsByName[v.Name] = v
	}

	ctrl.ingredients.ForEachValue(func(n *flownode.Node) bool {
		if n.IsReader() {
			view, ok := viewsByName[n.Name]
			if !ok {
				return true
			}

			normalizedSQL := topowatcher.ParametrizeQuery(view.Statement)
			viewDescriptor := ctrl.viewDescriptor(n.GlobalAddr())
			bounds, fullyMaterialized := topowatcher.GenerateBoundsForQuery(view.Statement, viewDescriptor.KeySchema)

			res = append(res, &vtboostpb.Materialization{
				Query:             view.CachedQuery,
				NormalizedSql:     normalizedSQL,
				Bounds:            bounds,
				FullyMaterialized: fullyMaterialized,
				View:              viewDescriptor,
			})
		}
		return true
	})

	return res, nil
}

func (ctrl *Controller) ModifyRecipe(ctx context.Context, recipepb *vtboostpb.Recipe, si *boostplan.SchemaInformation, migrate Migrator) (*boostplan.ActivationResult, error) {
	if recipepb.Version <= ctrl.recipe.Version() {
		return &boostplan.ActivationResult{}, nil
	}

	newrecipe, err := boostplan.NewVersionedRecipe(ctrl.recipe, recipepb)
	if err != nil {
		return nil, err
	}

	return ctrl.applyRecipe(ctx, newrecipe, si, migrate)
}

func (ctrl *Controller) cleanupRecipe(ctx context.Context, activation *boostplan.ActivationResult) error {
	var removedBases []graph.NodeIdx
	var removedOther []graph.NodeIdx
	for _, ni := range activation.NodesRemoved {
		if ctrl.ingredients.Value(ni).IsAnyBase() {
			removedBases = append(removedBases, ni)
		} else {
			removedOther = append(removedOther, ni)
		}
	}

	var topoRemovals []graph.NodeIdx
	var topo = graph.NewTopoVisitor(ctrl.ingredients)
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

	for _, base := range removedBases {
		children := ctrl.ingredients.NeighborsDirected(base, graph.DirectionOutgoing).Count()
		if children != 0 {
			panic("trying to remove base that still had children")
		}

		if err := ctrl.removeNodes(ctx, []graph.NodeIdx{base}); err != nil {
			removalErrors = append(removalErrors, err)
		}
	}

	return multierr.Combine(removalErrors...)
}

func (ctrl *Controller) applyRecipe(ctx context.Context, newrecipe *boostplan.VersionedRecipe, si *boostplan.SchemaInformation, migrate Migrator) (*boostplan.ActivationResult, error) {
	ctrl.log.Info("applying new recipe", zap.Int64("version", newrecipe.Version()))

	mig := migrate(ctx, ctrl)
	mig = NewTrackedMigration(mig, ctrl)

	activation, err := mig.Activate(newrecipe, si)
	if err != nil {
		return nil, err
	}

	err = mig.Commit(newrecipe.Upqueries())
	if err != nil {
		return nil, err
	}

	ctrl.recipe = newrecipe

	// cleanupRecipe only removes old nodes that are no longer in use; we do not fail recipe application
	// if it fails because it's not a required cluster change
	return activation, ctrl.cleanupRecipe(ctx, activation)
}

func (ctrl *Controller) removeNodes(ctx context.Context, removals []graph.NodeIdx) error {
	var domainRemovals = make(map[boostpb.DomainIndex][]boostpb.LocalNodeIndex)

	for _, ni := range removals {
		ctrl.log.Debug("removed node", ni.Zap())

		node := ctrl.ingredients.Value(ni)
		node.Remove()

		dom := node.Domain()
		domainRemovals[dom] = append(domainRemovals[dom], node.LocalAddr())
	}

	for dom, nodes := range domainRemovals {
		ctrl.log.Debug("notifying domain of node removals", dom.Zap())

		var pkt boostpb.Packet
		pkt.Inner = &boostpb.Packet_RemoveNodes_{
			RemoveNodes: &boostpb.Packet_RemoveNodes{
				Nodes: nodes,
			},
		}

		err := ctrl.domains[dom].SendToHealthy(ctx, &pkt, ctrl.workers)
		if err != nil {
			// TODO: ignore error if worker is down for good
			return err
		}
	}
	return nil
}

func (ctrl *Controller) removeLeaf(ctx context.Context, leaf graph.NodeIdx) error {
	var removals []graph.NodeIdx
	var start = leaf

	if ctrl.ingredients.Value(leaf).IsSource() {
		panic("trying to remove source")
	}

	ctrl.log.Info("computing removals", leaf.ZapField("remove_node"))

	nchildren := ctrl.ingredients.NeighborsDirected(leaf, graph.DirectionOutgoing).Count()
	if nchildren > 0 {
		// This query leaf node has children -- typically, these are readers, but they can also
		// include egress nodes or other, dependent queries. We need to find the actual reader,
		// and remove that.

		if nchildren != 1 {
			panic("trying to remove node with multiple children")
		}

		var readers []graph.NodeIdx
		var bfs = graph.NewBFSVisitor(ctrl.ingredients, leaf)
		for bfs.Next() {
			n := ctrl.ingredients.Value(bfs.Current)
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

	if ctrl.ingredients.NeighborsDirected(leaf, graph.DirectionOutgoing).Count() != 0 {
		panic("node should have no children left")
	}

	var nodes = []graph.NodeIdx{leaf}
	for len(nodes) > 0 {
		node := nodes[len(nodes)-1]
		nodes = nodes[:len(nodes)-1]

		parents := ctrl.ingredients.NeighborsDirected(node, graph.DirectionIncoming)
		for parents.Next() {
			parent := parents.Current
			edge := ctrl.ingredients.FindEdge(parent, node)
			ctrl.ingredients.RemoveEdge(edge)

			pp := ctrl.ingredients.Value(parent)
			if !pp.IsSource() &&
				!pp.IsAnyBase() &&
				(parent == start || !ctrl.recipe.IsLeafAddress(parent)) &&
				ctrl.ingredients.NeighborsDirected(parent, graph.DirectionOutgoing).Count() == 0 {
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

	ep.LoadRecipe(ctrl.ingredients, ctrl.materialization, ctrl.recipe.Recipe)

	for _, wrk := range ctrl.workers {
		client := wrk.Client
		wg.Go(func() error {
			resp, err := client.MemoryStats(ctx, &boostpb.MemoryStatsRequest{})
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
	ctrl.log.Info("preparing eviction plan for distributed eviction")

	plan, err := ctrl.PrepareEvictionPlan(ctx)
	if err != nil {
		return nil, err
	}

	plan.SetCustomLimits(forceLimits)
	evictions := plan.Evictions()

	ctrl.log.Info("distributed eviction plan ready", zap.Int("evictions", len(evictions)))

	var errs []error
	for _, ev := range plan.Evictions() {
		dom, err := ctrl.chanCoordinator.GetClient(ev.Domain.Domain, ev.Domain.Shard)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		err = dom.ProcessAsync(ctx, &boostpb.Packet{
			Inner: &boostpb.Packet_Evict_{Evict: &boostpb.Packet_Evict{
				Node:     ev.Local,
				NumBytes: ev.Evict,
			}},
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
