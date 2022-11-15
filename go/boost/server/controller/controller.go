package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/common/graphviz"
	"vitess.io/vitess/go/boost/dataflow/domain"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	topowatcher "vitess.io/vitess/go/boost/topo/watcher"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
)

type Controller struct {
	ingredients *graph.Graph[*flownode.Node]

	uuid               uuid.UUID
	source             graph.NodeIdx
	nDomains           uint
	sharding           *uint
	epoch              boostpb.Epoch
	quorum             uint
	heartbeatEvery     time.Duration
	healthcheckEvery   time.Duration
	lastCheckedWorkers time.Time

	domainConfig    *boostpb.DomainConfig
	materialization *Materialization
	recipe          *boostplan.VersionedRecipe
	domains         map[boostpb.DomainIndex]*DomainHandle
	domainNodes     map[boostpb.DomainIndex][]graph.NodeIdx
	remap           map[boostpb.DomainIndex]map[graph.NodeIdx]boostpb.IndexPair
	chanCoordinator *boostrpc.ChannelCoordinator

	workersReady        bool
	expectedWorkerCount int
	workers             map[WorkerID]*Worker
	readAddrs           map[WorkerID]string

	topo *toposerver.Server
	log  *zap.Logger

	BuildDomain domain.BuilderFn
}

func NewController(log *zap.Logger, uuid uuid.UUID, config *boostpb.Config, state *vtboostpb.ControllerState, ts *toposerver.Server) *Controller {
	g := new(graph.Graph[*flownode.Node])
	source := g.AddNode(flownode.New("source", []string{"dummy-field"}, &flownode.Source{}))

	mat := NewMaterialization()
	if !config.PartialEnabled {
		mat.DisablePartial()
	}
	mat.SetFrontierStrategy(config.FrontierStrategy)

	recipe := boostplan.BlankRecipe()
	recipe.EnableReuse(config.Reuse)

	var sharding *uint
	if config.Shards > 0 {
		sharding = &config.Shards
	}

	return &Controller{
		ingredients:        g,
		uuid:               uuid,
		source:             source,
		sharding:           sharding,
		epoch:              boostpb.Epoch(state.Epoch),
		quorum:             config.Quorum,
		heartbeatEvery:     config.HeartbeatEvery,
		healthcheckEvery:   config.HealthcheckEvery,
		lastCheckedWorkers: time.Now(),
		domainConfig:       config.DomainConfig,
		materialization:    mat,
		recipe:             recipe,
		domains:            make(map[boostpb.DomainIndex]*DomainHandle),
		domainNodes:        make(map[boostpb.DomainIndex][]graph.NodeIdx),
		remap:              make(map[boostpb.DomainIndex]map[graph.NodeIdx]boostpb.IndexPair),
		chanCoordinator:    boostrpc.NewChannelCoordinator(),
		workers:            make(map[WorkerID]*Worker),
		readAddrs:          make(map[WorkerID]string),
		topo:               ts,
		log:                log.With(zap.Int64("epoch", state.Epoch)),
		BuildDomain:        domain.ToProto,
	}
}

func (ctrl *Controller) IsReady() bool {
	return ctrl.expectedWorkerCount > 0 && len(ctrl.workers) >= ctrl.expectedWorkerCount
}

func (ctrl *Controller) Migrate(ctx context.Context, perform func(ctx context.Context, mig *Migration) error) error {
	mig := NewMigration(ctrl)
	ctx = common.ContextWithLogger(ctx, ctrl.log.With(zap.Uintptr("migration", uintptr(unsafe.Pointer(mig)))))
	if err := perform(ctx, mig); err != nil {
		return err
	}
	return mig.Commit(ctx)
}

func (ctrl *Controller) NewMigration() *Migration {
	return NewMigration(ctrl)
}

func (ctrl *Controller) TopoOrder(new map[graph.NodeIdx]bool) (topolist []graph.NodeIdx) {
	topolist = make([]graph.NodeIdx, 0, len(new))
	topo := graph.NewTopoVisitor(ctrl.ingredients)
	for topo.Next(ctrl.ingredients) {
		node := topo.Current
		if node == ctrl.source {
			continue
		}
		if ctrl.ingredients.Value(node).IsDropped() {
			continue
		}
		if !new[node] {
			continue
		}
		topolist = append(topolist, node)
	}
	return
}

func (ctrl *Controller) RegisterWorker(req *vtboostpb.TopoWorkerEntry) {
	workerid := WorkerID(req.Uuid)
	ws, err := NewWorker(workerid, req.AdminAddr)
	if err != nil {
		ctrl.log.Error("failed to register worker", zap.Error(err))
		return
	}

	ctrl.workers[workerid] = ws
	ctrl.readAddrs[workerid] = req.ReaderAddr
}

func (ctrl *Controller) PlaceDomain(ctx context.Context, idx boostpb.DomainIndex, maybeShardNumber *uint, innodes []NewNode) (*DomainHandle, error) {
	nodes := new(flownode.Map)

	for _, n := range innodes {
		node := ctrl.ingredients.Value(n.Idx).Finalize(ctrl.ingredients)
		nodes.Insert(node.LocalAddr(), node)
	}

	var (
		shards       = common.UnwrapOr(maybeShardNumber, 1)
		domainshards = make([]DomainShardHandle, shards)
		announce     = make([]*boostpb.DomainDescriptor, shards)
		workers      = maps.Values(ctrl.workers)
		wg           errgroup.Group
	)

	for s := uint(0); s < shards; s++ {
		var shardN = s
		wg.Go(func() error {
			dombuilder := ctrl.BuildDomain(idx, shardN, shards, nodes, ctrl.domainConfig)

			var wrk *Worker
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
			domainshards[shardN] = DomainShardHandle{worker: wrk.UUID, client: shardClient}
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

	return &DomainHandle{
		idx:    idx,
		shards: domainshards,
	}, nil
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

	neigh := ctrl.ingredients.NeighborsDirected(ctrl.source, graph.DirectionOutgoing)
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

func (ctrl *Controller) externalTableDescriptor(node graph.NodeIdx) *boostpb.ExternalTableDescriptor {
	base := ctrl.ingredients.Value(node)
	dom := ctrl.domains[base.Domain()]

	tbl := &boostpb.ExternalTableDescriptor{
		Txs:          nil,
		Node:         base.GlobalAddr(),
		Addr:         base.LocalAddr(),
		KeyIsPrimary: false,
		Key:          nil,
		TableName:    base.Name,
		Columns:      slices.Clone(base.Fields()),
		Schema:       slices.Clone(base.Schema()),
		Keyspace:     base.AsExternalBase().Keyspace(),
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
		tbl.Txs = append(tbl.Txs, &boostpb.DomainAddr{Domain: base.Domain(), Shard: s})
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
	for bfs.Next(ctrl.ingredients) {
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

func (ctrl *Controller) Graphviz(ctx context.Context, buf *strings.Builder, req *vtboostpb.GraphvizRequest) error {
	var gvz = graphviz.NewGraph[graph.NodeIdx]()
	gvz.Clustering = req.Clustering != vtboostpb.GraphvizRequest_NONE

	renderGraphviz(gvz, ctrl.ingredients, ctrl.materialization, req.Clustering == vtboostpb.GraphvizRequest_DOMAIN)

	if req.MemoryStats {
		plan, err := ctrl.PrepareEvictionPlan(ctx)
		if err != nil {
			return err
		}
		if req.ForceMemoryLimits != nil {
			plan.SetCustomLimits(req.ForceMemoryLimits)
		}
		plan.RenderGraphviz(gvz, req.Clustering == vtboostpb.GraphvizRequest_QUERY)
	}

	gvz.Render(buf)
	return nil
}

var errInvalidEpoch = errors.New("epoch race (somebody else already updated our controller state?)")

func (rt *RecipeTracker) updateRecipeStatus(state *vtboostpb.ControllerState, update func(status *vtboostpb.ControllerState_RecipeStatus)) error {
	if boostpb.Epoch(state.Epoch) > rt.ctrl.epoch {
		return errInvalidEpoch
	}
	if state.RecipeVersionStatus == nil {
		state.RecipeVersionStatus = make(map[int64]*vtboostpb.ControllerState_RecipeStatus)
	}
	version := rt.recipe.Version()
	status, ok := state.RecipeVersionStatus[version]
	if !ok {
		status = &vtboostpb.ControllerState_RecipeStatus{Version: version}
		state.RecipeVersionStatus[version] = status
	}
	update(status)
	return nil
}

type RecipeTracker struct {
	ctrl   *Controller
	recipe *boostplan.VersionedRecipe
}

func parseErrors(multiError error) (systemErrors []string, queryErrors map[string]string) {
	queryErrors = make(map[string]string)
	for _, err := range multierr.Errors(multiError) {
		if scoped, ok := err.(boostplan.QueryError); ok {
			queryErrors[scoped.QueryPublicID()] = scoped.Error()
		} else {
			systemErrors = append(systemErrors, err.Error())
		}
	}
	return
}

func (rt *RecipeTracker) BeforeCommit(ctx context.Context, activation *boostplan.ActivationResult, planErr error) {
	systemErrors, queryErrors := parseErrors(planErr)

	_, err := rt.ctrl.topo.UpdateControllerState(ctx, func(state *vtboostpb.ControllerState) error {
		return rt.updateRecipeStatus(state, func(status *vtboostpb.ControllerState_RecipeStatus) {
			status.Progress = vtboostpb.ControllerState_RecipeStatus_APPLYING
			status.SystemErrors = append(status.SystemErrors, systemErrors...)

			for publicID, err := range queryErrors {
				status.Queries = append(status.Queries, &vtboostpb.ControllerState_RecipeStatus_Query{
					QueryPublicId: publicID,
					Progress:      vtboostpb.ControllerState_RecipeStatus_FAILED,
					Error:         err,
				})
			}

			for _, existing := range activation.QueriesUnchanged {
				status.Queries = append(status.Queries, &vtboostpb.ControllerState_RecipeStatus_Query{
					QueryPublicId: existing.PublicId,
					Progress:      vtboostpb.ControllerState_RecipeStatus_READY,
				})
			}

			for _, added := range activation.QueriesAdded {
				if _, failed := queryErrors[added.PublicId]; failed {
					continue
				}
				status.Queries = append(status.Queries, &vtboostpb.ControllerState_RecipeStatus_Query{
					QueryPublicId: added.PublicId,
					Progress:      vtboostpb.ControllerState_RecipeStatus_APPLYING,
				})
			}

			for _, removed := range activation.QueriesRemoved {
				if _, failed := queryErrors[removed.PublicId]; failed {
					continue
				}
				status.Queries = append(status.Queries, &vtboostpb.ControllerState_RecipeStatus_Query{
					QueryPublicId: removed.PublicId,
					Progress:      vtboostpb.ControllerState_RecipeStatus_REMOVING,
				})
			}
		})
	})
	if err != nil {
		rt.ctrl.log.Error("UpdateControllerState failed; recipe may not be properly applied", zap.Error(err))
	}
}

func (rt *RecipeTracker) AfterCommit(ctx context.Context, commitErr error) {
	systemErrors, queryErrors := parseErrors(commitErr)
	success := len(systemErrors) == 0 && len(queryErrors) == 0

	_, err := rt.ctrl.topo.UpdateControllerState(ctx, func(state *vtboostpb.ControllerState) error {
		if success {
			// Upgrade recipe version if we've committed cleanly
			state.RecipeVersion = rt.recipe.Version()
		}

		return rt.updateRecipeStatus(state, func(status *vtboostpb.ControllerState_RecipeStatus) {
			if success {
				status.Progress = vtboostpb.ControllerState_RecipeStatus_READY
			} else {
				status.Progress = vtboostpb.ControllerState_RecipeStatus_FAILED
			}

			status.SystemErrors = append(status.SystemErrors, systemErrors...)

			for _, query := range status.Queries {
				switch query.Progress {
				case vtboostpb.ControllerState_RecipeStatus_FAILED:
					// nothing to do; query was previously failed

				case vtboostpb.ControllerState_RecipeStatus_APPLYING, vtboostpb.ControllerState_RecipeStatus_REMOVING:
					if err, ok := queryErrors[query.QueryPublicId]; ok {
						query.Progress = vtboostpb.ControllerState_RecipeStatus_FAILED
						query.Error = err
					} else {
						query.Progress = vtboostpb.ControllerState_RecipeStatus_READY
					}
				}
			}
		})
	})
	if err != nil {
		rt.ctrl.log.Error("UpdateControllerState failed; recipe may not be properly applied", zap.Error(err))
	}
}

func (ctrl *Controller) ModifyRecipe(ctx context.Context, recipepb *vtboostpb.Recipe, si *boostplan.SchemaInformation) (*boostplan.ActivationResult, error) {
	if recipepb.Version <= ctrl.recipe.Version() {
		return &boostplan.ActivationResult{}, nil
	}

	newrecipe, err := boostplan.NewVersionedRecipe(ctrl.recipe, recipepb)
	if err != nil {
		return nil, err
	}

	return ctrl.applyRecipe(ctx, newrecipe, si)
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
	for topo.Next(ctrl.ingredients) {
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

func (ctrl *Controller) applyRecipe(ctx context.Context, newrecipe *boostplan.VersionedRecipe, si *boostplan.SchemaInformation) (*boostplan.ActivationResult, error) {
	var tracker = &RecipeTracker{ctrl: ctrl, recipe: newrecipe}
	var activation *boostplan.ActivationResult

	err := ctrl.Migrate(ctx, func(ctx context.Context, mig *Migration) error {
		var err error
		activation, err = newrecipe.Activate(ctx, mig, si)
		tracker.BeforeCommit(ctx, activation, err)
		return err
	})

	tracker.AfterCommit(ctx, err)
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
		for bfs.Next(ctrl.ingredients) {
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
