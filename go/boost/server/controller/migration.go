package controller

import (
	"context"
	"fmt"
	"runtime/debug"
	"sort"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostrpc/service"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/domainrpc"
	"vitess.io/vitess/go/boost/server/controller/materialization"
	"vitess.io/vitess/go/vt/sqlparser"
)

type migration struct {
	context.Context

	log    *zap.Logger
	target MigrationTarget

	added     map[graph.NodeIdx]bool
	readers   map[graph.NodeIdx]graph.NodeIdx
	upqueries map[graph.NodeIdx]sqlparser.SelectStatement

	start time.Time
	uuid  uuid.UUID
}

func (mig *migration) AddUpquery(idx graph.NodeIdx, statement sqlparser.SelectStatement) {
	mig.upqueries[idx] = statement
}

func (mig *migration) GetUpquery(node graph.NodeIdx) (sqlparser.SelectStatement, bool) {
	sel, ok := mig.upqueries[node]
	return sel, ok
}

var _ materialization.Migration = (*migration)(nil)
var _ boostplan.Migration = (*migration)(nil)

func (mig *migration) Log() *zap.Logger {
	return mig.log
}

func (mig *migration) Graph() *graph.Graph[*flownode.Node] {
	return mig.target.graph()
}

func (mig *migration) Send(domain dataflow.DomainIdx) domainrpc.Client {
	return mig.target.domainClient(mig, domain)
}

func (mig *migration) SendToShard(domain dataflow.DomainIdx, shard uint) domainrpc.Client {
	return mig.target.domainShardClient(mig, domain, shard)
}

func (mig *migration) DomainShards(domain dataflow.DomainIdx) uint {
	return mig.target.domainShards(domain)
}

func (mig *migration) sortInTopoOrder(new map[graph.NodeIdx]bool) (topolist []graph.NodeIdx) {
	g := mig.target.graph()
	topolist = make([]graph.NodeIdx, 0, len(new))
	topo := graph.NewTopoVisitor(g)
	for topo.Next() {
		node := topo.Current
		if node.IsRoot() {
			continue
		}
		if g.Value(node).IsDropped() {
			continue
		}
		if !new[node] {
			continue
		}
		topolist = append(topolist, node)
	}
	return
}

func (mig *migration) AddIngredient(name string, fields []string, impl flownode.NodeImpl) (graph.NodeIdx, error) {
	g := mig.target.graph()
	newNode := flownode.New(name, fields, impl)

	err := newNode.OnConnected(g)
	if err != nil {
		return graph.InvalidNode, err
	}

	parents := newNode.Ancestors()

	ni := g.AddNode(newNode)
	mig.added[ni] = true
	for _, parent := range parents {
		g.AddEdge(parent, ni)
	}

	return ni, nil
}

func (mig *migration) AddTable(name string, fields []string, t *flownode.Table) graph.NodeIdx {
	g := mig.target.graph()
	table := flownode.New(name, fields, t)
	ni := g.AddNode(table)
	mig.added[ni] = true
	g.AddEdge(graph.Root, ni)
	return ni
}

func (mig *migration) AddView(name string, na graph.NodeIdx, connect func(g *graph.Graph[*flownode.Node], r *flownode.Reader)) {
	if _, found := mig.readers[na]; found {
		return
	}
	g := mig.target.graph()
	r := flownode.NewReader(na)
	connect(g, r)

	var rn *flownode.Node
	if name != "" {
		rn = g.Value(na).NamedMirror(r, name)
	} else {
		rn = g.Value(na).Mirror(r)
	}

	ri := g.AddNode(rn)
	g.AddEdge(na, ri)
	mig.added[ri] = true
	mig.readers[na] = ri
}

func (mig *migration) Commit() error {
	target := mig.target
	g := target.graph()
	newNodes := maps.Clone(mig.added)
	topo := mig.sortInTopoOrder(newNodes)

	var swapped0 map[graph.NodeIdxPair]graph.NodeIdx
	if sharding := target.sharding(); sharding != nil {
		var err error
		topo, swapped0, err = mig.shard(newNodes, topo, *sharding)
		if err != nil {
			return err
		}
	} else {
		swapped0 = make(map[graph.NodeIdxPair]graph.NodeIdx)
	}

	mig.assign(topo)
	swapped1 := mig.addRouting(newNodes, topo)
	topo = mig.sortInTopoOrder(newNodes)

	for pair, instead := range swapped1 {
		src := pair.Two
		if instead0, exists := swapped0[pair]; exists {
			if instead != instead0 {
				// This can happen if sharding decides to add a Sharder *under* a node,
				// and routing decides to add an ingress/egress pair between that node
				// and the Sharder. It's perfectly okay, but we should prefer the
				// "bottommost" swap to take place (i.e., the node that is *now*
				// closest to the dst node). This *should* be the sharding node, unless
				// routing added an ingress *under* the Sharder. We resolve the
				// collision by looking at which translation currently has an adge from
				// `src`, and then picking the *other*, since that must then be node
				// below.

				if g.FindEdge(src, instead) != graph.InvalidEdge {
					// src -> instead -> instead0 -> [children]
					// from [children]'s perspective, we should use instead0 for from, so
					// we can just ignore the `instead` swap.
				} else {
					swapped0[pair] = instead
				}
			}
		} else {
			swapped0[pair] = instead
		}

		// we may also already have swapped the parents of some node *to* `src`. in
		// swapped0. we want to change that mapping as well, since lookups in swapped
		// aren't recursive.
		for k, instead0 := range swapped0 {
			if instead0 == src {
				swapped0[k] = instead
			}
		}
	}

	swapped := swapped0
	sortedNew := maps.Keys(newNodes)
	sort.Slice(sortedNew, func(i, j int) bool {
		return sortedNew[i] < sortedNew[j]
	})

	changedDomains := make(map[dataflow.DomainIdx]struct{})
	for _, ni := range sortedNew {
		node := g.Value(ni)
		if !node.IsDropped() {
			changedDomains[node.Domain()] = struct{}{}
		}
	}

	domainNewNodes := make(map[dataflow.DomainIdx][]graph.NodeIdx)
	for _, ni := range sortedNew {
		if !ni.IsRoot() {
			node := g.Value(ni)
			if !node.IsDropped() {
				dom := node.Domain()
				domainNewNodes[dom] = append(domainNewNodes[dom], ni)
			}
		}
	}

	// Assign local addresses to all new nodes, and initialize them
	for dom, nodes := range domainNewNodes {
		rm := target.domainMapping(dom)
		nnodes := len(rm)

		if len(nodes) == 0 {
			continue
		}

		// Give local addresses to every (new) node
		for _, ni := range nodes {
			ip := dataflow.NewIndexPair(ni)
			ip.SetLocal(dataflow.LocalNodeIdx(nnodes))
			g.Value(ni).SetFinalizedAddr(ip)

			rm[ni] = ip
			nnodes++
		}

		// Initialize each new node
		for _, ni := range nodes {
			node := g.Value(ni)
			if node.IsInternal() {
				// Figure out all the remappings that have happened
				// NOTE: this has to be *per node*, since a shared parent may be remapped
				// differently to different children (due to sharding for example). we just
				// allocate it once though.
				remap := maps.Clone(rm)
				for pair, instead := range swapped {
					dst := pair.One
					src := pair.Two
					if dst != ni {
						continue
					}

					remap[src] = rm[instead]
				}

				node.OnCommit(remap)
			}
		}
	}

	if sharding := target.sharding(); sharding != nil {
		if err := mig.validateSharding(topo, *sharding); err != nil {
			return err
		}
	}

	// at this point, we've hooked up the graph such that, for any given domain, the graph
	// looks like this:
	//
	//      o (egress)
	//     +.\......................
	//     :  o (ingress)
	//     :  |
	//     :  o-------------+
	//     :  |             |
	//     :  o             o
	//     :  |             |
	//     :  o (egress)    o (egress)
	//     +..|...........+.|..........
	//     :  o (ingress) : o (ingress)
	//     :  |\          :  \
	//     :  | \         :   o
	//
	// etc.

	domainNodes := target.domainNodes()

	for ni := range newNodes {
		n := g.Value(ni)
		if !ni.IsRoot() && !n.IsDropped() {
			dom := n.Domain()
			domainNodes[dom] = append(domainNodes[dom], ni)
		}
	}

	uninformedDomainNodes := make(map[dataflow.DomainIdx][]nodeWithAge)
	for di := range changedDomains {
		var newnodes []nodeWithAge

		for _, ni := range domainNodes[di] {
			_, found := newNodes[ni]
			newnodes = append(newnodes, nodeWithAge{ni, found})
		}

		sort.SliceStable(newnodes, func(i, j int) bool {
			return newnodes[i].Idx < newnodes[j].Idx
		})

		uninformedDomainNodes[di] = newnodes
	}

	// Boot up new domains (they'll ignore all updates for now)
	for dom := range changedDomains {
		if target.domainExists(dom) {
			continue
		}

		nodes := uninformedDomainNodes[dom]
		delete(uninformedDomainNodes, dom)

		numshards := g.Value(nodes[0].Idx).Sharding().TryGetShards()
		err := target.domainPlace(mig, dom, numshards, nodes)
		if err != nil {
			return err
		}
	}

	// Add any new nodes to existing domains (they'll also ignore all updates for now)
	if err := mig.augmentationInform(uninformedDomainNodes); err != nil {
		return err
	}

	// Set up inter-domain connections
	// NOTE: once we do this, we are making existing domains block on new domains!
	if err := mig.routingConnect(newNodes); err != nil {
		return err
	}

	if err := mig.streamSetup(newNodes); err != nil {
		return err
	}

	// And now, the last piece of the puzzle -- set up materializations
	if err := target.materialization().Commit(mig, newNodes); err != nil {
		return err
	}

	mig.log.Info("recipe application complete", zap.Duration("duration", time.Since(mig.start)))
	return nil
}

func (mig *migration) describeExternalTable(node graph.NodeIdx) *service.ExternalTableDescriptor {
	table := mig.target.graph().Value(node)
	desc := &service.ExternalTableDescriptor{
		Txs:          nil,
		Node:         table.GlobalAddr(),
		Addr:         table.LocalAddr(),
		KeyIsPrimary: false,
		Key:          nil,
		TableName:    table.AsTable().Name(),
		Columns:      slices.Clone(table.Fields()),
		Schema:       slices.Clone(table.Schema()),
		Keyspace:     table.AsTable().Keyspace(),
	}

	key := table.SuggestIndexes(node)[node]
	if key == nil {
		if col, _, ok := table.Sharding().ByColumn(); ok {
			key = []int{col}
		}
	} else {
		desc.KeyIsPrimary = true
	}
	desc.Key = key

	shards := mig.DomainShards(table.Domain())
	for s := uint(0); s < shards; s++ {
		desc.Txs = append(desc.Txs, &dataflow.DomainAddr{Domain: table.Domain(), Shard: s})
	}
	return desc
}

func (mig *migration) streamSetup(newnodes map[graph.NodeIdx]bool) error {
	g := mig.target.graph()
	var externals []*service.ExternalTableDescriptor
	for n := range newnodes {
		node := g.Value(n)
		if node.IsTable() {
			externals = append(externals, mig.describeExternalTable(n))
		}
	}

	var request = service.AssignStreamRequest{Tables: externals}

	return mig.target.domainAssignStream(mig, &request)
}

func (mig *migration) Activate(recipe *boostplan.VersionedRecipe, schema *boostplan.SchemaInformation) (*boostplan.ActivationResult, error) {
	return recipe.Activate(mig, schema)
}

type MigrationTarget interface {
	graph() *graph.Graph[*flownode.Node]
	sharding() *uint
	materialization() *materialization.Materialization

	domainNext() dataflow.DomainIdx
	domainNodes() map[dataflow.DomainIdx][]graph.NodeIdx
	domainMapping(dom dataflow.DomainIdx) map[graph.NodeIdx]dataflow.IndexPair
	domainExists(idx dataflow.DomainIdx) bool
	domainShards(idx dataflow.DomainIdx) uint
	domainClient(ctx context.Context, idx dataflow.DomainIdx) domainrpc.Client
	domainShardClient(ctx context.Context, idx dataflow.DomainIdx, shard uint) domainrpc.Client
	domainPlace(ctx context.Context, idx dataflow.DomainIdx, maybeShardNumber *uint, innodes []nodeWithAge) error
	domainAssignStream(ctx context.Context, req *service.AssignStreamRequest) error
}

type Migration interface {
	materialization.Migration
	boostplan.Migration

	Activate(recipe *boostplan.VersionedRecipe, schema *boostplan.SchemaInformation) (*boostplan.ActivationResult, error)
	Commit() error
}

func NewMigration(ctx context.Context, log *zap.Logger, target MigrationTarget) Migration {
	miguuid := uuid.New()
	return &migration{
		Context:   ctx,
		log:       log.With(zap.String("migration", miguuid.String())),
		target:    target,
		added:     make(map[graph.NodeIdx]bool),
		readers:   make(map[graph.NodeIdx]graph.NodeIdx),
		upqueries: make(map[graph.NodeIdx]sqlparser.SelectStatement),
		start:     time.Now(),
		uuid:      miguuid,
	}
}

type InternalError struct {
	Err   error
	Stack []byte
}

func (e *InternalError) Error() string {
	return e.Err.Error()
}

func safeMigration(mig Migration, perform func(mig Migration) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &InternalError{
				Err:   fmt.Errorf("migration panicked: %v", r),
				Stack: debug.Stack(),
			}
		}
	}()
	if err = perform(mig); err != nil {
		return err
	}
	return mig.Commit()
}
