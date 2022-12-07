package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/materialization"
	"vitess.io/vitess/go/vt/sqlparser"
)

type migration struct {
	context.Context

	log    *zap.Logger
	target *Controller
	graph  *graph.Graph[*flownode.Node]

	added     map[graph.NodeIdx]bool
	readers   map[graph.NodeIdx]graph.NodeIdx
	upqueries map[graph.NodeIdx]sqlparser.SelectStatement

	start time.Time
	uuid  uuid.UUID
}

func (mig *migration) PlannedUpquery(node graph.NodeIdx) (sqlparser.SelectStatement, bool) {
	sel, ok := mig.upqueries[node]
	return sel, ok
}

var _ materialization.Migration = (*migration)(nil)
var _ boostplan.Migration = (*migration)(nil)

func (mig *migration) Log() *zap.Logger {
	return mig.log
}

func (mig *migration) Graph() *graph.Graph[*flownode.Node] {
	return mig.graph
}

func (mig *migration) SendPacket(domain boostpb.DomainIndex, pkt *boostpb.Packet) error {
	ctrl := mig.target
	return ctrl.domains[domain].SendToHealthy(mig, pkt, ctrl.workers)
}

func (mig *migration) SendPacketSync(domain boostpb.DomainIndex, pkt *boostpb.SyncPacket) error {
	ctrl := mig.target
	return ctrl.domains[domain].SendToHealthySync(mig, pkt, ctrl.workers)
}

func (mig *migration) SendPacketToShard(domain boostpb.DomainIndex, shard uint, pkt *boostpb.Packet) error {
	ctrl := mig.target
	return ctrl.domains[domain].SendToHealthyShard(mig, shard, pkt, ctrl.workers)
}

func (mig *migration) DomainShards(domain boostpb.DomainIndex) uint {
	return mig.target.domains[domain].Shards()
}

func (mig *migration) sortInTopoOrder(new map[graph.NodeIdx]bool) (topolist []graph.NodeIdx) {
	topolist = make([]graph.NodeIdx, 0, len(new))
	topo := graph.NewTopoVisitor(mig.graph)
	for topo.Next() {
		node := topo.Current
		if node.IsSource() {
			continue
		}
		if mig.graph.Value(node).IsDropped() {
			continue
		}
		if !new[node] {
			continue
		}
		topolist = append(topolist, node)
	}
	return
}

func (mig *migration) AddIngredient(name string, fields []string, impl flownode.NodeImpl, upquerySQL sqlparser.SelectStatement) graph.NodeIdx {
	newNode := flownode.New(name, fields, impl)
	newNode.OnConnected(mig.graph)

	parents := newNode.Ancestors()

	ni := mig.graph.AddNode(newNode)
	mig.added[ni] = true
	for _, parent := range parents {
		mig.graph.AddEdge(parent, ni)
	}

	if upquerySQL != nil {
		mig.upqueries[ni] = upquerySQL
	}

	return ni
}

func (mig *migration) AddBase(name string, fields []string, b flownode.AnyBase) graph.NodeIdx {
	base := flownode.New(name, fields, b)
	ni := mig.graph.AddNode(base)
	mig.added[ni] = true
	mig.graph.AddEdge(graph.Source, ni)
	return ni
}

func (mig *migration) ensureReaderFor(na graph.NodeIdx, name string, connect func(r *flownode.Reader)) *flownode.Node {
	fn, found := mig.readers[na]
	if found {
		return mig.graph.Value(fn)
	}
	r := flownode.NewReader(na)
	connect(r)

	var rn *flownode.Node
	if name != "" {
		rn = mig.graph.Value(na).NamedMirror(r, name)
	} else {
		rn = mig.graph.Value(na).Mirror(r)
	}

	ri := mig.graph.AddNode(rn)
	mig.graph.AddEdge(na, ri)
	mig.added[ri] = true
	mig.readers[na] = ri
	return rn
}

func (mig *migration) Maintain(name string, na graph.NodeIdx, key []int, parameters []boostpb.ViewParameter, colLen int) {
	mig.ensureReaderFor(na, name, func(reader *flownode.Reader) {
		reader.OnConnected(mig.graph, key, parameters, colLen)
	})
}

func (mig *migration) Commit() error {
	target := mig.target
	newNodes := maps.Clone(mig.added)
	topo := mig.sortInTopoOrder(newNodes)

	var swapped0 map[graph.NodeIdxPair]graph.NodeIdx
	if sharding := target.sharding; sharding != nil {
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

				if mig.graph.FindEdge(src, instead) != graph.InvalidEdge {
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

	changedDomains := make(map[boostpb.DomainIndex]struct{})
	for _, ni := range sortedNew {
		node := mig.graph.Value(ni)
		if !node.IsDropped() {
			changedDomains[node.Domain()] = struct{}{}
		}
	}

	domainNewNodes := make(map[boostpb.DomainIndex][]graph.NodeIdx)
	for _, ni := range sortedNew {
		if !ni.IsSource() {
			node := mig.graph.Value(ni)
			if !node.IsDropped() {
				dom := node.Domain()
				domainNewNodes[dom] = append(domainNewNodes[dom], ni)
			}
		}
	}

	// Assign local addresses to all new nodes, and initialize them
	for dom, nodes := range domainNewNodes {
		var nnodes int
		if rm, ok := target.remap[dom]; ok {
			nnodes = len(rm)
		}

		if len(nodes) == 0 {
			continue
		}

		// Give local addresses to every (new) node
		for _, ni := range nodes {
			ip := boostpb.NewIndexPair(ni)
			ip.SetLocal(boostpb.LocalNodeIndex(nnodes))
			mig.graph.Value(ni).SetFinalizedAddr(ip)

			if rm, ok := target.remap[dom]; ok {
				rm[ni] = ip
			} else {
				target.remap[dom] = map[graph.NodeIdx]boostpb.IndexPair{ni: ip}
			}

			nnodes++
		}

		// Initialize each new node
		for _, ni := range nodes {
			node := mig.graph.Value(ni)
			if node.IsInternal() {
				// Figure out all the remappings that have happened
				// NOTE: this has to be *per node*, since a shared parent may be remapped
				// differently to different children (due to sharding for example). we just
				// allocate it once though.
				remap := maps.Clone(target.remap[dom])
				for pair, instead := range swapped {
					dst := pair.One
					src := pair.Two
					if dst != ni {
						continue
					}

					remap[src] = target.remap[dom][instead]
				}

				node.OnCommit(remap)
			}
		}
	}

	if sharding := target.sharding; sharding != nil {
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

	for ni := range newNodes {
		n := mig.graph.Value(ni)
		if !ni.IsSource() && !n.IsDropped() {
			dom := n.Domain()
			target.domainNodes[dom] = append(target.domainNodes[dom], ni)
		}
	}

	uninformedDomainNodes := make(map[boostpb.DomainIndex][]nodeWithAge)
	for di := range changedDomains {
		var newnodes []nodeWithAge

		for _, ni := range target.domainNodes[di] {
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
		if _, found := target.domains[dom]; found {
			continue
		}

		nodes := uninformedDomainNodes[dom]
		delete(uninformedDomainNodes, dom)

		numshards := mig.graph.Value(nodes[0].Idx).Sharding().TryGetShards()
		d, err := target.placeDomain(mig, dom, numshards, nodes)
		if err != nil {
			return err
		}
		target.domains[dom] = d
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
	if err := target.materialization.Commit(mig, newNodes); err != nil {
		return err
	}

	mig.log.Info("recipe application complete", zap.Duration("duration", time.Since(mig.start)))
	return nil
}

func (mig *migration) describeExternalTable(node graph.NodeIdx) *boostpb.ExternalTableDescriptor {
	base := mig.graph.Value(node)

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

	shards := mig.DomainShards(base.Domain())
	for s := uint(0); s < shards; s++ {
		tbl.Txs = append(tbl.Txs, &boostpb.DomainAddr{Domain: base.Domain(), Shard: s})
	}
	return tbl
}

func (mig *migration) streamSetup(newnodes map[graph.NodeIdx]bool) error {
	var externals []*boostpb.ExternalTableDescriptor
	for n := range newnodes {
		node := mig.graph.Value(n)
		if node.IsExternalBase() {
			externals = append(externals, mig.describeExternalTable(n))
		}
	}

	var request = boostpb.AssignStreamRequest{Tables: externals}

	// TODO; do not pick a worker at random
	for _, worker := range mig.target.workers {
		if _, err := worker.Client.AssignStream(mig, &request); err != nil {
			return err
		}
		break
	}

	return nil
}

func (mig *migration) MaintainAnonymous(n graph.NodeIdx, key []int) {
	var params []boostpb.ViewParameter
	for i := range key {
		params = append(params, boostpb.ViewParameter{
			Name: fmt.Sprintf("k%d", i),
		})
	}
	mig.ensureReaderFor(n, "", func(r *flownode.Reader) {
		r.OnConnected(mig.graph, key, params, 0)
	})
}

func (mig *migration) Activate(recipe *boostplan.VersionedRecipe, schema *boostplan.SchemaInformation) (*boostplan.ActivationResult, error) {
	return recipe.Activate(mig, schema)
}

type Migration interface {
	materialization.Migration
	boostplan.Migration

	Activate(recipe *boostplan.VersionedRecipe, schema *boostplan.SchemaInformation) (*boostplan.ActivationResult, error)
	Commit() error
}

type Migrator func(ctx context.Context, target *Controller) Migration

func NewMigration(ctx context.Context, target *Controller) Migration {
	miguuid := uuid.New()
	return &migration{
		Context:   ctx,
		log:       target.log.With(zap.String("migration", miguuid.String())),
		target:    target,
		graph:     target.ingredients,
		added:     make(map[graph.NodeIdx]bool),
		readers:   make(map[graph.NodeIdx]graph.NodeIdx),
		upqueries: make(map[graph.NodeIdx]sqlparser.SelectStatement),
		start:     time.Now(),
		uuid:      miguuid,
	}
}
