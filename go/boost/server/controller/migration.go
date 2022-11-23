package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
)

type Migration struct {
	ctrl    *Controller
	added   map[graph.NodeIdx]bool
	readers map[graph.NodeIdx]graph.NodeIdx

	start time.Time
	uuid  uuid.UUID
}

var _ boostplan.Migration = (*Migration)(nil)

func (m *Migration) AddIngredient(name string, fields []string, impl flownode.NodeImpl) graph.NodeIdx {
	newNode := flownode.New(name, fields, impl)
	newNode.OnConnected(m.ctrl.ingredients)

	parents := newNode.Ancestors()

	ni := m.ctrl.ingredients.AddNode(newNode)
	m.added[ni] = true
	for _, parent := range parents {
		m.ctrl.ingredients.AddEdge(parent, ni)
	}
	return ni
}

func (m *Migration) AddBase(name string, fields []string, b flownode.AnyBase) graph.NodeIdx {
	base := flownode.New(name, fields, b)
	ni := m.ctrl.ingredients.AddNode(base)
	m.added[ni] = true
	m.ctrl.ingredients.AddEdge(m.ctrl.source, ni)
	return ni
}

func (m *Migration) ensureReaderFor(na graph.NodeIdx, name string, connect func(r *flownode.Reader)) *flownode.Node {
	fn, found := m.readers[na]
	if found {
		return m.ctrl.ingredients.Value(fn)
	}
	r := flownode.NewReader(na)
	connect(r)

	var rn *flownode.Node
	if name != "" {
		rn = m.ctrl.ingredients.Value(na).NamedMirror(r, name)
	} else {
		rn = m.ctrl.ingredients.Value(na).Mirror(r)
	}

	ri := m.ctrl.ingredients.AddNode(rn)
	m.ctrl.ingredients.AddEdge(na, ri)
	m.added[ri] = true
	m.readers[na] = ri
	return rn
}

func (m *Migration) Maintain(name string, na graph.NodeIdx, key []int, parameters []boostpb.ViewParameter, colLen int) {
	m.ensureReaderFor(na, name, func(reader *flownode.Reader) {
		reader.OnConnected(m.ctrl.ingredients, key, parameters, colLen)
	})
}

func (m *Migration) Commit(ctx context.Context, up *boostplan.Upqueries) error {
	ctx = common.ContextWithLogger(ctx, m.ctrl.log.With(zap.String("migration", m.uuid.String())))

	ctrl := m.ctrl
	newNodes := maps.Clone(m.added)
	topo := ctrl.topoOrder(newNodes)

	var swapped0 map[graph.NodeIdxPair]graph.NodeIdx
	if sharding := ctrl.sharding; sharding != nil {
		var err error
		topo, swapped0, err = migrationShard(ctx, m.ctrl.ingredients, newNodes, topo, *sharding)
		if err != nil {
			return err
		}
	} else {
		swapped0 = make(map[graph.NodeIdxPair]graph.NodeIdx)
	}

	migrationAssign(ctrl.ingredients, topo, &ctrl.nDomains)
	swapped1 := migrationAddRouting(ctrl.ingredients, ctrl.source, newNodes, topo)
	topo = ctrl.topoOrder(newNodes)

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

				if ctrl.ingredients.FindEdge(src, instead) != graph.InvalidEdge {
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
		node := ctrl.ingredients.Value(ni)
		if !node.IsDropped() {
			changedDomains[node.Domain()] = struct{}{}
		}
	}

	domainNewNodes := make(map[boostpb.DomainIndex][]graph.NodeIdx)
	for _, ni := range sortedNew {
		if ni != ctrl.source {
			node := ctrl.ingredients.Value(ni)
			if !node.IsDropped() {
				dom := node.Domain()
				domainNewNodes[dom] = append(domainNewNodes[dom], ni)
			}
		}
	}

	// Assign local addresses to all new nodes, and initialize them
	for dom, nodes := range domainNewNodes {
		var nnodes int
		if rm, ok := ctrl.remap[dom]; ok {
			nnodes = len(rm)
		}

		if len(nodes) == 0 {
			continue
		}

		// Give local addresses to every (new) node
		for _, ni := range nodes {
			ip := boostpb.NewIndexPair(ni)
			ip.SetLocal(boostpb.LocalNodeIndex(nnodes))
			ctrl.ingredients.Value(ni).SetFinalizedAddr(ip)

			if rm, ok := ctrl.remap[dom]; ok {
				rm[ni] = ip
			} else {
				ctrl.remap[dom] = map[graph.NodeIdx]boostpb.IndexPair{ni: ip}
			}

			nnodes++
		}

		// Initialize each new node
		for _, ni := range nodes {
			node := ctrl.ingredients.Value(ni)
			if node.IsInternal() {
				// Figure out all the remappings that have happened
				// NOTE: this has to be *per node*, since a shared parent may be remapped
				// differently to different children (due to sharding for example). we just
				// allocate it once though.
				remap := maps.Clone(ctrl.remap[dom])
				for pair, instead := range swapped {
					dst := pair.One
					src := pair.Two
					if dst != ni {
						continue
					}
					remap[src] = ctrl.remap[dom][instead]
				}

				node.OnCommit(remap)
			}
		}
	}

	if sharding := ctrl.sharding; sharding != nil {
		if err := migrationValidateSharding(m.ctrl.ingredients, topo, *sharding); err != nil {
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
		n := ctrl.ingredients.Value(ni)
		if ni != ctrl.source && !n.IsDropped() {
			di := n.Domain()
			ctrl.domainNodes[di] = append(ctrl.domainNodes[di], ni)
		}
	}

	uninformedDomainNodes := make(map[boostpb.DomainIndex][]NewNode)
	for di := range changedDomains {
		var m []NewNode

		for _, ni := range ctrl.domainNodes[di] {
			_, found := newNodes[ni]
			m = append(m, NewNode{ni, found})
		}

		sort.SliceStable(m, func(i, j int) bool {
			return m[i].Idx < m[j].Idx
		})

		uninformedDomainNodes[di] = m
	}

	// Boot up new domains (they'll ignore all updates for now)
	for dom := range changedDomains {
		if _, found := ctrl.domains[dom]; found {
			continue
		}

		nodes := uninformedDomainNodes[dom]
		delete(uninformedDomainNodes, dom)

		numshards := ctrl.ingredients.Value(nodes[0].Idx).Sharding().TryGetShards()
		d, err := ctrl.PlaceDomain(ctx, dom, numshards, nodes)
		if err != nil {
			return err
		}
		ctrl.domains[dom] = d
	}

	// Add any new nodes to existing domains (they'll also ignore all updates for now)
	if err := migrationAugmentationInform(ctx, ctrl, uninformedDomainNodes); err != nil {
		return err
	}

	// Set up inter-domain connections
	// NOTE: once we do this, we are making existing domains block on new domains!
	if err := migrationRoutingConnect(ctx, ctrl.ingredients, ctrl.domains, ctrl.workers, newNodes); err != nil {
		return err
	}

	if err := migrationStreamSetup(ctx, ctrl, newNodes); err != nil {
		return err
	}

	// And now, the last piece of the puzzle -- set up materializations
	if err := ctrl.materialization.Commit(ctx, ctrl.ingredients, newNodes, ctrl.domains, ctrl.workers); err != nil {
		return err
	}

	return nil
}

func migrationStreamSetup(ctx context.Context, mainline *Controller, newnodes map[graph.NodeIdx]bool) error {
	var externals []*boostpb.ExternalTableDescriptor

	for n := range newnodes {
		node := mainline.ingredients.Value(n)
		if node.IsExternalBase() {
			externals = append(externals, mainline.externalTableDescriptor(n))
		}
	}

	var request = boostpb.AssignStreamRequest{Tables: externals}

	// TODO; do not pick a worker at random
	for _, worker := range mainline.workers {
		if _, err := worker.Client.AssignStream(ctx, &request); err != nil {
			return err
		}
		break
	}

	return nil
}

func (m *Migration) MaintainAnonymous(n graph.NodeIdx, key []int) {
	var params []boostpb.ViewParameter
	for i := range key {
		params = append(params, boostpb.ViewParameter{
			Name: fmt.Sprintf("k%d", i),
		})
	}
	m.ensureReaderFor(n, "", func(r *flownode.Reader) {
		r.OnConnected(m.ctrl.ingredients, key, params, 0)
	})
}

func NewMigration(inner *Controller) *Migration {
	return &Migration{
		ctrl:    inner,
		added:   make(map[graph.NodeIdx]bool),
		readers: make(map[graph.NodeIdx]graph.NodeIdx),
		start:   time.Now(),
		uuid:    uuid.New(),
	}
}
