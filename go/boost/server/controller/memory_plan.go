package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common/graphviz"
	"vitess.io/vitess/go/boost/common/humanize"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
)

type nodeUsage struct {
	ptr         *flownode.Node
	memPerShard []int64
	memTotal    int64
	roots       map[boostpb.GraphNodeIdx]struct{}
}

func (n *nodeUsage) evict(evictions []Eviction, evict int64) []Eviction {
	for s := 0; s < len(n.memPerShard); s++ {
		evictions = append(evictions, Eviction{
			Domain: boostpb.DomainAddr{
				Domain: n.ptr.Domain(),
				Shard:  uint(s),
			},
			Node:  n.ptr.GlobalAddr(),
			Local: n.ptr.LocalAddr(),
			Evict: evict / int64(len(n.memPerShard)),
		})
	}
	return evictions
}

type rootUsage struct {
	root  boostpb.GraphNodeIdx
	view  *boostplan.CachedQuery
	nodes []*nodeUsage
}

func (ru *rootUsage) memTotal() (total int64) {
	for _, n := range ru.nodes {
		total += n.memTotal
	}
	return
}

type EvictionPlan struct {
	nodes  map[boostpb.GraphNodeIdx]*nodeUsage
	roots  []*rootUsage
	limits map[string]int64
}

func (ep *EvictionPlan) MarshalJSON() ([]byte, error) {
	type jsonNode struct {
		Idx         uint32  `json:"graph_idx"`
		Type        string  `json:"type"`
		Mem         int64   `json:"memory"`
		MemPerShard []int64 `json:"memory_per_shard,omitempty"`
	}
	type jsonQuery struct {
		Name      string      `json:"name"`
		Sql       string      `json:"sql"`
		MaxMemory int64       `json:"max_memory"`
		CurMemory int64       `json:"cur_memory"`
		Nodes     []*jsonNode `json:"nodes"`
	}

	var qq []*jsonQuery
	for _, root := range ep.roots {
		jq := &jsonQuery{
			Name:      root.view.Name,
			Sql:       root.view.Sql,
			MaxMemory: root.view.MaxMemoryUsage,
			CurMemory: root.memTotal(),
			Nodes:     nil,
		}
		for _, n := range root.nodes {
			jn := &jsonNode{
				Idx:         uint32(n.ptr.GlobalAddr()),
				Type:        n.ptr.Kind(),
				Mem:         n.memTotal,
				MemPerShard: nil,
			}
			if len(n.memPerShard) > 1 {
				jn.MemPerShard = n.memPerShard
			}
			jq.Nodes = append(jq.Nodes, jn)
		}
		qq = append(qq, jq)
	}

	return json.Marshal(qq)
}

func NewEvictionPlan() *EvictionPlan {
	return &EvictionPlan{
		nodes: map[boostpb.GraphNodeIdx]*nodeUsage{},
	}
}

func (ep *EvictionPlan) LoadRecipe(g *graph.Graph[*flownode.Node], rcp *boostplan.Recipe) {
	viewsByName := make(map[string]*boostplan.CachedQuery)
	for _, v := range rcp.GetAllPublicViews() {
		viewsByName[v.Name] = v
	}

	g.ForEachValue(func(root *flownode.Node) bool {
		if !root.IsReader() {
			return true
		}

		view, ok := viewsByName[root.Name]
		if !ok {
			return true
		}

		var (
			rootAddr = root.GlobalAddr()
			queue    = []boostpb.GraphNodeIdx{rootAddr}
			seen     = map[boostpb.GraphNodeIdx]bool{graph.InvalidNode: true}
			walk     []*nodeUsage
		)

		for len(queue) > 0 {
			nodeAddr := queue[0]
			queue = queue[1:]

			if seen[nodeAddr] {
				continue
			}

			node := g.Value(nodeAddr)
			usage, ok := ep.nodes[nodeAddr]
			if !ok {
				usage = &nodeUsage{
					ptr:         node,
					memPerShard: make([]int64, node.Sharding().GetShards()),
					roots:       map[boostpb.GraphNodeIdx]struct{}{},
				}
				ep.nodes[nodeAddr] = usage
			}

			usage.roots[rootAddr] = struct{}{}
			walk = append(walk, usage)

			// add parents to the queue
			queue = g.NeighborsDirected(nodeAddr, graph.DirectionIncoming).Collect(queue)
			seen[nodeAddr] = true
		}

		ep.roots = append(ep.roots, &rootUsage{view: view, nodes: walk, root: rootAddr})
		return true
	})
}

func (ep *EvictionPlan) LoadMemoryStats(resp *boostpb.MemoryStatsResponse) error {
	for _, usage := range resp.NodeUsage {
		if node, ok := ep.nodes[usage.Node]; ok {
			if node.ptr.Domain() != usage.Domain.Domain {
				return fmt.Errorf("node %d belongs to domain %d but was reported as %d", usage.Node, node.ptr.Domain(), usage.Domain.Domain)
			}
			memuse := &node.memPerShard[usage.Domain.Shard]
			if *memuse != 0 {
				return fmt.Errorf("node %d has a duplicated shard memory count", usage.Node)
			}
			*memuse = usage.Memory
			node.memTotal += usage.Memory
		}
	}
	return nil
}

type Eviction struct {
	Domain boostpb.DomainAddr
	Node   boostpb.GraphNodeIdx
	Local  boostpb.LocalNodeIndex
	Evict  int64
}

func (e *Eviction) MarshalJSON() ([]byte, error) {
	type evictionJson struct {
		Domain string `json:"domain"`
		Node   uint32 `json:"node"`
		Evict  int64  `json:"evict"`
	}

	return json.Marshal(&evictionJson{
		Domain: fmt.Sprintf("%d/%d", e.Domain.Domain, e.Domain.Shard),
		Node:   uint32(e.Node),
		Evict:  e.Evict,
	})
}

func (ep *EvictionPlan) evictOnRoot(evictions []Eviction, root *rootUsage, evictBytes int64) []Eviction {
	candidates := slices.Clone(root.nodes)
	slices.SortFunc(candidates, func(a, b *nodeUsage) bool {
		return a.memTotal > b.memTotal
	})

	if len(candidates) > 3 {
		candidates = candidates[:3]
	}

	n := int64(len(candidates))
	for i := len(candidates) - 1; i >= 0; i-- {
		// TODO: should this evenly divided or weighted by the size of the domains?
		var target = candidates[i]
		var share = (evictBytes + n - 1) / n
		var evict = share

		n--

		// don't evict from tiny domains (<10% of max)
		if target.memTotal < candidates[0].memTotal/10 {
			continue
		}

		// Do not evict more than half the state in each domain unless
		// this is the only domain left to evict from
		if n > 1 && target.memTotal/2 < share {
			evict = target.memTotal / 2
		}

		evictions = target.evict(evictions, evict)
		evictBytes -= evict
	}

	if evictBytes > 1024*1024 {
		evictions = candidates[0].evict(evictions, evictBytes)
	}

	return evictions
}

func (ep *EvictionPlan) SetCustomLimits(limit map[string]int64) {
	ep.limits = limit
}

func (ep *EvictionPlan) Evictions() []Eviction {
	var evictions []Eviction

	for _, root := range ep.roots {
		max, ok := ep.limits[root.view.Name]
		if !ok {
			max = root.view.MaxMemoryUsage
		}
		if max == 0 {
			continue
		}

		usage := root.memTotal()
		if usage < max {
			continue
		}

		evictions = ep.evictOnRoot(evictions, root, usage-max)
	}
	return evictions
}

func (ep *EvictionPlan) RenderGraphviz(gvz *graphviz.Graph[graph.NodeIdx], queryClustering bool) {
	evictionByNode := make(map[graph.NodeIdx][]Eviction)
	for _, ev := range ep.Evictions() {
		evictionByNode[ev.Node] = append(evictionByNode[ev.Node], ev)
	}

	for idx, usage := range ep.nodes {
		if idx == 0 {
			// Skip the graph's source
			continue
		}
		if node, ok := gvz.Node(idx); ok {
			evictions := evictionByNode[idx]

			for shard, mem := range usage.memPerShard {
				row := graphviz.Fmt("<I>shard[%d] alloc</I>: <B>%s</B>", shard, humanize.IBytes(uint64(mem)))

				for _, ev := range evictions {
					if ev.Domain.Shard == uint(shard) {
						row = row + graphviz.Fmt(" <FONT COLOR=\"red\">(evict <B>%s</B>)</FONT>", humanize.IBytes(uint64(ev.Evict)))
						break
					}
				}

				node.Row(row)
			}

			if queryClustering && len(usage.roots) == 1 {
				for root := range usage.roots {
					node.Subgraph = fmt.Sprintf("query_%d", root)
					break
				}
			}
		}
	}

	for _, root := range ep.roots {
		if node, ok := gvz.Node(root.root); ok {
			node.TableAttr["BORDER"] = "3"
			node.TableAttr["CELLBORDER"] = "1"
			node.Row(graphviz.Fmt("Total Query Usage: <B>%s</B>", humanize.IBytes(uint64(root.memTotal()))))
		}
	}
}

func (ctrl *Controller) PrepareEvictionPlan(ctx context.Context) (*EvictionPlan, error) {
	var mu sync.Mutex
	var wg errgroup.Group
	var ep = NewEvictionPlan()

	ep.LoadRecipe(ctrl.ingredients, ctrl.recipe.Recipe)

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

func (ctrl *Controller) PerformDistributedEviction(ctx context.Context, forceLimits map[string]int64) (*EvictionPlan, error) {
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
