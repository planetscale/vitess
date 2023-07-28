package materialization

import (
	"encoding/json"
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostrpc/service"
	"vitess.io/vitess/go/boost/common/graphviz"
	"vitess.io/vitess/go/boost/common/humanize"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
)

type nodeUsage struct {
	ptr         *flownode.Node
	memPerShard []int64
	memTotal    int64
	roots       map[dataflow.NodeIdx]struct{}
	status      dataflow.MaterializationStatus
}

func (n *nodeUsage) evict(evictions []Eviction, evict int64) []Eviction {
	for s := 0; s < len(n.memPerShard); s++ {
		evictions = append(evictions, Eviction{
			Domain: dataflow.DomainAddr{
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
	root  dataflow.NodeIdx
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
	nodes  map[dataflow.NodeIdx]*nodeUsage
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
		PublicID  string      `json:"public_id"`
		SQL       string      `json:"sql"`
		MaxMemory int64       `json:"max_memory"`
		CurMemory int64       `json:"cur_memory"`
		Nodes     []*jsonNode `json:"nodes"`
	}

	var qq []*jsonQuery
	for _, root := range ep.roots {
		jq := &jsonQuery{
			PublicID:  root.view.PublicId,
			SQL:       root.view.Sql,
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
		nodes: map[dataflow.NodeIdx]*nodeUsage{},
	}
}

func (ep *EvictionPlan) LoadRecipe(g *graph.Graph[*flownode.Node], mat *Materialization, rcp *boostplan.Recipe) {
	viewsByPublicID := make(map[string]*boostplan.CachedQuery)
	for _, v := range rcp.GetAllPublicViews() {
		viewsByPublicID[v.PublicId] = v
	}

	g.ForEachValue(func(root *flownode.Node) bool {
		r := root.AsReader()
		if r == nil {
			return true
		}

		view, ok := viewsByPublicID[r.PublicID()]
		if !ok {
			return true
		}

		var (
			rootAddr = root.GlobalAddr()
			queue    = []dataflow.NodeIdx{rootAddr}
			seen     = map[dataflow.NodeIdx]bool{graph.InvalidNode: true}
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
					roots:       map[dataflow.NodeIdx]struct{}{},
					status:      mat.GetStatus(node),
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

func (ep *EvictionPlan) LoadMemoryStats(resp *service.MemoryStatsResponse) error {
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
	Domain dataflow.DomainAddr
	Node   dataflow.NodeIdx
	Local  dataflow.LocalNodeIdx
	Evict  int64
}

func (e *Eviction) MarshalJSON() ([]byte, error) {
	type evictionJSON struct {
		Domain string `json:"domain"`
		Node   uint32 `json:"node"`
		Evict  int64  `json:"evict"`
	}

	return json.Marshal(&evictionJSON{
		Domain: fmt.Sprintf("%d/%d", e.Domain.Domain, e.Domain.Shard),
		Node:   uint32(e.Node),
		Evict:  e.Evict,
	})
}

func (ep *EvictionPlan) evictOnRoot(evictions []Eviction, root *rootUsage, evictBytes int64) []Eviction {
	candidates := make([]*nodeUsage, 0, len(root.nodes))
	for _, cand := range root.nodes {
		if cand.status == dataflow.MaterializationPartial {
			candidates = append(candidates, cand)
		}
	}
	slices.SortFunc(candidates, func(a, b *nodeUsage) int {
		// Sort in reverse order.
		switch {
		case a.memTotal > b.memTotal:
			return -1
		case a.memTotal < b.memTotal:
			return 1
		default:
			return 0
		}
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
		max, ok := ep.limits[root.view.PublicId]
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
		if idx.IsRoot() {
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
