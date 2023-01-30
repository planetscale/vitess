package graph

import (
	"math"

	"go.uber.org/zap"

	math2 "vitess.io/vitess/go/vt/vtorc/util"
)

type Direction int

const (
	DirectionOutgoing Direction = iota
	DirectionIncoming
)

type NodeIdx uint32
type EdgeIdx uint32

func (n NodeIdx) IsRoot() bool {
	return n == Root
}

func (n NodeIdx) Zap() zap.Field {
	return zap.Uint32("node_idx", uint32(n))
}

func (n NodeIdx) ZapField(field string) zap.Field {
	return zap.Uint32(field, uint32(n))
}

const Root NodeIdx = 0
const InvalidNode NodeIdx = math.MaxUint32
const InvalidEdge EdgeIdx = math.MaxUint32

type NodeIdxPair struct {
	One NodeIdx
	Two NodeIdx
}

type Node[N any] struct {
	Value N
	next  [2]EdgeIdx
}

type Edge struct {
	next [2]EdgeIdx
	node [2]NodeIdx
}

func (e *Edge) Source() NodeIdx {
	return e.node[0]
}

func (e *Edge) Target() NodeIdx {
	return e.node[1]
}

type Graph[N any] struct {
	nodes []Node[N]
	edges []Edge
}

func (g *Graph[N]) NodeCount() int {
	return len(g.nodes)
}

func (g *Graph[N]) EdgeCount() int {
	return len(g.edges)
}

func (g *Graph[N]) RawEdges() []Edge {
	return g.edges
}

func (g *Graph[N]) AddNode(n N) NodeIdx {
	node := Node[N]{
		Value: n,
		next:  [2]EdgeIdx{InvalidEdge, InvalidEdge},
	}
	nodeIdx := NodeIdx(len(g.nodes))
	g.nodes = append(g.nodes, node)
	return nodeIdx
}

func (g *Graph[N]) AddEdge(from, to NodeIdx) EdgeIdx {
	edgeIdx := EdgeIdx(len(g.edges))
	edge := Edge{
		node: [2]NodeIdx{from, to},
	}

	a, b := int(from), int(to)
	max := math2.MaxInt(a, b)
	switch {
	case max >= len(g.nodes):
		panic("Graph.AddEdge: node indices out of bounds")
	case a == b:
		an := &g.nodes[max]
		edge.next = an.next
		an.next[0] = edgeIdx
		an.next[1] = edgeIdx
	default:
		an := &g.nodes[a]
		bn := &g.nodes[b]
		edge.next[0] = an.next[0]
		edge.next[1] = bn.next[1]
		an.next[0] = edgeIdx
		bn.next[1] = edgeIdx
	}

	g.edges = append(g.edges, edge)
	return edgeIdx
}

func (g *Graph[N]) RemoveEdge(e EdgeIdx) bool {
	if int(e) < len(g.edges) {
		edge := &g.edges[int(e)]
		g.changeEdgeLinks(edge.node, e, edge.next)
		return g.removeEdgeAdjustIndices(e)
	}
	return false
}

func (g *Graph[N]) changeEdgeLinks(edgeNode [2]NodeIdx, e EdgeIdx, edgeNext [2]EdgeIdx) {
	for d := 0; d < 2; d++ {
		node := &g.nodes[int(edgeNode[d])]
		fst := node.next[d]
		if fst == e {
			node.next[d] = edgeNext[d]
		} else {
			edges := EdgeWalker{edges: g.edges, next: fst, dir: Direction(d)}
			for edges.Next() {
				if edges.Current.next[d] == e {
					edges.Current.next[d] = edgeNext[d]
					break
				}
			}
		}
	}
}

func (g *Graph[N]) removeEdgeAdjustIndices(e EdgeIdx) bool {
	g.edges[int(e)] = g.edges[len(g.edges)-1]
	g.edges = g.edges[:len(g.edges)-1]

	if int(e) < len(g.edges) {
		swap := g.edges[int(e)].node
		swappedE := EdgeIdx(len(g.edges))
		g.changeEdgeLinks(swap, swappedE, [2]EdgeIdx{e, e})
	}

	return true
}

func (g *Graph[N]) Value(idx NodeIdx) N {
	return g.nodes[int(idx)].Value
}

func (g *Graph[N]) LookupValue(i NodeIdx) (value N, found bool) {
	idx := int(i)
	if idx < len(g.nodes) {
		value = g.nodes[idx].Value
		found = true
	}
	return
}

func (g *Graph[N]) ForEachValue(each func(v N) bool) {
	for _, n := range g.nodes {
		if !each(n.Value) {
			break
		}
	}
}

func (g *Graph[N]) FindEdge(a, b NodeIdx) EdgeIdx {
	if int(a) < len(g.nodes) {
		return g.findEdgeDirectedFromNode(&g.nodes[int(a)], b)
	}
	return InvalidEdge
}

func (g *Graph[N]) findEdgeDirectedFromNode(node *Node[N], b NodeIdx) EdgeIdx {
	edix := node.next[0]
	for int(edix) < len(g.edges) {
		edge := g.edges[int(edix)]
		if edge.node[1] == b {
			return edix
		}
		edix = edge.next[0]
	}
	return InvalidEdge
}

func (g *Graph[N]) NeighborsDirected(a NodeIdx, dir Direction) *Neighbors {
	iter := g.NeighborsUndirected(a)
	iter.next[1-int(dir)] = InvalidEdge
	iter.skipStart = InvalidNode
	return iter
}

func (g *Graph[N]) NeighborsUndirected(a NodeIdx) *Neighbors {
	var iter Neighbors
	iter.skipStart = a
	iter.edges = g.edges

	idx := int(a)
	if idx < len(g.nodes) {
		iter.next = g.nodes[idx].next
	} else {
		iter.next = [2]EdgeIdx{InvalidEdge, InvalidEdge}
	}

	return &iter
}

func (g *Graph[N]) Externals(dir Direction) *Externals[N] {
	return &Externals[N]{
		nodes: g.nodes,
		dir:   dir,
	}
}

type Neighbors struct {
	Current NodeIdx

	skipStart NodeIdx
	edges     []Edge
	next      [2]EdgeIdx
}

func (n *Neighbors) Next() bool {
	ei := int(n.next[0])
	if ei < len(n.edges) {
		edge := n.edges[ei]
		n.next[0] = edge.next[0]
		n.Current = edge.node[1]
		return true
	}

	for {
		ei := int(n.next[1])
		if ei >= len(n.edges) {
			break
		}

		edge := n.edges[ei]
		n.next[1] = edge.next[1]
		if edge.node[0] != n.skipStart {
			n.Current = edge.node[0]
			return true
		}
	}

	return false
}

func (n *Neighbors) Collect(s []NodeIdx) []NodeIdx {
	for n.Next() {
		s = append(s, n.Current)
	}
	return s
}

func (n *Neighbors) First() NodeIdx {
	if !n.Next() {
		panic("Neighbors is empty")
	}
	return n.Current
}

func (n *Neighbors) Count() (count int) {
	for n.Next() {
		count++
	}
	return
}

type EdgeWalker struct {
	Current *Edge

	edges []Edge
	next  EdgeIdx
	dir   Direction
}

func (e *EdgeWalker) Next() bool {
	if int(e.next) < len(e.edges) {
		e.Current = &e.edges[e.next]
		e.next = e.Current.next[e.dir]
		return true
	}
	return false
}
