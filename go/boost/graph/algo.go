package graph

type Topo[N any] struct {
	Current NodeIdx

	g       *Graph[N]
	tovisit []NodeIdx
	ordered map[NodeIdx]struct{}
}

func NewTopoVisitor[N any](g *Graph[N]) *Topo[N] {
	topo := Topo[N]{
		g:       g,
		ordered: make(map[NodeIdx]struct{}),
	}

	for i := range g.nodes {
		idx := NodeIdx(i)
		if !g.NeighborsDirected(idx, DirectionIncoming).Next() {
			topo.tovisit = append(topo.tovisit, idx)
		}
	}

	return &topo
}

func (topo *Topo[N]) Next() bool {
	for len(topo.tovisit) > 0 {
		nix := topo.tovisit[len(topo.tovisit)-1]
		topo.tovisit = topo.tovisit[:len(topo.tovisit)-1]

		if _, visited := topo.ordered[nix]; visited {
			continue
		}
		topo.ordered[nix] = struct{}{}

		neigh := topo.g.NeighborsDirected(nix, DirectionOutgoing)
		for neigh.Next() {
			var allVisited = true
			reverse := topo.g.NeighborsDirected(neigh.Current, DirectionIncoming)
			for reverse.Next() {
				if _, visited := topo.ordered[reverse.Current]; !visited {
					allVisited = false
				}
			}
			if allVisited {
				topo.tovisit = append(topo.tovisit, neigh.Current)
			}
		}

		topo.Current = nix
		return true
	}
	return false
}

type BFS[N any] struct {
	Current NodeIdx

	g          *Graph[N]
	discovered bitset
	stack      []NodeIdx
}

func NewBFSVisitor[N any](g *Graph[N], start NodeIdx) *BFS[N] {
	discovered := bitsetWithCapacity(g.NodeCount())
	discovered.visit(start)
	return &BFS[N]{
		g:          g,
		discovered: discovered,
		stack:      []NodeIdx{start},
	}
}

func (it *BFS[N]) Next() bool {
	if len(it.stack) > 0 {
		it.Current = it.stack[0]
		it.stack = it.stack[1:]

		neigh := it.g.NeighborsDirected(it.Current, DirectionOutgoing)
		for neigh.Next() {
			if it.discovered.visit(neigh.Current) {
				it.stack = append(it.stack, neigh.Current)
			}
		}
		return true
	}
	it.Current = InvalidNode
	return false
}

type bitset struct {
	words  []uint32
	length int
}

func bitsetWithCapacity(bits int) bitset {
	blocks := bits / 32
	rem := bits % 32
	if rem > 0 {
		blocks++
	}
	return bitset{
		words:  make([]uint32, blocks),
		length: bits,
	}
}

func (b *bitset) visit(bit NodeIdx) bool {
	word := &b.words[bit/32]
	mask := uint32(1) << (bit % 32)
	firstVisit := (*word & mask) == 0
	*word |= mask
	return firstVisit
}

func (b *bitset) isVisited(bit NodeIdx) bool {
	return (b.words[bit/32] & 1 << (bit % 32)) != 0
}

type DFS[N any] struct {
	Current NodeIdx

	g          *Graph[N]
	stack      []NodeIdx
	discovered bitset
}

func NewEmptyDFS[N any](g *Graph[N]) *DFS[N] {
	return &DFS[N]{
		g:          g,
		discovered: bitsetWithCapacity(g.NodeCount()),
	}
}

func (it *DFS[N]) MoveTo(start NodeIdx) {
	it.stack = it.stack[:0]
	it.stack = append(it.stack, start)
}

func (it *DFS[N]) Next() bool {
	for len(it.stack) > 0 {
		it.Current = it.stack[len(it.stack)-1]
		it.stack = it.stack[:len(it.stack)-1]

		if it.discovered.visit(it.Current) {
			succ := it.g.NeighborsDirected(it.Current, DirectionOutgoing)
			for succ.Next() {
				if !it.discovered.isVisited(succ.Current) {
					it.stack = append(it.stack, succ.Current)
				}
			}
			return true
		}
	}
	it.Current = InvalidNode
	return false
}

func HasPathConnecting[N any](g *Graph[N], from, to NodeIdx) bool {
	dfs := NewEmptyDFS(g)
	dfs.MoveTo(from)
	for dfs.Next() {
		if dfs.Current == to {
			return true
		}
	}
	return false
}

type Externals[N any] struct {
	Current NodeIdx

	idx   int
	nodes []Node[N]
	dir   Direction
}

func (e *Externals[N]) Next() bool {
	k := int(e.dir)
	for e.idx < len(e.nodes) {
		idx := e.idx
		node := e.nodes[e.idx]
		e.idx++

		if node.next[k] == InvalidEdge {
			e.Current = NodeIdx(idx)
			return true
		} else {
			continue
		}
	}
	return false
}
