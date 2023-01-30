package materialization

import (
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
)

type OnJoin func(graph.NodeIdx, []int, []graph.NodeIdx) graph.NodeIdx

func OnJoinNone(graph.NodeIdx, []int, []graph.NodeIdx) graph.NodeIdx {
	return graph.InvalidNode
}

type PathElement struct {
	Node    graph.NodeIdx
	Columns []int
}

type Path []PathElement

func (p Path) Compare(other Path) int {
	return slices.CompareFunc(p, other, func(a, b PathElement) int {
		if a.Node < b.Node {
			return -1
		}
		if a.Node > b.Node {
			return 1
		}
		return slices.Compare(a.Columns, b.Columns)
	})
}

func ProvenanceOf(g *graph.Graph[*flownode.Node], node graph.NodeIdx, columns []int, onJoin OnJoin) [][]PathElement {
	return graphTrace(g, onJoin, []PathElement{{node, columns}})
}

func columnsAreSome(columns []int) bool {
	for _, col := range columns {
		if col < 0 {
			return false
		}
	}
	return true
}

func columnsAreNone(columns []int) bool {
	for _, col := range columns {
		if col >= 0 {
			return false
		}
	}
	return true
}

func makeEmptyColumns(count int) []int {
	var idk = make([]int, count)
	for n := range idk {
		idk[n] = -1
	}
	return idk
}

func graphTrace(g *graph.Graph[*flownode.Node], onJoin OnJoin, path []PathElement) [][]PathElement {
	var (
		last    = path[len(path)-1]
		node    = last.Node
		columns = last.Columns
		parents = g.NeighborsDirected(node, graph.DirectionIncoming).Collect(nil)
	)

	if len(parents) == 0 {
		panic("path reached source node before stopping")
	}

	n := g.Value(node)
	if n.IsTable() {
		return [][]PathElement{path}
	}

	if !n.IsInternal() {
		path = append(path, PathElement{parents[0], columns})
		return graphTrace(g, onJoin, path)
	}

	if columnsAreNone(columns) {
		if n.IsInternal() && n.IsJoin() {
			idk := makeEmptyColumns(len(columns))
			parent := onJoin(node, idk, parents)
			if parent != graph.InvalidNode {
				path = append(path, PathElement{parent, idk})
				return graphTrace(g, onJoin, path)
			}
		}

		var paths [][]PathElement
		for _, p := range parents {
			path := slices.Clone(path)
			path = append(path, PathElement{p, makeEmptyColumns(len(columns))})
			paths = append(paths, graphTrace(g, onJoin, path)...)
		}
		return paths
	}

	// try to resolve the currently selected columns
	var resolved = make(map[graph.NodeIdx][]int)
	for coli, c := range columns {
		if c < 0 {
			continue
		}
		for _, o := range n.ParentColumns(c) {
			rr, ok := resolved[o.Node]
			if !ok {
				rr = makeEmptyColumns(len(columns))
				resolved[o.Node] = rr
			}
			rr[coli] = o.Column
		}
	}

	if len(resolved) == 0 {
		panic("Column resolved into no ancestors")
	}

	// are any of the columns generated?
	if columns, ok := resolved[node]; ok {
		delete(resolved, node)

		// some are, so at this point we know we'll need to yield None for those columns all the
		// way back to the root of the graph.
		if len(parents) != 1 {
			panic("unimplemented")
		}

		var paths [][]PathElement
		for _, p := range parents {
			cc, ok := resolved[p]
			if !ok {
				cc = makeEmptyColumns(len(columns))
			}

			path := slices.Clone(path)
			path = append(path, PathElement{p, cc})
			paths = append(paths, graphTrace(g, onJoin, path)...)
		}
		return paths
	}

	// no, it resolves to at least one parent column
	// if there is only one parent, we can step right to that
	if len(parents) == 1 {
		parent := parents[0]
		path = append(path, PathElement{parent, resolved[parent]})
		return graphTrace(g, onJoin, path)
	}

	// there are multiple parents.
	// this means we are either a union or a join.
	// let's deal with the union case first.
	// in unions, all keys resolve to more than one parent.
	if !n.IsJoin() {
		if len(parents) != len(resolved) {
			panic("all columns should come from all parents")
		}

		var paths [][]PathElement
		for parent, columns := range resolved {
			path := slices.Clone(path)
			path = append(path, PathElement{parent, columns})
			paths = append(paths, graphTrace(g, onJoin, path)...)
		}
		return paths
	}
	parent := onJoin(node, columns, parents)
	if parent == graph.InvalidNode {
		// our caller wants information about all our parents.
		// since the column we're chasing only follows a single path through a join (unless it
		// is a join key, which we don't yet handle), we need to produce (_, None) for all the
		// others.
		var paths [][]PathElement
		for _, parent := range parents {
			cc, ok := resolved[parent]
			if !ok {
				cc = makeEmptyColumns(len(columns))
			}
			path := slices.Clone(path)
			path = append(path, PathElement{parent, cc})
			paths = append(paths, graphTrace(g, onJoin, path)...)
		}
		return paths
	}
	// our caller only cares about *one* parent.
	// hopefully we can give key information about that parent
	cc, ok := resolved[parent]
	if !ok {
		cc = makeEmptyColumns(len(columns))
	}
	path = append(path, PathElement{parent, cc})
	return graphTrace(g, onJoin, path)
}
