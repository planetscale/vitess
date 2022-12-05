package flownode

import (
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
)

type Stats struct {
	Processed uint64
}

type Node struct {
	Name   string
	index  boostpb.IndexPair
	domain boostpb.DomainIndex

	fields   []string
	schema   []boostpb.Type
	parents  []boostpb.LocalNodeIndex
	children []boostpb.LocalNodeIndex

	impl      NodeImpl
	taken     bool
	shardedBy boostpb.Sharding

	Stats Stats
}

type NodeImpl interface {
	dataflow()
}

type Internal interface {
	internal()
	NodeImpl
	Ingredient
}

func New(name string, fields []string, inner NodeImpl) *Node {
	return &Node{
		Name:      name,
		domain:    boostpb.InvalidDomainIndex,
		index:     boostpb.EmptyIndexPair(),
		fields:    fields,
		impl:      inner,
		shardedBy: boostpb.NewShardingNone(),
	}
}

func (n *Node) Fields() []string {
	return n.fields
}

func (n *Node) Index() boostpb.IndexPair {
	return n.index
}

func (n *Node) Mirror(inner NodeImpl) *Node {
	return New(n.Name, slices.Clone(n.fields), inner)
}

func (n *Node) NamedMirror(inner NodeImpl, name string) *Node {
	return New(name, slices.Clone(n.fields), inner)
}

func (n *Node) OnConnected(graph *graph.Graph[*Node]) {
	n.impl.(Internal).OnConnected(graph)
}

func (n *Node) OnCommit(remap map[graph.NodeIdx]boostpb.IndexPair) {
	if op, ok := n.impl.(Internal); ok {
		op.OnCommit(n.index.AsGlobal(), remap)
	}
}

func (n *Node) OnInput(ex processing.Executor, from boostpb.LocalNodeIndex, data []boostpb.Record, repl replay.Context, domain *Map, states *state.Map) (processing.Result, error) {
	return n.impl.(Internal).OnInput(n, ex, from, data, repl, domain, states)
}

func (n *Node) Ancestors() []graph.NodeIdx {
	return n.impl.(Internal).Ancestors()
}

func (n *Node) SetFinalizedAddr(addr boostpb.IndexPair) {
	n.index = addr
}

func (n *Node) Resolve(i int) []NodeColumn {
	return n.impl.(Internal).Resolve(i)
}

func (n *Node) ParentColumns(col int) []NodeColumn {
	return n.impl.(Internal).ParentColumns(col)
}

func (n *Node) Parents() []boostpb.LocalNodeIndex {
	return n.parents
}

func (n *Node) MustReplayAmong() map[graph.NodeIdx]struct{} {
	if j, ok := n.impl.(ingredientJoin); ok {
		return j.MustReplayAmong()
	}
	return nil
}

func (n *Node) SuggestIndexes(idx graph.NodeIdx) map[graph.NodeIdx][]int {
	switch impl := n.impl.(type) {
	case Internal:
		return impl.SuggestIndexes(idx)
	case *Base:
		return impl.SuggestIndexes(idx)
	default:
		return nil
	}
}

func (n *Node) Domain() boostpb.DomainIndex {
	return n.domain
}

func (n *Node) Sharding() boostpb.Sharding {
	return n.shardedBy
}

func (n *Node) ShardBy(sharding boostpb.Sharding) {
	n.shardedBy = sharding
}

func (n *Node) LocalAddr() boostpb.LocalNodeIndex {
	return n.index.AsLocal()
}

func (n *Node) GlobalAddr() graph.NodeIdx {
	return n.index.AsGlobal()
}

func (n *Node) Children() []boostpb.LocalNodeIndex {
	return n.children
}

func (n *Node) AddTo(dm boostpb.DomainIndex) {
	if n.domain != boostpb.InvalidDomainIndex {
		panic("re-assign domain")
	}
	n.domain = dm
}

func (n *Node) Finalize(g *graph.Graph[*Node]) *Node {
	if n.taken {
		panic("Take() on a taken node")
	}
	n.taken = true
	n.ResolveSchema(g)

	var final = &Node{
		Name:      n.Name,
		index:     n.index,
		domain:    n.domain,
		fields:    n.fields,
		schema:    n.schema,
		parents:   nil, // intentionally left blank
		children:  nil, // intentionally left blank
		impl:      n.impl,
		taken:     false,
		shardedBy: n.shardedBy,
	}
	var ni = final.index.AsGlobal()
	var dm = final.domain
	var iter = g.NeighborsDirected(ni, graph.DirectionOutgoing)

	for iter.Next() {
		node := g.Value(iter.Current)
		if node.domain == dm {
			final.children = append(final.children, node.LocalAddr())
		}
	}

	iter = g.NeighborsDirected(ni, graph.DirectionIncoming)
	for iter.Next() {
		node := g.Value(iter.Current)
		if !node.IsSource() && node.domain == dm {
			final.parents = append(final.parents, node.LocalAddr())
		}
	}

	return final
}

func (n *Node) Schema() []boostpb.Type {
	if n.schema == nil {
		panic("missing schema from node")
	}
	return n.schema
}

func (n *Node) ResolveSchema(g *graph.Graph[*Node]) {
	if n.schema != nil {
		return
	}

	switch impl := n.impl.(type) {
	case AnyBase:
		n.schema = impl.Schema()
	case Internal:
		for col := 0; col < len(n.fields); col++ {
			n.schema = append(n.schema, impl.ColumnType(g, col))
		}
	case *Reader:
		target := g.Value(impl.IsFor())
		for col := 0; col < len(n.fields); col++ {
			n.schema = append(n.schema, target.ColumnType(g, col))
		}
	case *Ingress, *Sharder, *Egress:
		// TODO@vmg: this may not be correct for all cases
		//		- what if ingress has >1 parent? can that happen?
		//		- sharder always has >1 parent, but they should all match on their column types
		//		- no idea about egress
		parent := g.Value(g.NeighborsDirected(n.index.AsGlobal(), graph.DirectionIncoming).First())
		for col := 0; col < len(n.fields); col++ {
			n.schema = append(n.schema, parent.ColumnType(g, col))
		}
	case *Source:
		n.schema = []boostpb.Type{}
	default:
		panic(fmt.Sprintf("TODO: columnType on %T", impl))
	}
}

func (n *Node) ColumnType(g *graph.Graph[*Node], col int) boostpb.Type {
	n.ResolveSchema(g)
	return n.schema[col]
}

func (n *Node) Remove() {
	n.impl = &Dropped{}
}

func (n *Node) AddChild(child boostpb.LocalNodeIndex) {
	n.children = append(n.children, child)
}

func (n *Node) TryRemoveChild(child boostpb.LocalNodeIndex) bool {
	idx := slices.Index(n.children, child)
	if idx < 0 {
		return false
	}
	n.children = slices.Delete(n.children, idx, idx+1)
	return true
}

type RowSlice []boostpb.Row

func (rows RowSlice) ForEach(f func(row boostpb.Row)) {
	for _, row := range rows {
		f(row)
	}
}

func (rows RowSlice) Len() int {
	return len(rows)
}

var _ RowIterator = (RowSlice)(nil)
var _ RowIterator = (*offheap.Rows)(nil)

func nodeLookup(parent boostpb.LocalNodeIndex, columns []int, key boostpb.Row, nodes *Map, states *state.Map) (RowIterator, bool, MaterializedState) {
	if st := states.Get(parent); st != nil {
		rowBag, found := st.Lookup(columns, key)
		return rowBag, found, IsMaterialized
	}
	if internal := nodes.Get(parent).asQueryThrough(); internal != nil {
		return internal.QueryThrough(columns, key, nodes, states)
	}
	return nil, false, NotMaterialized
}
