package flownode

import (
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
)

type Stats struct {
	Processed uint64
}

type Node struct {
	name   string
	index  dataflow.IndexPair
	domain dataflow.DomainIdx

	fields   []string
	schema   []sql.Type
	parents  []dataflow.LocalNodeIdx
	children []dataflow.LocalNodeIdx

	impl      NodeImpl
	taken     bool
	shardedBy dataflow.Sharding

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
		name:      name,
		domain:    dataflow.InvalidDomainIdx,
		index:     dataflow.EmptyIndexPair(),
		fields:    fields,
		impl:      inner,
		shardedBy: dataflow.NewShardingNone(),
	}
}

func (n *Node) Fields() []string {
	return n.fields
}

func (n *Node) Index() dataflow.IndexPair {
	return n.index
}

func (n *Node) Mirror(inner NodeImpl) *Node {
	return New(n.name, slices.Clone(n.fields), inner)
}

func (n *Node) NamedMirror(inner NodeImpl, name string) *Node {
	return New(name, slices.Clone(n.fields), inner)
}

func (n *Node) OnConnected(graph *graph.Graph[*Node]) error {
	return n.impl.(Internal).OnConnected(graph)
}

func (n *Node) OnCommit(remap map[graph.NodeIdx]dataflow.IndexPair) {
	if op, ok := n.impl.(Internal); ok {
		op.OnCommit(n.index.AsGlobal(), remap)
	}
}

func (n *Node) OnInput(ex processing.Executor, from dataflow.LocalNodeIdx, data []sql.Record, repl replay.Context, domain *Map, states *state.Map) (processing.Result, error) {
	return n.impl.(Internal).OnInput(n, ex, from, data, repl, domain, states)
}

func (n *Node) Ancestors() []graph.NodeIdx {
	return n.impl.(Internal).Ancestors()
}

func (n *Node) SetFinalizedAddr(addr dataflow.IndexPair) {
	n.index = addr
}

func (n *Node) Resolve(i int) []NodeColumn {
	return n.impl.(Internal).Resolve(i)
}

func (n *Node) ParentColumns(col int) []NodeColumn {
	return n.impl.(Internal).ParentColumns(col)
}

func (n *Node) Parents() []dataflow.LocalNodeIdx {
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
	default:
		return nil
	}
}

func (n *Node) Domain() dataflow.DomainIdx {
	return n.domain
}

func (n *Node) Sharding() dataflow.Sharding {
	return n.shardedBy
}

func (n *Node) ShardBy(sharding dataflow.Sharding) {
	n.shardedBy = sharding
}

func (n *Node) LocalAddr() dataflow.LocalNodeIdx {
	return n.index.AsLocal()
}

func (n *Node) GlobalAddr() graph.NodeIdx {
	return n.index.AsGlobal()
}

func (n *Node) Children() []dataflow.LocalNodeIdx {
	return n.children
}

func (n *Node) AddTo(dm dataflow.DomainIdx) {
	if n.domain != dataflow.InvalidDomainIdx {
		panic("re-assign domain")
	}
	n.domain = dm
}

func (n *Node) Finalize(g *graph.Graph[*Node]) (*Node, error) {
	if n.taken {
		panic("Take() on a taken node")
	}
	n.taken = true

	_, err := n.ResolveSchema(g)
	if err != nil {
		return nil, err
	}

	var final = &Node{
		name:      n.name,
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
		if !node.IsRoot() && node.domain == dm {
			final.parents = append(final.parents, node.LocalAddr())
		}
	}

	return final, nil
}

func (n *Node) Schema() []sql.Type {
	if n.schema == nil {
		panic("missing schema from node")
	}
	return n.schema
}

type schemaful interface {
	Schema() []sql.Type
}

func (n *Node) ResolveSchema(g *graph.Graph[*Node]) ([]sql.Type, error) {
	if n.schema != nil {
		return n.schema, nil
	}

	switch impl := n.impl.(type) {
	case schemaful:
		n.schema = impl.Schema()
	case Internal:
		for col := 0; col < len(n.fields); col++ {
			t, err := impl.ColumnType(g, col)
			if err != nil {
				return nil, err
			}
			n.schema = append(n.schema, t)
		}
	case *Reader:
		target := g.Value(impl.IsFor())
		for col := 0; col < len(n.fields); col++ {
			ct, err := target.ColumnType(g, col)
			if err != nil {
				return nil, err
			}
			n.schema = append(n.schema, ct)
		}
	case *Ingress, *Sharder, *Egress:
		// TODO@vmg: this may not be correct for all cases
		//		- what if ingress has >1 parent? can that happen?
		//		- sharder always has >1 parent, but they should all match on their column types
		//		- no idea about egress
		parent := g.Value(g.NeighborsDirected(n.index.AsGlobal(), graph.DirectionIncoming).First())
		for col := 0; col < len(n.fields); col++ {
			ct, err := parent.ColumnType(g, col)
			if err != nil {
				return nil, err
			}
			n.schema = append(n.schema, ct)
		}
	case *Root:
		n.schema = []sql.Type{}
	default:
		panic(fmt.Sprintf("TODO: columnType on %T", impl))
	}
	return n.schema, nil
}

func (n *Node) ColumnType(g *graph.Graph[*Node], col int) (sql.Type, error) {
	s, err := n.ResolveSchema(g)
	if err != nil {
		return sql.Type{}, err
	}
	return s[col], nil
}

func (n *Node) Remove() {
	n.impl = &Dropped{}
}

func (n *Node) AddChild(child dataflow.LocalNodeIdx) {
	n.children = append(n.children, child)
}

func (n *Node) TryRemoveChild(child dataflow.LocalNodeIdx) bool {
	idx := slices.Index(n.children, child)
	if idx < 0 {
		return false
	}
	n.children = slices.Delete(n.children, idx, idx+1)
	return true
}

func (n *Node) OnDeploy() error {
	if d, ok := n.impl.(interface{ OnDeploy() error }); ok {
		return d.OnDeploy()
	}
	return nil
}

type RowSlice []sql.Row

func (rows RowSlice) ForEach(f func(row sql.Row)) {
	for _, row := range rows {
		f(row)
	}
}

func (rows RowSlice) Len() int {
	return len(rows)
}

var _ RowIterator = (RowSlice)(nil)
var _ RowIterator = (*offheap.Rows)(nil)

func nodeLookup(parent dataflow.LocalNodeIdx, columns []int, key sql.Row, nodes *Map, states *state.Map) (RowIterator, bool, MaterializedState) {
	if st := states.Get(parent); st != nil {
		rowBag, found := st.Lookup(columns, key)
		return rowBag, found, IsMaterialized
	}
	if internal := nodes.Get(parent).asQueryThrough(); internal != nil {
		return internal.QueryThrough(columns, key, nodes, states)
	}
	return nil, false, NotMaterialized
}
