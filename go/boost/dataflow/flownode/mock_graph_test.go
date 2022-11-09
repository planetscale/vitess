package flownode

import (
	"context"
	"testing"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/sqltypes"
)

type MockGraph struct {
	t testing.TB

	graph  *graph.Graph[*Node]
	source graph.NodeIdx
	nut    boostpb.IndexPair
	states *state.Map
	nodes  *Map
	remap  map[graph.NodeIdx]boostpb.IndexPair
}

func NewMockGraph(t testing.TB) *MockGraph {
	gr := new(graph.Graph[*Node])
	source := gr.AddNode(New("source", []string{"dummy"}, &Source{}))

	return &MockGraph{
		t:      t,
		graph:  gr,
		source: source,
		nut:    boostpb.EmptyIndexPair(),
		states: new(state.Map),
		nodes:  new(Map),
		remap:  make(map[graph.NodeIdx]boostpb.IndexPair),
	}
}

func (mg *MockGraph) AddBase(name string, fields []string, schema []boostpb.Type, defaults []sqltypes.Value) boostpb.IndexPair {
	i := NewBase(nil, schema, defaults)
	global := mg.graph.AddNode(New(name, fields, i))
	mg.graph.AddEdge(mg.source, global)

	remap := make(map[graph.NodeIdx]boostpb.IndexPair)
	local := boostpb.LocalNodeIndex(len(mg.remap))

	ip := boostpb.NewIndexPair(global)
	ip.SetLocal(local)

	nn := mg.graph.Value(global)
	nn.SetFinalizedAddr(ip)
	nn.ResolveSchema(mg.graph)

	remap[global] = ip
	nn.OnCommit(remap)

	mg.states.Insert(local, state.NewMemoryState())
	mg.remap[global] = ip
	return ip
}

func (mg *MockGraph) SetOp(name string, fields []string, i Internal, materialized bool) {
	if !mg.nut.IsEmpty() {
		mg.t.Fatalf("only one node under test is supported")
	}

	i.OnConnected(mg.graph)
	parents := i.Ancestors()

	if len(parents) == 0 {
		mg.t.Fatalf("node under test should have ancestors")
	}

	global := mg.graph.AddNode(New(name, fields, i))
	local := boostpb.LocalNodeIndex(len(mg.remap))
	if materialized {
		mg.states.Insert(local, state.NewMemoryState())
	}
	for _, parent := range parents {
		mg.graph.AddEdge(parent, global)
	}

	ip := boostpb.NewIndexPair(global)
	ip.SetLocal(local)

	mg.remap[global] = ip
	nn := mg.graph.Value(global)
	nn.SetFinalizedAddr(ip)
	nn.ResolveSchema(mg.graph)
	nn.OnCommit(mg.remap)

	idx := nn.SuggestIndexes(global)
	for tbl, col := range idx {
		tbl := mg.graph.Value(tbl)
		if st := mg.states.Get(tbl.LocalAddr()); st != nil {
			st.AddKey(col, tbl.Schema(), nil)
		}
	}

	var unused []boostpb.LocalNodeIndex
	for _, ni := range mg.remap {
		ni := mg.graph.Value(ni.AsGlobal()).LocalAddr()
		if st := mg.states.Get(ni); st != nil && !st.IsUseful() {
			unused = append(unused, ni)
		}
	}
	for _, ni := range unused {
		mg.states.Remove(ni)
	}

	mg.graph.ForEachValue(func(node *Node) bool {
		if !node.IsSource() {
			node.AddTo(0)
		}
		return true
	})

	mg.nut = ip
	mg.nodes = new(Map)

	var topo = graph.NewTopoVisitor(mg.graph)
	for topo.Next(mg.graph) {
		if topo.Current == mg.source {
			continue
		}

		node := mg.graph.Value(topo.Current).Finalize(mg.graph)
		mg.nodes.Insert(node.LocalAddr(), node)
	}
}

func (mg *MockGraph) Node() *Node {
	return mg.nodes.Get(mg.nut.AsLocal())
}

type DummyExecutor struct {
}

func (d *DummyExecutor) Send(ctx context.Context, dest boostpb.DomainAddr, m *boostpb.Packet) error {
	return nil
}

func (mg *MockGraph) One(src boostpb.IndexPair, u []boostpb.Record, remember bool) []boostpb.Record {
	if mg.nut.IsEmpty() {
		panic("no node being tested right now")
	}

	var ex DummyExecutor
	id := mg.nut
	n := mg.nodes.Get(id.AsLocal())
	m, err := n.OnInput(&ex, src.AsLocal(), u, nil, mg.nodes, mg.states)
	if err != nil {
		mg.t.Fatal(err)
	}
	if len(m.Misses) != 0 {
		panic("miss during local integration test")
	}

	u = m.Records
	if !remember || !mg.states.ContainsKey(id.AsLocal()) {
		return u
	}

	Materialize(&u, boostpb.TagNone, mg.states.Get(id.AsLocal()))
	return u
}

func (mg *MockGraph) OneRow(src boostpb.IndexPair, r RecordLike, remember bool) []boostpb.Record {
	return mg.One(src, []boostpb.Record{r.AsRecord()}, remember)
}

func (mg *MockGraph) NarrowBaseID() boostpb.IndexPair {
	if len(mg.remap) != 2 {
		panic("expected remap = base + nut")
	}

	for _, ip := range mg.remap {
		if ip.AsGlobal() == mg.nut.AsGlobal() {
			continue
		}
		return ip
	}
	panic("failed to narrow")
}

func (mg *MockGraph) NarrowOne(u []boostpb.Record, remember bool) []boostpb.Record {
	src := mg.NarrowBaseID()
	return mg.One(src, u, remember)
}

type RecordLike interface {
	AsRecord() boostpb.Record
}

func (mg *MockGraph) NarrowOneRow(d RecordLike, remember bool) []boostpb.Record {
	return mg.NarrowOne([]boostpb.Record{d.AsRecord()}, remember)
}

func (mg *MockGraph) Seed(base boostpb.IndexPair, data RecordLike) {
	if mg.nut.IsEmpty() {
		mg.t.Fatal("seed must happen after set_op")
	}

	// base here is some identifier that was returned by Self::add_base.
	// which means it's a global address (and has to be so that it will correctly refer to
	// ancestors pre on_commit). we need to translate it into a local address.
	// since we set up the graph, we actually know that the NodeIndex is simply one greater
	// than the local index (since bases are added first, and assigned local + global
	// indices in order, but global ids are prefixed by the id of the source node).

	// no need to call on_input since base tables just forward anyway

	// if the base node has state, keep it
	if mg != nil {
		st := mg.states.Get(base.AsLocal())
		records := []boostpb.Record{data.AsRecord()}
		st.ProcessRecords(&records, boostpb.TagNone)
	} else {
		mg.t.Fatalf("unnecessary seed value for %v", base.AsGlobal())
	}
}

func (mg *MockGraph) Unseed(base boostpb.IndexPair) {
	if mg.nut.IsEmpty() {
		mg.t.Fatal("unseed must happen after set_op")
	}

	global := mg.nut.AsGlobal()
	idx := mg.graph.Value(global).SuggestIndexes(global)
	schema := mg.graph.Value(base.AsGlobal()).Schema()
	st := state.NewMemoryState()
	for tbl, cols := range idx {
		if tbl == base.AsGlobal() {
			st.AddKey(cols, schema, nil)
		}
	}

	mg.states.Insert(base.AsLocal(), st)
}
