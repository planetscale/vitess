package flownode

import (
	"context"
	"testing"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
)

type MockGraph struct {
	t testing.TB

	graph  *graph.Graph[*Node]
	source graph.NodeIdx
	nut    dataflow.IndexPair
	states *state.Map
	nodes  *Map
	remap  map[graph.NodeIdx]dataflow.IndexPair
}

func NewMockGraph(t testing.TB) *MockGraph {
	gr := new(graph.Graph[*Node])
	source := gr.AddNode(New("root", []string{"dummy"}, &Root{}))

	return &MockGraph{
		t:      t,
		graph:  gr,
		source: source,
		nut:    dataflow.EmptyIndexPair(),
		states: new(state.Map),
		nodes:  new(Map),
		remap:  make(map[graph.NodeIdx]dataflow.IndexPair),
	}
}

type mockBase struct {
	schema []sql.Type
}

func (m *mockBase) Schema() []sql.Type {
	return m.schema
}

func (m *mockBase) dataflow() {}

func (mg *MockGraph) AddBase(name string, fields []string, schema []sql.Type) dataflow.IndexPair {
	global := mg.graph.AddNode(New(name, fields, &mockBase{schema: schema}))
	mg.graph.AddEdge(mg.source, global)

	remap := make(map[graph.NodeIdx]dataflow.IndexPair)
	local := dataflow.LocalNodeIdx(len(mg.remap))

	ip := dataflow.NewIndexPair(global)
	ip.SetLocal(local)

	nn := mg.graph.Value(global)
	nn.SetFinalizedAddr(ip)
	_, err := nn.ResolveSchema(mg.graph)
	if err != nil {
		mg.t.Fatalf("failed to resolve schema: %v", err)
	}

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

	err := i.OnConnected(mg.graph)
	if err != nil {
		mg.t.Fatalf("failed to connect graph: %v", err)
	}
	parents := i.Ancestors()

	if len(parents) == 0 {
		mg.t.Fatalf("node under test should have ancestors")
	}

	global := mg.graph.AddNode(New(name, fields, i))
	local := dataflow.LocalNodeIdx(len(mg.remap))
	if materialized {
		mg.states.Insert(local, state.NewMemoryState())
	}
	for _, parent := range parents {
		mg.graph.AddEdge(parent, global)
	}

	ip := dataflow.NewIndexPair(global)
	ip.SetLocal(local)

	mg.remap[global] = ip
	nn := mg.graph.Value(global)
	nn.SetFinalizedAddr(ip)
	_, err = nn.ResolveSchema(mg.graph)
	if err != nil {
		mg.t.Fatalf("failed to resolve schema: %v", err)
	}
	nn.OnCommit(mg.remap)

	idx := nn.SuggestIndexes(global)
	for tbl, col := range idx {
		tbl := mg.graph.Value(tbl)
		if st := mg.states.Get(tbl.LocalAddr()); st != nil {
			st.AddKey(col, tbl.Schema(), nil, false)
		}
	}

	var unused []dataflow.LocalNodeIdx
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
		if !node.IsRoot() {
			node.AddTo(0)
		}
		return true
	})

	mg.nut = ip
	mg.nodes = new(Map)

	var topo = graph.NewTopoVisitor(mg.graph)
	for topo.Next() {
		if topo.Current == mg.source {
			continue
		}

		node, err := mg.graph.Value(topo.Current).Finalize(mg.graph)
		if err != nil {
			mg.t.Fatalf("failed to finalize node: %v", err)
		}
		mg.nodes.Insert(node.LocalAddr(), node)
		if err := node.OnDeploy(); err != nil {
			mg.t.Fatalf("failed to Deploy node: %v", err)
		}
	}
}

func (mg *MockGraph) Node() *Node {
	return mg.nodes.Get(mg.nut.AsLocal())
}

type DummyExecutor struct {
}

func (d *DummyExecutor) Send(ctx context.Context, dest dataflow.DomainAddr, m packet.FlowPacket) error {
	return nil
}

func (mg *MockGraph) One(src dataflow.IndexPair, u []sql.Record, remember bool) []sql.Record {
	if mg.nut.IsEmpty() {
		mg.t.Fatal("no node being tested right now")
	}

	var ex DummyExecutor
	id := mg.nut
	n := mg.nodes.Get(id.AsLocal())
	m, err := n.OnInput(&ex, src.AsLocal(), u, replay.Context{}, mg.nodes, mg.states)
	if err != nil {
		mg.t.Fatal(err)
	}
	if len(m.Misses) != 0 {
		mg.t.Fatal("miss during local integration test")
	}

	u = m.Records
	if !remember || !mg.states.ContainsKey(id.AsLocal()) {
		return u
	}

	st := mg.states.Get(id.AsLocal())
	return st.ProcessRecords(u, dataflow.TagNone, nil)
}

func (mg *MockGraph) OneRow(src dataflow.IndexPair, r RecordLike, remember bool) []sql.Record {
	return mg.One(src, []sql.Record{r.AsRecord()}, remember)
}

func (mg *MockGraph) NarrowBaseID() dataflow.IndexPair {
	if len(mg.remap) != 2 {
		mg.t.Fatal("expected remap = base + nut")
	}

	for _, ip := range mg.remap {
		if ip.AsGlobal() == mg.nut.AsGlobal() {
			continue
		}
		return ip
	}
	mg.t.Fatal("failed to narrow")
	return dataflow.IndexPair{}
}

func (mg *MockGraph) NarrowOne(u []sql.Record, remember bool) []sql.Record {
	src := mg.NarrowBaseID()
	return mg.One(src, u, remember)
}

type RecordLike interface {
	AsRecord() sql.Record
}

func (mg *MockGraph) NarrowOneRow(d RecordLike, remember bool) []sql.Record {
	return mg.NarrowOne([]sql.Record{d.AsRecord()}, remember)
}

func (mg *MockGraph) Seed(base dataflow.IndexPair, data RecordLike) {
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
		records := []sql.Record{data.AsRecord()}
		st.ProcessRecords(records, dataflow.TagNone, nil)
	} else {
		mg.t.Fatalf("unnecessary seed value for %v", base.AsGlobal())
	}
}

func (mg *MockGraph) Unseed(base dataflow.IndexPair) {
	if mg.nut.IsEmpty() {
		mg.t.Fatal("unseed must happen after set_op")
	}

	global := mg.nut.AsGlobal()
	idx := mg.graph.Value(global).SuggestIndexes(global)
	schema := mg.graph.Value(base.AsGlobal()).Schema()
	st := state.NewMemoryState()
	for tbl, cols := range idx {
		if tbl == base.AsGlobal() {
			st.AddKey(cols, schema, nil, false)
		}
	}

	mg.states.Insert(base.AsLocal(), st)
}