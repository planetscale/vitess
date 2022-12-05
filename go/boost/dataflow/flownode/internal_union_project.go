package flownode

import (
	"fmt"
	"sort"
	"strings"

	"github.com/tidwall/btree"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/graph"
)

type unionEmitProject struct {
	emit map[boostpb.IndexPair][]int

	emitLeft btree.Map[boostpb.LocalNodeIndex, []int]
	cols     map[boostpb.IndexPair]int
	colsLeft btree.Map[boostpb.LocalNodeIndex, int]
}

func (u *unionEmitProject) OnInput(from boostpb.LocalNodeIndex, rs []boostpb.Record) (processing.Result, error) {
	var records = make([]boostpb.Record, 0, len(rs))
	mapping, ok := u.emitLeft.Get(from)
	if !ok {
		panic("missing mapping for EMIT")
	}

	for _, rec := range rs {
		mappedRow := make([]boostpb.Value, 0, len(mapping))
		values := rec.Row.ToValues()
		for _, col := range mapping {
			mappedRow = append(mappedRow, values[col])
		}
		records = append(records, boostpb.NewRecord(mappedRow, rec.Positive))
	}
	return processing.Result{
		Records: records,
	}, nil
}

func (u *unionEmitProject) Ancestors() []graph.NodeIdx {
	var keys = make([]graph.NodeIdx, 0, len(u.emit))
	for k := range u.emit {
		keys = append(keys, k.AsGlobal())
	}
	return keys
}

func (u *unionEmitProject) Resolve(col int) (resolve []NodeColumn) {
	for src, emit := range u.emit {
		resolve = append(resolve, NodeColumn{Node: src.AsGlobal(), Column: emit[col]})
	}
	return
}

func (u *unionEmitProject) Description() string {
	var ips = maps.Keys(u.emit)
	sort.Slice(ips, func(i, j int) bool {
		return ips[i].AsGlobal() < ips[j].AsGlobal()
	})

	var buf strings.Builder
	for i, ip := range ips {
		if i > 0 {
			buf.WriteString(" ⋃ ")
		}
		fmt.Fprintf(&buf, "%v:%v", ip, u.emit[ip])
	}
	return buf.String()
}

func (u *unionEmitProject) ParentColumns(col int) (pc []NodeColumn) {
	for src, emit := range u.emit {
		pc = append(pc, NodeColumn{Node: src.AsGlobal(), Column: emit[col]})
	}
	return
}

func (u *unionEmitProject) ColumnType(g *graph.Graph[*Node], col int) boostpb.Type {
	var tt = boostpb.Type{T: -1}
	for src, emit := range u.emit {
		t := g.Value(src.AsGlobal()).ColumnType(g, emit[col])
		if tt.T >= 0 && tt.T != t.T {
			panic("wut")
		}
		tt = t
	}
	return tt
}

func (u *unionEmitProject) OnCommit(remap map[graph.NodeIdx]boostpb.IndexPair) {
	var mappedEmit = make(map[boostpb.IndexPair][]int, len(u.emit))
	var mappedCols = make(map[boostpb.IndexPair]int, len(u.cols))

	for k, v := range u.emit {
		k.Remap(remap)
		u.emitLeft.Set(k.AsLocal(), v)
		mappedEmit[k] = v
	}

	for k, v := range u.cols {
		k.Remap(remap)
		u.colsLeft.Set(k.AsLocal(), v)
		mappedCols[k] = v
	}

	u.emit = mappedEmit
	u.cols = mappedCols
}

func (u *unionEmitProject) OnConnected(graph *graph.Graph[*Node]) {
	for n := range u.emit {
		u.cols[n] = len(graph.Value(n.AsGlobal()).Fields())
	}
}

func (u *unionEmitProject) ToProto() *boostpb.Node_InternalUnion_EmitProject {
	pemit := &boostpb.Node_InternalUnion_EmitProject{}
	for k, v := range u.emit {
		k := k
		pemit.Emit = append(pemit.Emit, &boostpb.Node_InternalUnion_EmitProject_EmitTuple{
			Ip:      &k,
			Columns: v,
		})
	}
	u.emitLeft.Scan(func(k boostpb.LocalNodeIndex, v []int) bool {
		pemit.EmitLeft = append(pemit.EmitLeft, &boostpb.Node_InternalUnion_EmitProject_EmitLeftTuple{
			Index:   k,
			Columns: v,
		})
		return true
	})
	for k, v := range u.cols {
		k := k
		pemit.Cols = append(pemit.Cols, &boostpb.Node_InternalUnion_EmitProject_ColumnsTuple{
			Ip:     &k,
			Column: v,
		})
	}
	u.colsLeft.Scan(func(k boostpb.LocalNodeIndex, v int) bool {
		pemit.ColsLeft = append(pemit.ColsLeft, &boostpb.Node_InternalUnion_EmitProject_ColumnsLeftTuple{
			Index:  k,
			Column: v,
		})
		return true
	})
	return pemit
}

func newUnionEmitProjectFromProto(pemit *boostpb.Node_InternalUnion_EmitProject) *unionEmitProject {
	emit := &unionEmitProject{
		emit: make(map[boostpb.IndexPair][]int),
		cols: make(map[boostpb.IndexPair]int),
	}
	for _, e := range pemit.Emit {
		emit.emit[*e.Ip] = e.Columns
	}
	for _, e := range pemit.EmitLeft {
		emit.emitLeft.Set(e.Index, e.Columns)
	}
	for _, e := range pemit.Cols {
		emit.cols[*e.Ip] = e.Column
	}
	for _, e := range pemit.ColsLeft {
		emit.colsLeft.Set(e.Index, e.Column)
	}
	return emit
}
