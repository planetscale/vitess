package flownode

import (
	"fmt"
	"sort"
	"strings"

	"github.com/tidwall/btree"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
)

type unionEmitProject struct {
	emit map[dataflow.IndexPair][]int

	emitLeft btree.Map[dataflow.LocalNodeIdx, []int]
	cols     map[dataflow.IndexPair]int
	colsLeft btree.Map[dataflow.LocalNodeIdx, int]
}

func (u *unionEmitProject) OnInput(from dataflow.LocalNodeIdx, rs []sql.Record) (processing.Result, error) {
	var records = make([]sql.Record, 0, len(rs))
	mapping, ok := u.emitLeft.Get(from)
	if !ok {
		panic("missing mapping for EMIT")
	}

	for _, rec := range rs {
		mappedRow := make([]sql.Value, 0, len(mapping))
		values := rec.Row.ToValues()
		for _, col := range mapping {
			mappedRow = append(mappedRow, values[col])
		}
		records = append(records, sql.NewRecord(mappedRow, rec.Positive))
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

func (u *unionEmitProject) ColumnType(g *graph.Graph[*Node], col int) (sql.Type, error) {
	var tt = sql.Type{T: -1}
	for src, emit := range u.emit {
		t, err := g.Value(src.AsGlobal()).ColumnType(g, emit[col])
		if err != nil {
			return sql.Type{}, err
		}
		if tt.T >= 0 && tt.T != t.T {
			panic("wut")
		}
		tt = t
	}
	return tt, nil
}

func (u *unionEmitProject) OnCommit(remap map[graph.NodeIdx]dataflow.IndexPair) {
	var mappedEmit = make(map[dataflow.IndexPair][]int, len(u.emit))
	var mappedCols = make(map[dataflow.IndexPair]int, len(u.cols))

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

func (u *unionEmitProject) OnConnected(graph *graph.Graph[*Node]) error {
	for n := range u.emit {
		u.cols[n] = len(graph.Value(n.AsGlobal()).Fields())
	}
	return nil
}

func (u *unionEmitProject) ToProto() *flownodepb.Node_InternalUnion_EmitProject {
	pemit := &flownodepb.Node_InternalUnion_EmitProject{}
	for k, v := range u.emit {
		k := k
		pemit.Emit = append(pemit.Emit, &flownodepb.Node_InternalUnion_EmitProject_EmitTuple{
			Ip:      &k,
			Columns: v,
		})
	}
	u.emitLeft.Scan(func(k dataflow.LocalNodeIdx, v []int) bool {
		pemit.EmitLeft = append(pemit.EmitLeft, &flownodepb.Node_InternalUnion_EmitProject_EmitLeftTuple{
			Index:   k,
			Columns: v,
		})
		return true
	})
	for k, v := range u.cols {
		k := k
		pemit.Cols = append(pemit.Cols, &flownodepb.Node_InternalUnion_EmitProject_ColumnsTuple{
			Ip:     &k,
			Column: v,
		})
	}
	u.colsLeft.Scan(func(k dataflow.LocalNodeIdx, v int) bool {
		pemit.ColsLeft = append(pemit.ColsLeft, &flownodepb.Node_InternalUnion_EmitProject_ColumnsLeftTuple{
			Index:  k,
			Column: v,
		})
		return true
	})
	return pemit
}

func newUnionEmitProjectFromProto(pemit *flownodepb.Node_InternalUnion_EmitProject) *unionEmitProject {
	emit := &unionEmitProject{
		emit: make(map[dataflow.IndexPair][]int),
		cols: make(map[dataflow.IndexPair]int),
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
