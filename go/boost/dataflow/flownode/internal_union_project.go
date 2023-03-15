package flownode

import (
	"fmt"
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
)

type EmitTuple = flownodepb.Node_InternalUnion_EmitProject_EmitTuple

type unionEmitProject struct {
	emit []EmitTuple
}

func (u *unionEmitProject) replay(union *Union, from dataflow.LocalNodeIdx, keyCols []int, rk unionreplayKey) {
	for _, emit := range u.emit {
		var extend []int
		for _, c := range keyCols {
			extend = append(extend, emit.Columns[c])
		}

		if emit.Ip.Local == from {
			union.replayKey[rk] = append(union.replayKey[rk], extend...)
		} else {
			union.replayKey[unionreplayKey{rk.tag, emit.Ip.Local}] = extend
		}
	}
}

func (u *unionEmitProject) OnInput(from dataflow.LocalNodeIdx, rs []sql.Record) (processing.Result, error) {
	records := make([]sql.Record, 0, len(rs))

	for _, emit := range u.emit {
		if emit.Ip.Local != from {
			continue
		}

		for _, rec := range rs {
			mappedRow := make([]sql.Value, 0, len(emit.Columns))
			values := rec.Row.ToValues()
			for _, col := range emit.Columns {
				mappedRow = append(mappedRow, values[col])
			}
			records = append(records, sql.NewRecord(mappedRow, rec.Positive))
		}

		return processing.Result{
			Records: records,
		}, nil
	}

	panic("did not find EMIT for source column")
}

func (u *unionEmitProject) Ancestors() []graph.NodeIdx {
	keys := make([]graph.NodeIdx, 0, len(u.emit))
	for _, emit := range u.emit {
		idx := emit.Ip.AsGlobal()
		if !slices.Contains(keys, idx) {
			keys = append(keys, idx)
		}
	}
	return keys
}

func (u *unionEmitProject) Resolve(col int) (resolve []NodeColumn) {
	for _, e := range u.emit {
		idx := e.Ip.AsGlobal()
		if !slices.ContainsFunc(resolve, func(nc NodeColumn) bool { return nc.Node == idx }) {
			resolve = append(resolve, NodeColumn{Node: idx, Column: e.Columns[col]})
		}
	}
	return
}

func (u *unionEmitProject) Description() string {
	var buf strings.Builder
	for i, emit := range u.emit {
		if i > 0 {
			buf.WriteString(" ⋃ ")
		}
		fmt.Fprintf(&buf, "%v:%v", emit.Ip, emit.Columns)
	}
	return buf.String()
}

func (u *unionEmitProject) ParentColumns(col int) (pc []NodeColumn) {
	for _, emit := range u.emit {
		idx := emit.Ip.AsGlobal()
		if !slices.ContainsFunc(pc, func(nc NodeColumn) bool { return nc.Node == idx }) {
			pc = append(pc, NodeColumn{Node: idx, Column: emit.Columns[col]})
		}
	}
	return
}

func (u *unionEmitProject) ColumnType(g *graph.Graph[*Node], col int) (sql.Type, error) {
	var tt = sql.Type{T: -1}
	for _, emit := range u.emit {
		t, err := g.Value(emit.Ip.AsGlobal()).ColumnType(g, emit.Columns[col])
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
	for i, emit := range u.emit {
		emit.Ip.Remap(remap)
		u.emit[i] = emit
	}
}

func (u *unionEmitProject) OnConnected(graph *graph.Graph[*Node]) error {
	return nil
}

func (u *unionEmitProject) ToProto() *flownodepb.Node_InternalUnion_EmitProject {
	return &flownodepb.Node_InternalUnion_EmitProject{
		Emit: u.emit,
	}
}

func newUnionEmitProjectFromProto(pemit *flownodepb.Node_InternalUnion_EmitProject) *unionEmitProject {
	return &unionEmitProject{
		emit: pemit.Emit,
	}
}
