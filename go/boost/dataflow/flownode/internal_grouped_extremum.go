package flownode

import (
	"fmt"
	"strconv"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type numeric interface {
	int64 | uint64 | float64
}

type integerDelta[N numeric] struct {
	v        N
	positive bool
}

type aggExtremumInteger[N numeric] struct {
	from func(boostpb.Value) (N, error)
	to   func(N) boostpb.Value

	diffs []integerDelta[N]
	kind  AggregationKind
	over  int
}

func (g *aggExtremumInteger[N]) len() int {
	return len(g.diffs)
}

func (g *aggExtremumInteger[N]) reset() {
	g.diffs = g.diffs[:0]
}

func (g *aggExtremumInteger[N]) update(r boostpb.Record) {
	v, err := g.from(r.Row.ValueAt(g.over))
	if err != nil {
		panic(err)
	}
	g.diffs = append(g.diffs, integerDelta[N]{v: v, positive: r.Positive})
}

func (g *aggExtremumInteger[N]) apply(current *boostpb.Value) boostpb.Value {
	var extremes []N
	var isExtreme func(v N) bool

	if current != nil && current.Type() != sqltypes.Null {
		n, _ := g.from(*current)
		extremes = append(extremes, n)

		switch g.kind {
		case ExtremumMax:
			isExtreme = func(x N) bool { return x >= n }
		case ExtremumMin:
			isExtreme = func(x N) bool { return x <= n }
		}
	} else {
		isExtreme = func(_ N) bool { return true }
	}

	for _, d := range g.diffs {
		if isExtreme(d.v) {
			if d.positive {
				extremes = append(extremes, d.v)
			} else {
				if idx := slices.Index(extremes, d.v); idx >= 0 {
					extremes[idx] = extremes[len(extremes)-1]
					extremes = extremes[:len(extremes)-1]
				}
			}
		}
	}

	if len(extremes) == 0 {
		// TODO: handle this case by querying into the parent.
		panic("unimplemented")
	}

	var extreme = extremes[0]

	switch g.kind {
	case ExtremumMax:
		for _, v := range extremes[1:] {
			if v > extreme {
				extreme = v
			}
		}
	case ExtremumMin:
		for _, v := range extremes[1:] {
			if v < extreme {
				extreme = v
			}
		}
	}
	return g.to(extreme)
}

type groupedExtremum struct {
	kind AggregationKind
	over int
}

func (e *groupedExtremum) state(tt boostpb.Type) aggregationState {
	switch {
	case sqltypes.IsSigned(tt.T):
		return &aggExtremumInteger[int64]{
			from: func(value boostpb.Value) (int64, error) {
				return strconv.ParseInt(value.RawStr(), 10, 64)
			},
			to: func(n int64) boostpb.Value {
				return boostpb.MakeValue(tt.T, func(buf []byte) []byte {
					return strconv.AppendInt(buf, n, 10)
				})
			},
			kind: e.kind,
			over: e.over,
		}
	case sqltypes.IsUnsigned(tt.T):
		return &aggExtremumInteger[uint64]{
			from: func(value boostpb.Value) (uint64, error) {
				return strconv.ParseUint(value.RawStr(), 10, 64)
			},
			to: func(n uint64) boostpb.Value {
				return boostpb.MakeValue(tt.T, func(buf []byte) []byte {
					return strconv.AppendUint(buf, n, 10)
				})
			},
			kind: e.kind,
			over: e.over,
		}
	case sqltypes.IsFloat(tt.T):
		return &aggExtremumInteger[float64]{
			from: func(value boostpb.Value) (float64, error) {
				return strconv.ParseFloat(value.RawStr(), 64)
			},
			to: func(f float64) boostpb.Value {
				return boostpb.MakeValue(tt.T, func(buf []byte) []byte {
					return evalengine.AppendFloat(buf, tt.T, f)
				})
			},
			kind: e.kind,
			over: e.over,
		}
	default:
		panic("unsupported MAX/MIN type")
	}
}

func (e *groupedExtremum) defaultValue() sqltypes.Value {
	return sqltypes.NULL
}

func (e *groupedExtremum) setup(parent *Node) {
	if e.over >= len(parent.Fields()) {
		panic("cannot aggregate over non-existing column")
	}
}

func (e *groupedExtremum) description() string {
	switch e.kind {
	case ExtremumMin:
		return fmt.Sprintf("MIN(:%d)", e.over)
	case ExtremumMax:
		return fmt.Sprintf("MAX(:%d)", e.over)
	}
	panic("unreachable")
}

var _ AggrExpr = (*groupedExtremum)(nil)

type AggregationKind = boostpb.Node_InternalGrouped_Aggregation_Kind

const (
	ExtremumMin          = boostpb.Node_InternalGrouped_Aggregation_Min
	ExtremumMax          = boostpb.Node_InternalGrouped_Aggregation_Max
	AggregationCount     = boostpb.Node_InternalGrouped_Aggregation_Count
	AggregationCountStar = boostpb.Node_InternalGrouped_Aggregation_CountStar
	AggregationSum       = boostpb.Node_InternalGrouped_Aggregation_Sum

	AggregationInvalid AggregationKind = -1
)

func AggregationOver(kind AggregationKind, over int) AggrExpr {
	switch kind {
	case ExtremumMin, ExtremumMax:
		return &groupedExtremum{
			kind: kind,
			over: over,
		}
	case AggregationSum, AggregationCount, AggregationCountStar:
		return &groupedAggregator{
			kind: kind,
			over: over,
		}
	}
	panic("should not happen")
}
