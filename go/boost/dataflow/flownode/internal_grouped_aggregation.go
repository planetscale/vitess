package flownode

import (
	"fmt"
	"strconv"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type groupInt64 struct {
	diffs []int64
	kind  AggregationKind
	over  int
}

func (g *groupInt64) len() int {
	return len(g.diffs)
}

func (g *groupInt64) reset() {
	g.diffs = g.diffs[:0]
}

func (g *groupInt64) update(r boostpb.Record) {
	switch g.kind {
	case AggregationCountStar:
		if r.Positive {
			g.diffs = append(g.diffs, 1)
		} else {
			g.diffs = append(g.diffs, -1)
		}
	case AggregationCount:
		valType := r.Row.ValueAt(g.over).Type()
		if valType == sqltypes.Null {
			g.diffs = append(g.diffs, 0)
			return
		}
		if r.Positive {
			g.diffs = append(g.diffs, 1)
		} else {
			g.diffs = append(g.diffs, -1)
		}
	case AggregationSum:
		val := r.Row.ValueAt(g.over)
		if val.Type() == sqltypes.Null {
			g.diffs = append(g.diffs, 0)
			return
		}
		i, err := val.ToVitessUnsafe().ToInt64()
		if err != nil {
			panic(err)
		}
		if !r.Positive {
			i = -i
		}
		g.diffs = append(g.diffs, i)
	}
}

func (g *groupInt64) apply(current *boostpb.Value) boostpb.Value {
	var n int64
	if current != nil && current.Type() != sqltypes.Null {
		var err error
		n, err = strconv.ParseInt(current.RawStr(), 10, 64)
		if err != nil {
			panic(err)
		}
	}
	for _, d := range g.diffs {
		n += d
	}
	var tt sqltypes.Type
	switch g.kind {
	case AggregationCount, AggregationCountStar:
		tt = sqltypes.Int64
	case AggregationSum:
		tt = sqltypes.Decimal
	}
	return boostpb.MakeValue(tt, func(buf []byte) []byte {
		return strconv.AppendInt(buf, n, 10)
	})
}

type groupFloat struct {
	diffs []float64
	over  int
}

func (g *groupFloat) len() int {
	return len(g.diffs)
}

func (g *groupFloat) reset() {
	g.diffs = g.diffs[:0]
}

func (g *groupFloat) update(r boostpb.Record) {
	f, err := r.Row.ValueAt(g.over).ToVitessUnsafe().ToFloat64()
	if err != nil {
		panic(err)
	}
	if !r.Positive {
		f = -f
	}
	g.diffs = append(g.diffs, f)
}

func (g *groupFloat) apply(current *boostpb.Value) boostpb.Value {
	var n float64
	if current != nil && current.Type() != sqltypes.Null {
		n, _ = current.ToVitessUnsafe().ToFloat64()
	}
	for _, d := range g.diffs {
		n += d
	}
	return boostpb.MakeValue(sqltypes.Float64, func(buf []byte) []byte {
		return evalengine.AppendFloat(buf, sqltypes.Float64, n)
	})
}

type groupedAggregator struct {
	kind AggregationKind
	over int
}

func (a *groupedAggregator) setup(parent *Node) {
	if a.over >= len(parent.Fields()) {
		panic("cannot aggregate over non-existing column")
	}
}

func (a *groupedAggregator) state(tt boostpb.Type) aggregationState {
	switch tt.T {
	case sqltypes.Int64:
		if a.kind == AggregationSum {
			panic("SUM() aggregated as INT64 (should be DECIMAL)")
		}
		fallthrough
	case sqltypes.Decimal:
		return &groupInt64{kind: a.kind, over: a.over}
	case sqltypes.Float64:
		if a.kind == AggregationCount {
			panic("COUNT() aggregated as Float64 (should be INT64)")
		}
		return &groupFloat{over: a.over}
	default:
		panic(fmt.Errorf("unexpected aggregation %v", tt.T))
	}
}

// mysql> select count(*), count(id), sum(id), avg(id), min(id), max(id) from users where id = <id>;
//+----------+-----------+---------+---------+---------+---------+
//| count(*) | count(id) | sum(id) | avg(id) | min(id) | max(id) |
//+----------+-----------+---------+---------+---------+---------+
//|        0 |         0 |    NULL |    NULL |    NULL |    NULL |
//+----------+-----------+---------+---------+---------+---------+
//1 row in set (0.01 sec)

var defaultValueZero = sqltypes.NewInt64(0)

func (a *groupedAggregator) defaultValue() sqltypes.Value {
	switch a.kind {
	case AggregationCount, AggregationCountStar:
		return defaultValueZero
	default:
		return sqltypes.NULL
	}
}

func (a *groupedAggregator) description() string {
	switch a.kind {
	case AggregationCount:
		return fmt.Sprintf("COUNT(%d)", a.over)
	case AggregationCountStar:
		return "COUNT(*)"
	case AggregationSum:
		return fmt.Sprintf("SUM(%d)", a.over)
	default:
		panic("unreachable")
	}
}

var _ AggrExpr = (*groupedAggregator)(nil)
