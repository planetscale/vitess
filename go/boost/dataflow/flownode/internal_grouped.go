package flownode

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vthash"
)

type agstatus int

const (
	aggregationOK agstatus = iota
	aggregationMiss
	aggregationEmpty
)

type agstate interface {
	len() int
	reset()
	update(record sql.Record)
	aggregate(existing *sql.Value) (sql.Value, agstatus)
}

type AggregationKind = flownodepb.Node_InternalGrouped_Aggregation_Kind

const (
	ExtremumMin      = flownodepb.Node_InternalGrouped_Aggregation_Min
	ExtremumMax      = flownodepb.Node_InternalGrouped_Aggregation_Max
	AggregationCount = flownodepb.Node_InternalGrouped_Aggregation_Count
	AggregationSum   = flownodepb.Node_InternalGrouped_Aggregation_Sum
)

type Aggregation flownodepb.Node_InternalGrouped_Aggregation

func (ag *Aggregation) description() string {
	switch ag.Kind {
	case ExtremumMin:
		return fmt.Sprintf("MIN(:%d)", ag.Over)
	case ExtremumMax:
		return fmt.Sprintf("MAX(:%d)", ag.Over)
	case AggregationCount:
		if ag.Over < 0 {
			return "COUNT(*)"
		}
		return fmt.Sprintf("COUNT(%d)", ag.Over)
	case AggregationSum:
		return fmt.Sprintf("SUM(%d)", ag.Over)
	default:
		panic("unreachable")
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

func (ag *Aggregation) defaultValue() sqltypes.Value {
	switch ag.Kind {
	case AggregationCount:
		return defaultValueZero
	default:
		return sqltypes.NULL
	}
}

type Grouped struct {
	src       dataflow.IndexPair
	srcSchema []sql.Type
	inner     []Aggregation

	cols       int
	scalar     bool
	groupByKey []int
	outKey     []int
	colfix     []int

	state []agstate
}

func (g *Grouped) internal() {}

func (g *Grouped) dataflow() {}

func (g *Grouped) Ancestors() []graph.NodeIdx {
	return []graph.NodeIdx{g.src.AsGlobal()}
}

func (g *Grouped) SuggestIndexes(you graph.NodeIdx) map[graph.NodeIdx][]int {
	return map[graph.NodeIdx][]int{you: g.outKey}
}

func (g *Grouped) Resolve(col int) []NodeColumn {
	if col == len(g.colfix) {
		return nil
	}
	return []NodeColumn{{
		Node:   g.src.AsGlobal(),
		Column: g.colfix[col],
	}}
}

func (g *Grouped) ParentColumns(col int) []NodeColumn {
	if col == len(g.colfix) {
		return []NodeColumn{{Node: g.src.AsGlobal(), Column: -1}}
	}
	return []NodeColumn{{Node: g.src.AsGlobal(), Column: g.colfix[col]}}
}

func (g *Grouped) ColumnType(gra *graph.Graph[*Node], col int) (sql.Type, error) {
	parent := gra.Value(g.src.AsGlobal())

	if col < len(g.colfix) {
		return parent.ColumnType(gra, g.colfix[col])
	}

	agg := g.inner[col-len(g.colfix)]
	switch agg.Kind {
	case ExtremumMin, ExtremumMax:
		return parent.ColumnType(gra, agg.Over)

	case AggregationCount:
		return sql.Type{T: sqltypes.Int64, Nullable: false, Collation: collations.CollationBinaryID}, nil

	case AggregationSum:
		tt, err := parent.ColumnType(gra, agg.Over)
		if err != nil {
			return sql.Type{}, err
		}
		switch {
		case sqltypes.IsFloat(tt.T):
			return sql.Type{T: sqltypes.Float64, Nullable: true, Collation: collations.CollationBinaryID}, nil
		case sqltypes.IsIntegral(tt.T) || tt.T == sqltypes.Decimal:
			return sql.Type{T: sqltypes.Decimal, Nullable: true, Collation: collations.CollationBinaryID}, nil
		}
		return sql.Type{}, fmt.Errorf("unsupported type for SUM: %v", tt.T)
	default:
		panic("unreachable")
	}
}

func (g *Grouped) Description() string {
	var descs []string
	for _, inner := range g.inner {
		descs = append(descs, inner.description())
	}
	descs = append(descs, fmt.Sprintf("GROUP BY(%v)", g.groupByKey))
	return strings.Join(descs, ", ")
}

func (g *Grouped) OnConnected(graph *graph.Graph[*Node]) error {
	srcn := graph.Value(g.src.AsGlobal())
	for _, agg := range g.inner {
		if agg.Over >= len(srcn.Fields()) {
			return fmt.Errorf("invalid aggregation column: %d", agg.Over)
		}
	}

	var err error
	g.srcSchema, err = srcn.ResolveSchema(graph)
	if err != nil {
		return err
	}
	g.cols = len(srcn.Fields())
	sort.Ints(g.groupByKey)

	g.outKey = make([]int, 0, len(g.groupByKey))
	for i := range g.groupByKey {
		g.outKey = append(g.outKey, i)
	}

	for c := 0; c < g.cols; c++ {
		if slices.Contains(g.groupByKey, c) {
			g.colfix = append(g.colfix, c)
		}
	}
	return nil
}

func (g *Grouped) OnCommit(_ graph.NodeIdx, remap map[graph.NodeIdx]dataflow.IndexPair) {
	g.src.Remap(remap)
}

func (g *Grouped) OnDeploy() error {
	for _, ag := range g.inner {
		var st agstate

		switch ag.Kind {
		case AggregationCount:
			st = &agstateCount{over: ag.Over, scalar: g.scalar}

		case AggregationSum:
			tt := g.srcSchema[ag.Over].T
			switch {
			case sqltypes.IsIntegral(tt):
				st = &agstateSumInt{over: ag.Over, scalar: g.scalar}
			case tt == sqltypes.Decimal:
				st = &agstateSumDecimal{over: ag.Over, scalar: g.scalar}
			case sqltypes.IsFloat(tt):
				st = &agstateSumFloat{over: ag.Over, scalar: g.scalar}
			default:
				return fmt.Errorf("unsupported aggregation: SUM(%s)", tt)
			}

		case ExtremumMin, ExtremumMax:
			var err error
			st, err = createExtremumState(g.srcSchema[ag.Over], ag.Kind == ExtremumMax, ag.Over)
			if err != nil {
				return err
			}
		default:
			panic("unreachable")
		}

		g.state = append(g.state, st)
	}
	return nil
}

func getGroupValues(groupBy []int, row sql.Record) (group []sql.Value) {
	group = make([]sql.Value, 0, len(groupBy))
	for _, idx := range groupBy {
		group = append(group, row.Row.ValueAt(idx))
	}
	return group
}

func (g *Grouped) InitEmptyState(rs []sql.Record, repl replay.Context, st *state.Memory) []sql.Record {
	if len(rs) == 0 && g.scalar {
		if partial := repl.Partial; partial != nil {
			for key := range partial.Keys {
				def := sql.NewRowBuilder(len(partial.KeyCols) + len(g.inner))
				for i := range partial.KeyCols {
					def.Add(key.ValueAt(i))
				}
				for _, inn := range g.inner {
					def.AddVitess(inn.defaultValue())
				}
				rs = append(rs, def.Finish().ToRecord(true))
			}
		} else if regular := repl.Regular; regular != nil && regular.Last && st.IsEmpty() {
			def := sql.NewRowBuilder(1 + len(g.inner))
			def.AddVitess(sqltypes.NewInt64(0))
			for _, inn := range g.inner {
				def.AddVitess(inn.defaultValue())
			}
			rs = append(rs, def.Finish().ToRecord(true))
		}
	}
	return rs
}

func (g *Grouped) OnInput(you *Node, ex processing.Executor, from dataflow.LocalNodeIdx, rs []sql.Record, repl replay.Context, domain *Map, states *state.Map) (processing.Result, error) {
	us := you.LocalAddr()
	db := states.Get(us)
	if db == nil {
		panic("grouped operators must have their own state materialized")
	}

	if len(rs) == 0 {
		rs = g.InitEmptyState(rs, repl, db)
		return processing.Result{Records: rs}, nil
	}

	// First, we want to be smart about multiple added/removed rows with same group.
	// For example, if we get a -, then a +, for the same group, we don't want to
	// execute two queries. We'll do this by sorting the batch by our group by.
	var hashrs = make([]sql.HashedRecord, 0, len(rs))
	var hasher vthash.Hasher
	for _, r := range rs {
		hashrs = append(hashrs, sql.HashedRecord{
			Record: r,
			Hash:   r.Row.HashWithKey(&hasher, g.groupByKey, g.srcSchema),
		})
	}
	slices.SortStableFunc(hashrs, func(a, b sql.HashedRecord) bool {
		return bytes.Compare(a.Hash[:], b.Hash[:]) < 0
	})

	var misses []processing.Miss
	var lookups []processing.Lookup
	var out []sql.Record
	replKey := repl.Key()

	handleGroup := func(groupRs []sql.HashedRecord, states []agstate) error {
		group := sql.RowFromValues(getGroupValues(g.groupByKey, groupRs[0].Record))

		rs, found := db.Lookup(g.outKey, group)
		if found {
			if replKey != nil {
				lookups = append(lookups, processing.Lookup{
					On:   us,
					Cols: g.outKey,
					Key:  group,
				})
			}
		} else {
			for _, r := range groupRs {
				misses = append(misses, processing.Miss{
					On:         us,
					LookupIdx:  g.outKey,
					LookupCols: g.groupByKey,
					ReplayCols: replKey,
					Record:     r,
					ForceTag:   dataflow.TagNone,
				})
			}
			return nil
		}

		var negative = rs.Len() > 0
		var positive bool
		var changed bool
		var existing []sql.Value
		var newstate []sql.Value

		if negative {
			values := rs.First().ToValues()
			existing = values[len(values)-len(states):]
		}

		for i, st := range states {
			var current *sql.Value
			if existing != nil {
				current = &existing[i]
			}
			newst, kind := st.aggregate(current)
			switch kind {
			case aggregationOK:
				newstate = append(newstate, newst)
				if current == nil || current.Cmp(newst) != 0 {
					changed = true
					positive = true
				}
			case aggregationMiss:
				for _, r := range groupRs {
					misses = append(misses, processing.Miss{
						On:         us,
						LookupIdx:  g.outKey,
						LookupCols: g.groupByKey,
						ReplayCols: replKey,
						Record:     r,
						Flush:      true,
						ForceTag:   dataflow.TagNone,
					})
				}
				return nil
			case aggregationEmpty:
				changed = true
			}
		}

		if changed {
			if negative {
				out = append(out, rs.First().ToRecord(false))
			}
			if positive {
				var builder = sql.NewRowBuilder(len(g.groupByKey) + len(newstate))
				for _, v := range group.ToValues() { // TODO: do not call ToValues
					builder.Add(v)
				}
				for _, v := range newstate {
					builder.Add(v)
				}
				out = append(out, builder.Finish().ToRecord(true))
			}
		}
		return nil
	}

	for _, st := range g.state {
		st.reset()
	}

	var groupRs []sql.HashedRecord
	for _, r := range hashrs {
		if len(groupRs) > 0 && groupRs[0].Hash != r.Hash {
			err := handleGroup(groupRs, g.state)
			if err != nil {
				return processing.Result{}, err
			}
			groupRs = groupRs[:0]
			for _, st := range g.state {
				st.reset()
			}
		}

		for _, st := range g.state {
			st.update(r.Record)
		}
		groupRs = append(groupRs, r)
	}

	for _, st := range g.state {
		if st.len() == 0 && len(rs) > 0 {
			panic("diffs should not be empty")
		}
	}

	err := handleGroup(groupRs, g.state)
	if err != nil {
		return processing.Result{}, err
	}

	return processing.Result{
		Records: out,
		Lookups: lookups,
		Misses:  misses,
	}, nil
}

var _ Internal = (*Grouped)(nil)
var _ ingredientEmptyState = (*Grouped)(nil)

func NewGrouped(src graph.NodeIdx, scalar bool, groupByKey []int, op []Aggregation) *Grouped {
	return &Grouped{
		src:        dataflow.NewIndexPair(src),
		inner:      op,
		scalar:     scalar,
		groupByKey: groupByKey,
	}
}

func newGroupedFromProto(pgroup *flownodepb.Node_InternalGrouped) *Grouped {
	grp := &Grouped{
		src:        pgroup.Src,
		srcSchema:  pgroup.SrcSchema,
		inner:      nil,
		cols:       pgroup.Cols,
		scalar:     pgroup.Scalar,
		groupByKey: pgroup.GroupByKey,
		outKey:     pgroup.OutKey,
		colfix:     pgroup.Colfix,
	}

	for _, agg := range pgroup.Inner {
		grp.inner = append(grp.inner, Aggregation(agg))
	}
	return grp
}

func (g *Grouped) ToProto() *flownodepb.Node_InternalGrouped {
	pg := &flownodepb.Node_InternalGrouped{
		Src:        g.src,
		SrcSchema:  g.srcSchema,
		Cols:       g.cols,
		Scalar:     g.scalar,
		GroupByKey: g.groupByKey,
		OutKey:     g.outKey,
		Colfix:     g.colfix,
	}

	for _, agg := range g.inner {
		pg.Inner = append(pg.Inner, flownodepb.Node_InternalGrouped_Aggregation(agg))
	}

	return pg
}
