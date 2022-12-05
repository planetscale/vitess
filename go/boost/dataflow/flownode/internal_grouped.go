package flownode

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vthash"
)

type GroupKind interface {
	groupKindIdentifier()
}

type aggregationState interface {
	len() int
	reset()
	update(record boostpb.Record)
	apply(v *boostpb.Value) boostpb.Value
}

type AggrExpr interface {
	setup(parent *Node)
	state(schema boostpb.Type) aggregationState
	description() string
	defaultValue() sqltypes.Value
}

type Grouped struct {
	src   boostpb.IndexPair
	inner []AggrExpr

	cols       int
	scalar     bool
	groupByKey []int
	outKey     []int
	colfix     []int
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
		return []NodeColumn{{g.src.AsGlobal(), -1}}
	}
	return []NodeColumn{{g.src.AsGlobal(), g.colfix[col]}}
}

func (g *Grouped) ColumnType(gra *graph.Graph[*Node], col int) boostpb.Type {
	parent := gra.Value(g.src.AsGlobal())

	if col < len(g.colfix) {
		return parent.ColumnType(gra, g.colfix[col])
	}

	switch inner := g.inner[col-len(g.colfix)].(type) {
	case *groupedExtremum:
		return parent.ColumnType(gra, inner.over)

	case *groupedAggregator:
		switch inner.kind {
		case AggregationCount, AggregationCountStar:
			return boostpb.Type{T: sqltypes.Int64, Nullable: false, Collation: collations.CollationBinaryID}
		case AggregationSum:
			tt := parent.ColumnType(gra, inner.over)
			switch {
			case sqltypes.IsFloat(tt.T):
				return boostpb.Type{T: sqltypes.Float64, Nullable: true, Collation: collations.CollationBinaryID}
			case sqltypes.IsIntegral(tt.T) || tt.T == sqltypes.Decimal:
				return boostpb.Type{T: sqltypes.Decimal, Nullable: true, Collation: collations.CollationBinaryID}
			}
		}
	}

	panic("unreachable")
}

func (g *Grouped) Description() string {
	var descs []string
	for _, inner := range g.inner {
		descs = append(descs, inner.description())
	}
	descs = append(descs, fmt.Sprintf("GROUP BY(%v)", g.groupByKey))
	return strings.Join(descs, ", ")
}

func (g *Grouped) OnConnected(graph *graph.Graph[*Node]) {
	srcn := graph.Value(g.src.AsGlobal())
	for _, agg := range g.inner {
		agg.setup(srcn)
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
}

func (g *Grouped) OnCommit(_ graph.NodeIdx, remap map[graph.NodeIdx]boostpb.IndexPair) {
	g.src.Remap(remap)
}

func getGroupValues(groupBy []int, row boostpb.Record) (group []boostpb.Value) {
	group = make([]boostpb.Value, 0, len(groupBy))
	for _, idx := range groupBy {
		group = append(group, row.Row.ValueAt(idx))
	}
	return group
}

var defaultBogoKey = boostpb.RowFromVitess([]sqltypes.Value{sqltypes.NewInt64(0)})

func (g *Grouped) OnInput(you *Node, ex processing.Executor, from boostpb.LocalNodeIndex, rs []boostpb.Record, repl replay.Context, domain *Map, states *state.Map) (processing.Result, error) {
	us := you.LocalAddr()
	db := states.Get(us)
	if db == nil {
		panic("grouped operators must have their own state materialized")
	}

	if len(rs) == 0 {
		if g.scalar {
			if partial := repl.Partial; partial != nil {
				for key := range partial.Keys {
					def := boostpb.NewRowBuilder(len(partial.KeyCols) + len(g.inner))
					for i := range partial.KeyCols {
						def.Add(key.ValueAt(i))
					}
					for _, inn := range g.inner {
						def.AddVitess(inn.defaultValue())
					}
					rs = append(rs, def.Finish().ToRecord(true))
				}
			} else if repl.Full != nil && *repl.Full {
				rows, found := db.Lookup(g.outKey, defaultBogoKey)
				if !found {
					panic("miss on fully materialized state")
				}
				if rows.Len() == 0 {
					def := boostpb.NewRowBuilder(1 + len(g.inner))
					def.AddVitess(sqltypes.NewInt64(0))
					for _, inn := range g.inner {
						def.AddVitess(inn.defaultValue())
					}
					rs = append(rs, def.Finish().ToRecord(true))
				}
			}
		}
		return processing.Result{Records: rs}, nil
	}

	// First, we want to be smart about multiple added/removed rows with same group.
	// For example, if we get a -, then a +, for the same group, we don't want to
	// execute two queries. We'll do this by sorting the batch by our group by.
	var pschema = domain.Get(from).schema
	var hashrs = make([]boostpb.HashedRecord, 0, len(rs))
	var hasher vthash.Hasher
	for _, r := range rs {
		hashrs = append(hashrs, boostpb.HashedRecord{
			Record: r,
			Hash:   r.Row.HashWithKey(&hasher, g.groupByKey, pschema),
		})
	}
	slices.SortStableFunc(hashrs, func(a, b boostpb.HashedRecord) bool {
		return bytes.Compare(a.Hash[:], b.Hash[:]) < 0
	})

	var misses []processing.Miss
	var lookups []processing.Lookup
	var out []boostpb.Record
	replKey := repl.Key()

	handleGroup := func(groupRs []boostpb.HashedRecord, states []aggregationState) error {
		group := boostpb.RowFromValues(getGroupValues(g.groupByKey, groupRs[0].Record))

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
				})
			}
			return nil
		}

		var nonEmpty = rs.Len() > 0
		var changed bool
		var existing []boostpb.Value
		var newstate []boostpb.Value

		if nonEmpty {
			values := rs.First().ToValues()
			existing = values[len(values)-len(states):]
		}

		for i, st := range states {
			var current *boostpb.Value
			if existing != nil {
				current = &existing[i]
			}
			newst := st.apply(current)
			newstate = append(newstate, newst)

			if current == nil || current.Cmp(newst) != 0 {
				changed = true
			}
		}

		if changed {
			if nonEmpty {
				out = append(out, rs.First().ToRecord(false))
			}
			var builder = boostpb.NewRowBuilder(len(g.groupByKey) + len(newstate))
			for _, v := range group.ToValues() {
				builder.Add(v)
			}
			for _, v := range newstate {
				builder.Add(v)
			}
			out = append(out, builder.Finish().ToRecord(true))
		}
		return nil
	}

	var groupRs []boostpb.HashedRecord
	var aggStates = make([]aggregationState, 0, len(g.inner))
	for i, agg := range g.inner {
		aggStates = append(aggStates, agg.state(you.schema[len(g.colfix)+i]))
	}

	for _, r := range hashrs {
		if len(groupRs) > 0 && groupRs[0].Hash != r.Hash {
			err := handleGroup(groupRs, aggStates)
			if err != nil {
				return processing.Result{}, err
			}
			groupRs = groupRs[:0]
			for _, st := range aggStates {
				st.reset()
			}
		}

		for _, st := range aggStates {
			st.update(r.Record)
		}
		groupRs = append(groupRs, r)
	}

	for _, st := range aggStates {
		if st.len() == 0 && len(rs) > 0 {
			panic("diffs should not be empty")
		}
	}

	err := handleGroup(groupRs, aggStates)
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

func NewGrouped(src graph.NodeIdx, scalar bool, groupByKey []int, op []AggrExpr) *Grouped {
	return &Grouped{
		src:        boostpb.NewIndexPair(src),
		inner:      op,
		scalar:     scalar,
		groupByKey: groupByKey,
	}
}

func newGroupedFromProto(pgroup *boostpb.Node_InternalGrouped) *Grouped {
	grp := &Grouped{
		src:        *pgroup.Src,
		inner:      nil,
		cols:       pgroup.Cols,
		scalar:     pgroup.Scalar,
		groupByKey: pgroup.GroupByKey,
		outKey:     pgroup.OutKey,
		colfix:     pgroup.Colfix,
	}

	for _, agg := range pgroup.Inner {
		grp.inner = append(grp.inner, AggregationOver(agg.Kind, agg.Over))
	}
	return grp
}

func (g *Grouped) ToProto() *boostpb.Node_InternalGrouped {
	pg := &boostpb.Node_InternalGrouped{
		Src:        &g.src,
		Cols:       g.cols,
		Scalar:     g.scalar,
		GroupByKey: g.groupByKey,
		OutKey:     g.outKey,
		Colfix:     g.colfix,
	}

	for _, agg := range g.inner {
		switch agg := agg.(type) {
		case *groupedAggregator:
			pg.Inner = append(pg.Inner, &boostpb.Node_InternalGrouped_Aggregation{
				Kind: agg.kind,
				Over: agg.over,
			})
		case *groupedExtremum:
			pg.Inner = append(pg.Inner, &boostpb.Node_InternalGrouped_Aggregation{
				Kind: agg.kind,
				Over: agg.over,
			})
		default:
			panic("unsupported")
		}
	}

	return pg
}
