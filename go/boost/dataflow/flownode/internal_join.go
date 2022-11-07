package flownode

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vthash"
)

type JoinType = boostpb.Node_InternalJoin_JoinType

const (
	JoinTypeOuter = boostpb.Node_InternalJoin_Left
	JoinTypeInner = boostpb.Node_InternalJoin_Inner
)

func ParserJoinTypeToFlowNodeJoinType(inner bool) JoinType {
	if inner {
		return JoinTypeInner
	}
	return JoinTypeOuter
}

type preprocessed int32

const (
	preprocessedLeft preprocessed = iota
	preprocessedRight
	preprocessedNeither
)

type JoinSource struct {
	left  int
	right int
}

func JoinSourceLeft(col int) JoinSource {
	return JoinSource{left: col, right: -1}
}

func JoinSourceRight(col int) JoinSource {
	return JoinSource{left: -1, right: col}
}

func JoinSourceBoth(left, right int) JoinSource {
	return JoinSource{left: left, right: right}
}

type emission = boostpb.Node_InternalJoin_Emission

var _ Internal = (*Join)(nil)
var _ JoinIngredient = (*Join)(nil)

type Join struct {
	left  boostpb.IndexPair
	right boostpb.IndexPair

	on               [2]int
	emit             []emission
	inPlaceLeftEmit  []emission
	inPlaceRightEmit []emission

	kind JoinType
}

func (j *Join) internal() {}

func (j *Join) dataflow() {}

var _ JoinIngredient = (*Join)(nil)

func (j *Join) DataflowNode() {}

func (j *Join) IsJoin() {}

func (j *Join) MustReplayAmong() map[graph.NodeIdx]struct{} {
	switch j.kind {
	case JoinTypeOuter:
		return map[graph.NodeIdx]struct{}{
			j.left.AsGlobal(): {},
		}
	case JoinTypeInner:
		return map[graph.NodeIdx]struct{}{
			j.left.AsGlobal():  {},
			j.right.AsGlobal(): {},
		}
	default:
		panic("unreachable")
	}
}

func (j *Join) Ancestors() []graph.NodeIdx {
	return []graph.NodeIdx{j.left.AsGlobal(), j.right.AsGlobal()}
}

func (j *Join) SuggestIndexes(you graph.NodeIdx) map[graph.NodeIdx][]int {
	return map[graph.NodeIdx][]int{
		j.left.AsGlobal():  {j.on[0]},
		j.right.AsGlobal(): {j.on[1]},
	}
}

func (j *Join) Resolve(col int) []NodeColumn {
	e := j.emit[col]
	if e.Left {
		return []NodeColumn{{j.left.AsGlobal(), e.Col}}
	}
	return []NodeColumn{{j.right.AsGlobal(), e.Col}}
}

func (j *Join) ParentColumns(col int) []NodeColumn {
	pcol := j.emit[col]
	if (pcol.Left && pcol.Col == j.on[0]) || (!pcol.Left && pcol.Col == j.on[1]) {
		// join column comes from both parents
		return []NodeColumn{
			{j.left.AsGlobal(), j.on[0]},
			{j.right.AsGlobal(), j.on[1]},
		}
	}

	var parent boostpb.IndexPair
	if pcol.Left {
		parent = j.left
	} else {
		parent = j.right
	}
	return []NodeColumn{{parent.AsGlobal(), pcol.Col}}
}

func (j *Join) ColumnType(g *graph.Graph[*Node], col int) boostpb.Type {
	parents := j.ParentColumns(col)
	switch len(parents) {
	case 1:
		return g.Value(parents[0].Node).ColumnType(g, parents[0].Column)

	case 2:
		t1 := g.Value(parents[0].Node).ColumnType(g, parents[0].Column)
		t2 := g.Value(parents[1].Node).ColumnType(g, parents[1].Column)

		var comparisonType = t1.T
		if t1.T != t2.T {
			var err error
			comparisonType, err = evalengine.CoerceTo(t1.T, t2.T)
			if err != nil {
				panic(err)
			}
		}
		return boostpb.Type{
			T:         comparisonType,
			Collation: t1.Collation, // TODO: We need to merge collations here instead
			Nullable:  t1.Nullable || t2.Nullable,
		}

	default:
		panic("???")
	}
}

func (j *Join) Description(detailed bool) string {
	var op string
	switch j.kind {
	case JoinTypeOuter:
		op = "⋉"
	case JoinTypeInner:
		op = "⋈"
	}

	if !detailed {
		return op
	}

	var buf strings.Builder
	for i, e := range j.emit {
		src := j.right
		if e.Left {
			src = j.left
		}
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%v:%v", src, e.Col)
	}

	return fmt.Sprintf("[%v] %v:%v %v %v:%v", buf.String(), j.left, j.on[0], op, j.right, j.on[1])
}

func (j *Join) OnConnected(graph *graph.Graph[*Node]) {
	// No-op
}

func (j *Join) OnCommit(_ graph.NodeIdx, remap map[graph.NodeIdx]boostpb.IndexPair) {
	j.left.Remap(remap)
	j.right.Remap(remap)
}

func (j *Join) OnInput(us *Node, ex processing.Executor, from boostpb.LocalNodeIndex, rs []boostpb.Record, replayKeyCols []int, domain *Map, state *state.Map) (processing.Result, error) {
	var misses []processing.Miss
	var lookups []processing.Lookup

	if len(rs) == 0 {
		return processing.Result{
			Records: rs,
			Misses:  misses,
			Lookups: lookups,
		}, nil
	}

	var other boostpb.LocalNodeIndex
	var fromKey int
	var otherKey int
	if from == j.left.AsLocal() {
		other = j.right.AsLocal()
		fromKey = j.on[0]
		otherKey = j.on[1]
	} else {
		other = j.left.AsLocal()
		fromKey = j.on[1]
		otherKey = j.on[0]
	}

	var jointype boostpb.Type
	for col, pcol := range j.emit {
		if (pcol.Left && pcol.Col == j.on[0]) || (!pcol.Left && pcol.Col == j.on[1]) {
			jointype = us.schema[col]
			break
		}
	}

	if replayKeyCols != nil {
		mappedReplayKeyCols := make([]int, 0, len(replayKeyCols))
		for _, replayCol := range replayKeyCols {
			e := j.emit[replayCol]
			if e.Left {
				if from == j.left.AsLocal() {
					mappedReplayKeyCols = append(mappedReplayKeyCols, e.Col)
				} else if e.Col == j.on[0] {
					// since we didn't hit the case above, we know that the message
					// *isn't* from left.
					mappedReplayKeyCols = append(mappedReplayKeyCols, j.on[1])
				}
			} else {
				if from == j.right.AsLocal() {
					mappedReplayKeyCols = append(mappedReplayKeyCols, e.Col)
				} else if e.Col == j.on[1] {
					// same
					mappedReplayKeyCols = append(mappedReplayKeyCols, j.on[0])
				}
			}
		}
		replayKeyCols = mappedReplayKeyCols
	}

	// First, we want to be smart about multiple added/removed rows with the same join key
	// value. For example, if we get a -, then a +, for the same key, we don't want to execute
	// two queries. We'll do this by sorting the batch by our join key.
	var hashrs = make([]boostpb.HashedRecord, 0, len(rs))
	var hasher vthash.Hasher
	for _, r := range rs {
		hashrs = append(hashrs, boostpb.HashedRecord{
			Record: r,
			Hash:   r.Row.HashValue(&hasher, fromKey, jointype),
		})
	}
	sort.SliceStable(hashrs, func(i, j int) bool {
		return bytes.Compare(hashrs[i].Hash[:], hashrs[j].Hash[:]) < 0
	})

	var joinrs = rs[:0]
	var at = 0

	for at < len(hashrs) {
		oldRightCount := -1
		newRightCount := -1
		prevJoinKey := hashrs[at].Row.ValueAt(fromKey)
		prevJoinHash := hashrs[at].Hash
		prevJoinKeyRow := boostpb.RowFromValues([]boostpb.Value{prevJoinKey})

		if from == j.right.AsLocal() && j.kind == JoinTypeOuter {
			rowBag, found, isMaterialized, err := Lookup(j, j.right.AsLocal(), []int{j.on[1]}, prevJoinKeyRow, domain, state)
			if err != nil {
				return processing.Result{}, err
			}
			if !isMaterialized {
				panic("join parent should always be materialized")
			}
			if !found {
				// we got something from right, but that row's key is not in right??
				//
				// this *can* happen! imagine if you have two partial indices on right,
				// one on column a and one on column b. imagine that a is the join key.
				// we get a replay request for b = 4, which must then be replayed from
				// right (since left doesn't have b). say right replays (a=1,b=4). we
				// will hit this case, since a=1 is not in right. the correct thing to
				// do here is to replay a=1 first, and *then* replay b=4 again
				// (possibly several times over for each a).
				for at < len(hashrs) {
					if hashrs[at].Hash != prevJoinHash {
						break
					}
					at++
				}
				continue
			}

			if replayKeyCols != nil {
				lookups = append(lookups, processing.Lookup{
					On:   j.right.AsLocal(),
					Cols: []int{j.on[1]},
					Key:  prevJoinKeyRow,
				})
			}

			rc := rowBag.Len()
			oldRightCount = rc
			newRightCount = rc
		}

		// get rows from the other side
		otherRows, found, isMaterialized, err := Lookup(j, other, []int{otherKey}, prevJoinKeyRow, domain, state)
		if err != nil {
			return processing.Result{}, err
		}
		if !isMaterialized {
			panic("other should always be materialized")
		}
		if !found {
			// we missed in the other side!
			from := at
			for at < len(hashrs) {
				if hashrs[at].Hash != prevJoinHash {
					break
				}
				at++
			}
			for i := from; i < at; i++ {
				misses = append(misses, processing.Miss{
					On:         other,
					LookupIdx:  []int{otherKey},
					LookupCols: []int{fromKey},
					ReplayCols: replayKeyCols,
					Record:     hashrs[i],
				})
				hashrs[i] = boostpb.HashedRecord{}
			}
			continue
		}

		if replayKeyCols != nil {
			lookups = append(lookups, processing.Lookup{
				On:   other,
				Cols: []int{otherKey},
				Key:  prevJoinKeyRow,
			})
		}

		start := at
		makePositiveNull := false
		makeNegativeNull := false

		if j.kind == JoinTypeOuter && from == j.right.AsLocal() {
			// If records are being received from the right, we need to find the number of
			// records that existed *before* this batch of records was processed so we know
			// whether or not to generate +/- NULL rows.
			if oldRightCount >= 0 {
				oldRc := oldRightCount
				for at < len(hashrs) && hashrs[at].Hash == prevJoinHash {
					if hashrs[at].Positive {
						oldRc--
					} else {
						oldRc++
					}
					at++
				}

				if newRightCount < 0 {
					panic("new right count should have been seen at least once")
				}
				// emit null rows if necessary for left join
				newRc := newRightCount
				if newRc == 0 && oldRc != 0 {
					makePositiveNull = true
				} else if newRc != 0 && oldRc == 0 {
					makeNegativeNull = true
				}
			} else {
				// we got a right, but missed in right; clearly, a replay is needed
				start := at
				for at < len(hashrs) {
					if hashrs[at].Hash != prevJoinHash {
						break
					}
					at++
				}
				for i := start; i < at; i++ {
					misses = append(misses, processing.Miss{
						On:         from,
						LookupIdx:  []int{j.on[1]},
						LookupCols: []int{fromKey},
						ReplayCols: replayKeyCols,
						Record:     hashrs[i],
					})
					hashrs[i] = boostpb.HashedRecord{}
				}
				continue
			}
		}

		if start == at {
			// we didn't find the end above, so find it now
			for at < len(hashrs) {
				if hashrs[at].Hash != prevJoinHash {
					break
				}
				at++
			}
		}

		otherRowsCount := 0
		for _, r := range hashrs[start:at] {
			// put something bogus in rs (which will be discarded anyway) so we can take r.
			row := r.Row
			positive := r.Positive
			if otherRows != nil {
				// we have yet to iterate through other_rows
				if otherRows.Len() == 0 {
					if j.kind == JoinTypeOuter && from == j.left.AsLocal() {
						// left join, got a thing from left, no rows in right == NULL
						joinrs = append(joinrs, j.generateNull(row).ToRecord(positive))
					}
					otherRows = nil
					continue
				}

				otherRows.ForEach(func(other boostpb.Row) {
					if makeNegativeNull {
						joinrs = append(joinrs, j.generateNull(other).ToRecord(false))
					}
					if from == j.left.AsLocal() {
						joinrs = append(joinrs, j.generateRow(row, other, preprocessedNeither).ToRecord(positive))
					} else {
						joinrs = append(joinrs, j.generateRow(other, row, preprocessedNeither).ToRecord(positive))
					}
					if makePositiveNull {
						joinrs = append(joinrs, j.generateNull(other).ToRecord(true))
					}
					otherRowsCount++
				})

				otherRows = nil
			} else if otherRowsCount == 0 {
				if j.kind == JoinTypeOuter && from == j.left.AsLocal() {
					// left join, got a thing from left, no rows in right == NULL
					joinrs = append(joinrs, j.generateNull(row).ToRecord(positive))
				}
			} else {
				start := len(joinrs) - otherRowsCount
				end := len(joinrs)
				for i := start; i < end; i++ {
					if from == j.left.AsLocal() {
						joinrs = append(joinrs, j.generateRow(row, joinrs[i].Row, preprocessedRight).ToRecord(positive))
					} else {
						joinrs = append(joinrs, j.generateRow(joinrs[i].Row, row, preprocessedLeft).ToRecord(positive))
					}
				}
			}
		}
	}

	return processing.Result{
		Records: joinrs,
		Misses:  misses,
		Lookups: lookups,
	}, nil
}

func (j *Join) generateNull(left boostpb.Row) boostpb.Row {
	result := boostpb.NewRowBuilder(len(j.emit))
	for _, e := range j.emit {
		if e.Left {
			result.Add(left.ValueAt(e.Col))
		} else {
			result.Add(boostpb.NULL)
		}
	}
	return result.Finish()
}

func (j *Join) generateRow(left boostpb.Row, right boostpb.Row, reusing preprocessed) boostpb.Row {
	result := boostpb.NewRowBuilder(len(j.emit))
	for i, e := range j.emit {
		if e.Left {
			if reusing == preprocessedLeft {
				result.Add(left.ValueAt(i))
			} else {
				result.Add(left.ValueAt(e.Col))
			}
		} else {
			if reusing == preprocessedRight {
				result.Add(right.ValueAt(i))
			} else {
				result.Add(right.ValueAt(e.Col))
			}
		}
	}
	return result.Finish()
}

func NewJoin(left, right graph.NodeIdx, kind JoinType, joinSources []JoinSource) *Join {
	var joinColumns [][2]int
	var emit []emission

	for _, e := range joinSources {
		switch {
		case e.left >= 0 && e.right >= 0:
			joinColumns = append(joinColumns, [2]int{e.left, e.right})
			emit = append(emit, emission{Left: true, Col: e.left})
		case e.left >= 0:
			emit = append(emit, emission{Left: true, Col: e.left})
		case e.right >= 0:
			emit = append(emit, emission{Left: false, Col: e.right})
		}
	}

	if len(joinColumns) > 1 {
		panic("unsupported: multiple column joins")
	}

	computeInPlaceEmit := func(left bool) []emission {
		var numColumns int
		for _, e := range emit {
			if e.Left == left {
				if e.Col+1 > numColumns {
					numColumns = e.Col + 1
				}
			}
		}

		var remap = make([]int, numColumns)
		for i := range remap {
			remap[i] = i
		}

		var inplace []emission
		for i, e := range emit {
			if e.Left == left {
				remapped := remap[e.Col]
				other := slices.Index(remap, i)
				remap[e.Col] = i
				if other != -1 {
					remap[other] = remapped
				}
				inplace = append(inplace, emission{Left: e.Left, Col: remapped})
			} else {
				inplace = append(inplace, emission{Left: e.Left, Col: e.Col})
			}
		}
		return inplace
	}

	return &Join{
		left:             boostpb.NewIndexPair(left),
		right:            boostpb.NewIndexPair(right),
		on:               joinColumns[0],
		emit:             emit,
		inPlaceLeftEmit:  computeInPlaceEmit(true),
		inPlaceRightEmit: computeInPlaceEmit(false),
		kind:             kind,
	}
}

func (j *Join) ToProto() *boostpb.Node_InternalJoin {
	return &boostpb.Node_InternalJoin{
		Left:             &j.left,
		Right:            &j.right,
		On0:              j.on[0],
		On1:              j.on[1],
		Emit:             j.emit,
		InPlaceLeftEmit:  j.inPlaceLeftEmit,
		InPlaceRightEmit: j.inPlaceRightEmit,
		Kind:             j.kind,
	}
}

func NewJoinFromProto(pjoin *boostpb.Node_InternalJoin) *Join {
	return &Join{
		left:             *pjoin.Left,
		right:            *pjoin.Right,
		on:               [2]int{pjoin.On0, pjoin.On1},
		emit:             pjoin.Emit,
		inPlaceLeftEmit:  pjoin.InPlaceLeftEmit,
		inPlaceRightEmit: pjoin.InPlaceRightEmit,
		kind:             pjoin.Kind,
	}
}
