package flownode

import (
	"fmt"
	"strconv"

	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type agstateCount struct {
	diffs  []int8
	over   int
	scalar bool
}

func (g *agstateCount) len() int {
	return len(g.diffs)
}

func (g *agstateCount) reset() {
	g.diffs = g.diffs[:0]
}

func (g *agstateCount) update(r sql.Record) {
	if g.over >= 0 {
		if r.Row.ValueAt(g.over).Type() == sqltypes.Null {
			g.diffs = append(g.diffs, 0)
			return
		}
	}
	if r.Positive {
		g.diffs = append(g.diffs, 1)
	} else {
		g.diffs = append(g.diffs, -1)
	}
}

func (g *agstateCount) aggregate(current *sql.Value) (sql.Value, agstatus) {
	var n int64
	if current != nil && current.Type() != sqltypes.Null {
		var err error
		n, err = current.ToVitessUnsafe().ToInt64()
		if err != nil {
			panic(err)
		}
	}
	for _, d := range g.diffs {
		n += int64(d)
	}
	if !g.scalar && n == 0 {
		return sql.NULL, aggregationEmpty
	}
	return sql.MakeValue(sqltypes.Int64, func(buf []byte) []byte {
		return strconv.AppendInt(buf, n, 10)
	}), aggregationOK
}

type agstateSumDecimal struct {
	diffs  []evalengine.Decimal
	over   int
	offset int
	scalar bool
}

func (g *agstateSumDecimal) len() int {
	return len(g.diffs)
}

func (g *agstateSumDecimal) reset() {
	// explicitly clear the Decimals to prevent GC leaks
	for i := range g.diffs {
		g.diffs[i] = evalengine.Decimal{}
	}
	g.diffs = g.diffs[:0]
	g.offset = 0
}

func (g *agstateSumDecimal) update(r sql.Record) {
	val := r.Row.ValueAt(g.over)

	switch val.Type() {
	case sqltypes.Null:
		g.diffs = append(g.diffs, evalengine.DecimalZero)

	case sqltypes.Decimal:
		d, err := evalengine.NewDecimalFromMySQL(val.RawBytes())
		if err != nil {
			panic(err)
		}
		if r.Positive {
			g.offset++
		} else {
			g.offset--
			d.NegInPlace()
		}
		g.diffs = append(g.diffs, d)
	default:
		panic("unexpected value type")
	}
}

func (g *agstateSumDecimal) aggregate(current *sql.Value) (sql.Value, agstatus) {
	var tt sqltypes.Type
	if current != nil {
		tt = current.Type()
	}

	canBeEmpty := !g.scalar && g.offset <= 0

	switch tt {
	case sqltypes.Null:
		return aggregateSumDecimal(canBeEmpty, g.diffs[0], g.diffs[1:], nil)
	case sqltypes.Decimal:
		d, err := evalengine.NewDecimalFromMySQL(current.RawBytes())
		if err != nil {
			panic(err)
		}
		return aggregateSumDecimal(canBeEmpty, d, g.diffs, nil)
	default:
		panic(fmt.Sprintf("unexpected current type: %s", current.Type()))
	}
}

type agstateSumInt struct {
	diffs  []int64
	over   int
	offset int
	scalar bool
}

func (g *agstateSumInt) len() int {
	return len(g.diffs)
}

func (g *agstateSumInt) reset() {
	g.offset = 0
	g.diffs = g.diffs[:0]
}

func (g *agstateSumInt) update(r sql.Record) {
	val := r.Row.ValueAt(g.over)
	tt := val.Type()

	switch {
	case tt == sqltypes.Null:
		g.diffs = append(g.diffs, 0)
	case sqltypes.IsIntegral(tt):
		i, err := val.ToVitessUnsafe().ToInt64()
		if err != nil {
			panic(err)
		}
		if r.Positive {
			g.offset++
		} else {
			i = -i
			g.offset--
		}
		g.diffs = append(g.diffs, i)
	default:
		panic("unexpected value type")
	}
}

func safeAdd64(a, b int64) (int64, bool) {
	c := a + b
	if (c > a) == (b > 0) {
		return c, true
	}
	return a, false
}

func aggregateSumInt(zeroCanBeEmpty bool, sum int64, diffs []int64) (sql.Value, agstatus) {
	var ok bool
	for n, d := range diffs {
		if sum, ok = safeAdd64(sum, d); !ok {
			return aggregateSumDecimal(zeroCanBeEmpty, evalengine.NewDecimalFromInt(sum), nil, diffs[n:])
		}
	}
	if zeroCanBeEmpty && sum == 0 {
		return sql.NULL, aggregationMiss
	}
	return sql.MakeValue(sqltypes.Decimal, func(buf []byte) []byte {
		return strconv.AppendInt(buf, sum, 10)
	}), aggregationOK
}

func aggregateSumDecimal(zeroCanBeEmpty bool, sum evalengine.Decimal, diffD []evalengine.Decimal, diffI []int64) (sql.Value, agstatus) {
	for _, d := range diffD {
		sum = sum.Add(d)
	}
	for _, d := range diffI {
		sum = sum.Add(evalengine.NewDecimalFromInt(d))
	}
	if zeroCanBeEmpty && sum.IsZero() {
		return sql.NULL, aggregationMiss
	}
	return sql.MakeValue(sqltypes.Decimal, func(buf []byte) []byte {
		return append(buf, sum.FormatMySQL(0)...)
	}), aggregationOK
}

func (g *agstateSumInt) aggregate(current *sql.Value) (sql.Value, agstatus) {
	var tt sqltypes.Type
	if current != nil {
		tt = current.Type()
	}

	zeroCanBeEmpty := !g.scalar && g.offset <= 0

	switch tt {
	case sqltypes.Null:
		return aggregateSumInt(zeroCanBeEmpty, 0, g.diffs)
	case sqltypes.Int64:
		n, err := strconv.ParseInt(current.RawStr(), 10, 64)
		if err != nil {
			panic(err)
		}
		return aggregateSumInt(zeroCanBeEmpty, n, g.diffs)
	case sqltypes.Decimal:
		d, err := evalengine.NewDecimalFromMySQL(current.RawBytes())
		if err != nil {
			panic(err)
		}
		return aggregateSumDecimal(zeroCanBeEmpty, d, nil, g.diffs)
	default:
		panic(fmt.Sprintf("unexpected current type: %s", current.Type()))
	}
}

type agstateSumFloat struct {
	diffs  []float64
	over   int
	offset int
	scalar bool
}

func (g *agstateSumFloat) len() int {
	return len(g.diffs)
}

func (g *agstateSumFloat) reset() {
	g.offset = 0
	g.diffs = g.diffs[:0]
}

func (g *agstateSumFloat) update(r sql.Record) {
	f, err := r.Row.ValueAt(g.over).ToVitessUnsafe().ToFloat64()
	if err != nil {
		panic(err)
	}
	if r.Positive {
		g.offset++
	} else {
		g.offset--
		f = -f
	}
	g.diffs = append(g.diffs, f)
}

func (g *agstateSumFloat) aggregate(current *sql.Value) (sql.Value, agstatus) {
	var n float64
	if current != nil && current.Type() != sqltypes.Null {
		n, _ = current.ToVitessUnsafe().ToFloat64()
	}
	for _, d := range g.diffs {
		n += d
	}
	return sql.MakeValue(sqltypes.Float64, func(buf []byte) []byte {
		return evalengine.AppendFloat(buf, sqltypes.Float64, n)
	}), aggregationOK
}
