package flownode

import (
	"fmt"
	"strconv"
	"time"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/mysql/decimal"

	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
)

type deltaValue interface {
	int64 | uint64 | float64 | decimal.Decimal | time.Time | []byte
}

type delta[N deltaValue] struct {
	v        N
	positive bool
	null     bool
}

type agstateExtremum[N deltaValue] struct {
	cmp  func(N, N) int
	from func(sql.Value) (N, error)
	to   func(N) sql.Value

	diffs []delta[N]
	max   bool
	over  int
}

func (g *agstateExtremum[N]) len() int {
	return len(g.diffs)
}

func (g *agstateExtremum[N]) reset() {
	g.diffs = g.diffs[:0]
}

func (g *agstateExtremum[N]) update(r sql.Record) {
	sqlval := r.Row.ValueAt(g.over)
	if sqlval.Type() == sqltypes.Null {
		g.diffs = append(g.diffs, delta[N]{null: true})
	} else {
		v, err := g.from(sqlval)
		if err != nil {
			panic(err)
		}
		g.diffs = append(g.diffs, delta[N]{v: v, positive: r.Positive})
	}
}

func (g *agstateExtremum[N]) aggregate(current *sql.Value) (sql.Value, agstatus) {
	var extremes []N
	var isExtreme func(v N) bool

	if current != nil && current.Type() != sqltypes.Null {
		n, _ := g.from(*current)
		extremes = append(extremes, n)

		if g.max {
			isExtreme = func(x N) bool { return g.cmp(x, n) >= 0 }
		} else {
			isExtreme = func(x N) bool { return g.cmp(x, n) <= 0 }
		}
	} else {
		isExtreme = func(_ N) bool { return true }
	}

	null := true
	for _, d := range g.diffs {
		if d.null {
			continue
		}
		null = false

		if isExtreme(d.v) {
			if d.positive {
				extremes = append(extremes, d.v)
			} else {
				idx := slices.IndexFunc(extremes, func(n N) bool {
					return g.cmp(n, d.v) == 0
				})
				if idx >= 0 {
					extremes[idx] = extremes[len(extremes)-1]
					extremes = extremes[:len(extremes)-1]
				}
			}
		}
	}

	if len(extremes) == 0 {
		if null {
			return sql.NULL, aggregationOK
		}
		return "", aggregationMiss
	}

	extreme := extremes[0]
	if g.max {
		for _, v := range extremes[1:] {
			if g.cmp(v, extreme) > 0 {
				extreme = v
			}
		}
	} else {
		for _, v := range extremes[1:] {
			if g.cmp(v, extreme) < 0 {
				extreme = v
			}
		}
	}
	return g.to(extreme), aggregationOK
}

func createExtremumState(tt sql.Type, max bool, over int) (agstate, error) {
	switch {
	case sqltypes.IsSigned(tt.T):
		return &agstateExtremum[int64]{
			cmp: func(a, b int64) int {
				if a < b {
					return -1
				}
				if a > b {
					return 1
				}
				return 0
			},
			from: func(value sql.Value) (int64, error) {
				return strconv.ParseInt(value.RawStr(), 10, 64)
			},
			to: func(n int64) sql.Value {
				return sql.MakeValue(tt.T, func(buf []byte) []byte {
					return strconv.AppendInt(buf, n, 10)
				})
			},
			max:  max,
			over: over,
		}, nil
	case sqltypes.IsUnsigned(tt.T):
		return &agstateExtremum[uint64]{
			cmp: func(a, b uint64) int {
				if a < b {
					return -1
				}
				if a > b {
					return 1
				}
				return 0
			},
			from: func(value sql.Value) (uint64, error) {
				return strconv.ParseUint(value.RawStr(), 10, 64)
			},
			to: func(n uint64) sql.Value {
				return sql.MakeValue(tt.T, func(buf []byte) []byte {
					return strconv.AppendUint(buf, n, 10)
				})
			},
			max:  max,
			over: over,
		}, nil
	case sqltypes.IsFloat(tt.T):
		return &agstateExtremum[float64]{
			cmp: func(a, b float64) int {
				if a < b {
					return -1
				}
				if a > b {
					return 1
				}
				return 0
			},
			from: func(value sql.Value) (float64, error) {
				return strconv.ParseFloat(value.RawStr(), 64)
			},
			to: func(f float64) sql.Value {
				return sql.MakeValue(tt.T, func(buf []byte) []byte {
					return mysql.AppendFloat(buf, tt.T, f)
				})
			},
			max:  max,
			over: over,
		}, nil
	case tt.T == sqltypes.Decimal:
		return &agstateExtremum[decimal.Decimal]{
			cmp: func(a, b decimal.Decimal) int {
				return a.Cmp(b)
			},
			from: func(value sql.Value) (decimal.Decimal, error) {
				return decimal.NewFromMySQL(value.RawBytes())
			},
			to: func(f decimal.Decimal) sql.Value {
				return sql.MakeValue(tt.T, func(buf []byte) []byte {
					return append(buf, f.FormatMySQL(0)...)
				})
			},
			max:  max,
			over: over,
		}, nil
	case tt.T == sqltypes.Date, tt.T == sqltypes.Datetime, tt.T == sqltypes.Timestamp:
		return &agstateExtremum[time.Time]{
			cmp: func(a, b time.Time) int {
				if a.Before(b) {
					return -1
				}
				if a.After(b) {
					return 1
				}
				return 0
			},
			from: func(value sql.Value) (time.Time, error) {
				str := value.RawStr()
				switch tt.T {
				case sqltypes.Date:
					if str == "0000-00-00" {
						return time.Time{}, nil
					}
					return datetime.ParseDate(str)
				case sqltypes.Datetime, sqltypes.Timestamp:
					if str == "0000-00-00 00:00:00" {
						return time.Time{}, nil
					}
					return datetime.ParseDateTime(str)
				}
				return time.Time{}, fmt.Errorf("invalid type %v", tt)
			},
			to: func(t time.Time) sql.Value {
				return sql.MakeValue(tt.T, func(buf []byte) []byte {
					switch tt.T {
					case sqltypes.Date:
						if t.IsZero() {
							return append(buf, []byte("0000-00-00")...)
						}
						return t.AppendFormat(buf, "2006-01-02")
					case sqltypes.Datetime, sqltypes.Timestamp:
						if t.IsZero() {
							return append(buf, []byte("0000-00-00 00:00:00")...)
						}
						return t.AppendFormat(buf, "2006-01-02 15:04:05.999999")
					}
					return nil
				})
			},
			max:  max,
			over: over,
		}, nil
	case sqltypes.IsQuoted(tt.T):
		col := tt.Collation.Get()
		return &agstateExtremum[[]byte]{
			cmp: func(a, b []byte) int {
				return col.Collate(a, b, false)
			},
			from: func(value sql.Value) ([]byte, error) {
				return value.RawBytes(), nil
			},
			to: func(v []byte) sql.Value {
				return sql.MakeValue(tt.T, func(buf []byte) []byte {
					return append(buf, v...)
				})
			},
			max:  max,
			over: over,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported aggregation: MIN/MAX(%s)", tt.T)
	}
}
