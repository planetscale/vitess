package evalengine

import "vitess.io/vitess/go/vt/vtgate/evalengine/internal/decimal"

type Decimal = decimal.Decimal

var DecimalZero = decimal.Zero

func NewDecimalFromMySQL(s []byte) (Decimal, error) {
	return decimal.NewFromMySQL(s)
}

func NewDecimalFromInt(value int64) Decimal {
	return decimal.NewFromInt(value)
}
