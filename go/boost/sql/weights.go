package sql

import (
	"encoding/binary"
	"math"
	"strconv"

	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/fastparse"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func WeightString(dst []byte, v sqltypes.Value, coerceTo Type) ([]byte, bool, error) {
	switch {
	case v.IsNull(), sqltypes.IsNull(coerceTo.T):
		return nil, true, nil
	case sqltypes.IsFloat(coerceTo.T):
		var f float64
		var err error

		switch {
		case v.IsSigned():
			var ival int64
			ival, err = v.ToInt64()
			f = float64(ival)
		case v.IsUnsigned():
			var uval uint64
			uval, err = v.ToUint64()
			f = float64(uval)
		case v.IsFloat() || v.IsDecimal():
			f, err = v.ToFloat64()
		case v.IsQuoted():
			f, _ = fastparse.ParseFloat64(v.RawStr())
		default:
			return dst, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
		}
		if err != nil {
			return dst, false, err
		}
		raw := math.Float64bits(f)
		if math.Signbit(f) {
			raw = ^raw
		} else {
			raw = raw ^ (1 << 63)
		}
		return binary.BigEndian.AppendUint64(dst, raw), true, nil

	case sqltypes.IsSigned(coerceTo.T):
		var i int64
		var err error

		switch {
		case v.IsSigned():
			i, err = v.ToInt64()
		case v.IsUnsigned():
			var uval uint64
			uval, err = v.ToUint64()
			i = int64(uval)
		case v.IsFloat():
			var fval float64
			fval, err = v.ToFloat64()
			if fval != math.Trunc(fval) {
				return dst, false, evalengine.ErrHashCoercionIsNotExact
			}
			i = int64(fval)
		case v.IsQuoted():
			i, err = strconv.ParseInt(v.RawStr(), 10, 64)
			if err != nil {
				fval, _ := fastparse.ParseFloat64(v.RawStr())
				if fval != math.Trunc(fval) {
					return dst, false, evalengine.ErrHashCoercionIsNotExact
				}
				i, err = int64(fval), nil
			}
		default:
			return dst, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
		}
		if err != nil {
			return dst, false, err
		}
		raw := uint64(i)
		raw = raw ^ (1 << 63)
		return binary.BigEndian.AppendUint64(dst, raw), true, nil

	case sqltypes.IsUnsigned(coerceTo.T):
		var u uint64
		var err error

		switch {
		case v.IsSigned():
			var ival int64
			ival, err = v.ToInt64()
			u = uint64(ival)
		case v.IsUnsigned():
			u, err = v.ToUint64()
		case v.IsFloat():
			var fval float64
			fval, err = v.ToFloat64()
			if fval != math.Trunc(fval) || fval < 0 {
				return dst, false, evalengine.ErrHashCoercionIsNotExact
			}
			u = uint64(fval)
		case v.IsQuoted():
			u, err = strconv.ParseUint(v.RawStr(), 10, 64)
			if err != nil {
				fval, _ := fastparse.ParseFloat64(v.RawStr())
				if fval != math.Trunc(fval) || fval < 0 {
					return dst, false, evalengine.ErrHashCoercionIsNotExact
				}
				u, err = uint64(fval), nil
			}
		default:
			return dst, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
		}
		if err != nil {
			return dst, false, err
		}
		return binary.BigEndian.AppendUint64(dst, u), true, nil

	case sqltypes.IsBinary(coerceTo.T):
		return append(dst, v.Raw()...), false, nil

	case sqltypes.IsText(coerceTo.T):
		coll := coerceTo.Collation.Get()
		if coll == nil {
			panic("cannot hash unsupported collation")
		}
		return coll.WeightString(dst, v.Raw(), 0), false, nil

	case sqltypes.IsDecimal(coerceTo.T):
		var dec decimal.Decimal
		switch {
		case v.IsIntegral() || v.IsDecimal():
			var err error
			dec, err = decimal.NewFromMySQL(v.Raw())
			if err != nil {
				return dst, false, err
			}
		case v.IsFloat():
			fval, err := v.ToFloat64()
			if err != nil {
				return dst, false, err
			}
			dec = decimal.NewFromFloat(fval)
		case v.IsText() || v.IsBinary():
			fval, _ := fastparse.ParseFloat64(v.RawStr())
			dec = decimal.NewFromFloat(fval)
		default:
			return dst, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a decimal: %v", v)
		}
		return dec.WeightString(dst, coerceTo.Length, coerceTo.Precision), true, nil
	case coerceTo.T == sqltypes.TypeJSON:
		j, err := json.NewFromSQL(v)
		if err != nil {
			return dst, false, err
		}
		return j.WeightString(dst), true, nil

	default:
		return dst, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
	}
}
