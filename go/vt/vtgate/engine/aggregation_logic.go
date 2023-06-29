package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type (
	AggrLogic interface {
		IsDistinct() bool
		Type(in *sqltypes.Type) (sqltypes.Type, bool)
		FirstValueInGroup(in sqltypes.Value, expected sqltypes.Type) sqltypes.Value
		AddToGroup(acc, val sqltypes.Value, expected querypb.Type) (sqltypes.Value, error)
		EmptyResult() sqltypes.Value
	}

	NullEmptyValue struct{}
	ZeroEmptyValue struct{}
	NotDistinct    struct{}
	Int64Typed     struct{}

	GroupConcat struct {
		NullEmptyValue
		NotDistinct
	}

	CountStar struct {
		NotDistinct
		ZeroEmptyValue
		Int64Typed
	}

	Count struct {
		Distinct bool
		ZeroEmptyValue
		Int64Typed
	}
)

var _ AggrLogic = GroupConcat{}
var _ AggrLogic = CountStar{}
var _ AggrLogic = Count{}

func (Int64Typed) Type(*sqltypes.Type) (sqltypes.Type, bool) {
	return sqltypes.Int64, true
}

func (NotDistinct) IsDistinct() bool {
	return false
}

func (NullEmptyValue) EmptyResult() sqltypes.Value {
	return sqltypes.NULL
}

func (ZeroEmptyValue) EmptyResult() sqltypes.Value {
	return intZero
}

func (GroupConcat) Type(in *sqltypes.Type) (sqltypes.Type, bool) {
	if in == nil {
		return sqltypes.Text, false
	}
	if sqltypes.IsBinary(*in) {
		return sqltypes.Blob, true
	}
	return sqltypes.Text, true
}

func (GroupConcat) FirstValueInGroup(in sqltypes.Value, expected sqltypes.Type) sqltypes.Value {
	if in.IsNull() {
		return in
	}
	trusted := sqltypes.MakeTrusted(expected, []byte(in.ToString()))
	return trusted
}

func (gc GroupConcat) AddToGroup(acc, val sqltypes.Value, expected querypb.Type) (sqltypes.Value, error) {
	if val.IsNull() {
		return acc, nil
	}

	if acc.IsNull() {
		return gc.FirstValueInGroup(val, expected), nil
	}
	concat := acc.ToString() + "," + val.ToString()
	trusted := sqltypes.MakeTrusted(expected, []byte(concat))
	return trusted, nil
}

func (c CountStar) FirstValueInGroup(sqltypes.Value, sqltypes.Type) sqltypes.Value {
	return intOne
}

func (c CountStar) AddToGroup(acc, _ sqltypes.Value, _ querypb.Type) (sqltypes.Value, error) {
	return evalengine.NullSafeAdd(acc, intOne, sqltypes.Int64)
}

func (c Count) IsDistinct() bool {
	return c.Distinct
}

func (Count) FirstValueInGroup(in sqltypes.Value, _ sqltypes.Type) sqltypes.Value {
	if in.IsNull() {
		return intZero
	}

	return intOne
}

func (Count) AddToGroup(acc, val sqltypes.Value, _ querypb.Type) (sqltypes.Value, error) {
	if val.IsNull() {
		return intZero, nil
	}

	return evalengine.NullSafeAdd(acc, val, sqltypes.Int64)
}
