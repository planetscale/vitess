package sql

import (
	"fmt"
	"unsafe"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vthash"
)

type Value string

type Row string

func (r Row) Len() int {
	bytelen := uint16(len(r))
	if bytelen == 0 {
		return 0
	}

	s := string(r)
	rowlen := 0
	for getUint16s(s) < bytelen {
		s = s[2:]
		rowlen++
	}
	return rowlen
}

func (r Row) String() string {
	return fmt.Sprintf("%v", r.ToVitess())
}

func (r Row) GoString() string {
	return fmt.Sprintf("Row%v", r.ToVitess())
}

func (r Row) IndexWith(cols []int) Row {
	var idxrow = make([]Value, 0, len(cols))
	for _, c := range cols {
		idxrow = append(idxrow, r.ValueAt(c))
	}
	return RowFromValues(idxrow)
}

func (r Row) ValueAt(i int) Value {
	start := getUint16s(string(r)[i*2:])
	end := getUint16s(string(r)[(i+1)*2:])
	return Value(string(r)[start:end])
}

func (r Row) Truncate(colLen int) Row {
	if colLen == 0 {
		return r
	}
	builder := NewRowBuilder(colLen)
	for i := 0; i < colLen; i++ {
		builder.Add(r.ValueAt(i))
	}
	return builder.Finish()
}

func getUint16s(b string) uint16 {
	_ = b[1] // bounds check hint to compiler; see golang.org/issue/14808
	return uint16(b[0]) | uint16(b[1])<<8
}

func (v Value) Type() sqltypes.Type {
	return sqltypes.Type(getUint16s(string(v)))
}

func (v Value) RawStr() string {
	return string(v)[2:]
}

func (v Value) RawBytes() []byte {
	return hack.StringBytes(string(v)[2:])
}

func (v Value) ToVitess() sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Type(getUint16s(string(v))), []byte(v[2:]))
}

func (v Value) ToVitessUnsafe() sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Type(getUint16s(string(v))), hack.StringBytes(string(v[2:])))
}

func (r Row) ToVitess() (out sqltypes.Row) {
	bytelen := uint16(len(r))
	if bytelen == 0 {
		return nil
	}

	s := string(r)
	last := getUint16s(s)
	s = s[2:]

	for last < bytelen {
		pos := getUint16s(s)
		s = s[2:]

		tt := getUint16s(string(r)[last:])
		vv := []byte(r[last+2 : pos])
		out = append(out, sqltypes.MakeTrusted(sqltypes.Type(tt), vv))

		last = pos
	}
	return
}

func (r Row) ToVitessTruncate(length int) (out sqltypes.Row) {
	bytelen := uint16(len(r))
	if bytelen == 0 {
		return nil
	}

	s := string(r)
	last := getUint16s(s)
	s = s[2:]

	for col := 0; col < length && last < bytelen; col++ {
		pos := getUint16s(s)
		s = s[2:]

		tt := getUint16s(string(r)[last:])
		vv := []byte(r[last+2 : pos])
		out = append(out, sqltypes.MakeTrusted(sqltypes.Type(tt), vv))

		last = pos
	}
	return
}

func (r Row) ToValues() (out []Value) {
	bytelen := uint16(len(r))
	if bytelen == 0 {
		return nil
	}

	s := string(r)
	last := getUint16s(s)
	s = s[2:]

	for last < bytelen {
		pos := getUint16s(s)
		s = s[2:]
		out = append(out, Value(r[last:pos]))
		last = pos
	}
	return
}

func ValuesFromVitess(in []sqltypes.Value) (out []Value) {
	if in == nil {
		return nil
	}
	out = make([]Value, 0, len(in))
	for _, v := range in {
		out = append(out, ValueFromVitess(v))
	}
	return
}

func MakeValue(t sqltypes.Type, valuefn func(buf []byte) []byte) Value {
	var raw = make([]byte, 0, 16)
	var tt = uint16(t)

	raw = append(raw, byte(tt), byte(tt>>8))
	raw = valuefn(raw)
	return Value(*(*string)(unsafe.Pointer(&raw)))
}

func ValueFromVitess(v sqltypes.Value) Value {
	tt := uint16(v.Type())
	vv := v.Raw()

	var raw = make([]byte, 0, len(vv)+2)
	raw = append(raw, byte(tt), byte(tt>>8))
	raw = append(raw, vv...)
	return Value(*(*string)(unsafe.Pointer(&raw)))
}

func appendRow(bytes []byte, row []Value) []byte {
	written := len(row)*2 + 2
	for _, v := range row {
		bytes = append(bytes, byte(written), byte(written>>8))
		written += len(v)
	}
	bytes = append(bytes, byte(written), byte(written>>8))
	for _, v := range row {
		bytes = append(bytes, v...)
	}
	return bytes
}

func appendRowFromVitess(bytes []byte, row sqltypes.Row) []byte {
	written := len(row)*2 + 2
	for _, v := range row {
		bytes = append(bytes, byte(written), byte(written>>8))
		written += len(v.Raw()) + 2
	}
	bytes = append(bytes, byte(written), byte(written>>8))
	for _, v := range row {
		tt := uint16(v.Type())
		rr := v.Raw()
		bytes = append(bytes, byte(tt), byte(tt>>8))
		bytes = append(bytes, rr...)
	}
	return bytes
}

func RowFromVitess(row sqltypes.Row) Row {
	bytes := appendRowFromVitess(nil, row)
	return Row(*(*string)(unsafe.Pointer(&bytes)))
}

func RowFromValues(vs []Value) Row {
	bytes := appendRow(nil, vs)
	return Row(*(*string)(unsafe.Pointer(&bytes)))
}

func NewRecord(vs []Value, sign bool) Record {
	return Record{Row: RowFromValues(vs), Positive: sign, Offset: -1}
}

func (r Row) ToRecord(sign bool) Record {
	return Record{Row: r, Positive: sign, Offset: -1}
}

func (r Row) ToOffsetRecord(offset int32, sign bool) Record {
	return Record{Row: r, Offset: offset, Positive: sign}
}

func (v Value) Cmp(v2 Value) int {
	if v == v2 {
		return 0
	}
	cmp, _ := evalengine.NullsafeCompare(v.ToVitessUnsafe(), v2.ToVitessUnsafe(), collations.CollationUtf8mb4ID)
	return cmp
}

func RowsEqual(a, b Row) bool {
	if a == "" || b == "" {
		return a == "" && b == ""
	}
	lenA := a.Len()
	lenB := b.Len()
	if lenA != lenB {
		return false
	}
	for i := 0; i < lenA; i++ {
		if a.ValueAt(i).Cmp(b.ValueAt(i)) != 0 {
			return false
		}
	}
	return true
}

func (r Row) Hash(h *vthash.Hasher, schema []Type) vthash.Hash {
	hash, _ := r.HashExact(h, schema)
	return hash
}

func (r Row) HashExact(h *vthash.Hasher, schema []Type) (vthash.Hash, bool) {
	h.Reset()
	var col int

	bytelen := uint16(len(r))
	if bytelen == 0 {
		return h.Sum128(), true
	}

	s := string(r)
	last := getUint16s(s)
	s = s[2:]

	for last < bytelen {
		pos := getUint16s(s)
		s = s[2:]

		tt := sqltypes.Type(getUint16s(string(r)[last:]))
		vv := []byte(r[last+2 : pos])
		if !evalengine.NullsafeHashCode128(h, sqltypes.MakeTrusted(tt, vv), schema[col].Collation, schema[col].T) {
			return vthash.Hash{}, false
		}

		last = pos
		col++
	}
	return h.Sum128(), true
}

func (r Row) HashValue(h *vthash.Hasher, valpos int, valtype Type) vthash.Hash {
	h.Reset()
	vv := r.ValueAt(valpos).ToVitessUnsafe()
	evalengine.NullsafeHashCode128(h, vv, valtype.Collation, valtype.T)
	return h.Sum128()
}

func (r Row) HashWithKey(h *vthash.Hasher, key []int, schema []Type) vthash.Hash {
	h.Reset()
	for _, col := range key {
		vv := r.ValueAt(col).ToVitessUnsafe()
		ss := &schema[col]
		evalengine.NullsafeHashCode128(h, vv, ss.Collation, ss.T)
	}
	return h.Sum128()
}

func (r Row) HashWithKeySchema(h *vthash.Hasher, key []int, schema []Type) vthash.Hash {
	h.Reset()
	for i, col := range key {
		vv := r.ValueAt(col).ToVitessUnsafe()
		evalengine.NullsafeHashCode128(h, vv, schema[i].Collation, schema[i].T)
	}
	return h.Sum128()
}

func (r Row) ShardValue(h *vthash.Hasher, valpos int, valtype Type, shards uint) uint {
	h.Reset()
	vv := r.ValueAt(valpos).ToVitessUnsafe()
	evalengine.NullsafeHashCode128(h, vv, valtype.Collation, valtype.T)
	return uint(h.Sum64()) % shards
}

func (r Row) Extract(index []int) Row {
	var rb = NewRowBuilder(len(index))
	for _, col := range index {
		rb.Add(r.ValueAt(col))
	}
	return rb.Finish()
}

func (r Row) Slice(from, to int) Row {
	// TODO: this is not efficient
	chunks := r.ToValues()[from:to]
	return RowFromValues(chunks)
}

func (r Row) AsRecord() Record {
	return Record{Row: r, Positive: true, Offset: -1}
}

func (r Row) AsRecords() []Record {
	return []Record{r.AsRecord()}
}

func (r Row) Zap(field string) zapcore.Field {
	return zap.String(field, fmt.Sprintf("%v", r.ToVitess()))
}

type HashedRecord struct {
	Record
	Hash vthash.Hash
}

func (r Record) GoString() string {
	if r.Positive {
		return fmt.Sprintf("<+>%s", r.Row.String())
	} else {
		return fmt.Sprintf("<->%s", r.Row.String())
	}
}

func (r Record) AsRecord() Record {
	return r
}

func (r Record) AsRecords() []Record {
	return []Record{r}
}

func FilterRecords(rs []Record, keep func(r Record) bool) []Record {
	var j int
	for _, r := range rs {
		if keep(r) {
			rs[j] = r
			j++
		}
	}
	if j == 0 {
		return nil
	}
	return rs[:j]
}

func OffsetRecords(rs []Record) []Record {
	for i := range rs {
		rs[i].Offset = int32(i)
	}
	return rs
}

func TestRow(cols ...interface{}) Row {
	var rb = NewRowBuilder(len(cols))
	for _, c := range cols {
		var v sqltypes.Value

		switch goval := c.(type) {
		case nil:
			v = sqltypes.NULL
		case []byte:
			v = sqltypes.MakeTrusted(sqltypes.VarBinary, goval)
		case int64:
			v = sqltypes.NewInt64(goval)
		case int:
			v = sqltypes.NewInt64(int64(goval))
		case uint64:
			v = sqltypes.NewUint64(goval)
		case float64:
			v = sqltypes.NewFloat64(goval)
		case string:
			v = sqltypes.NewVarChar(goval)
		default:
			panic(fmt.Errorf("unexpected type %T: %v", goval, goval))
		}
		rb.AddVitess(v)
	}
	return rb.Finish()
}

var NULL = ValueFromVitess(sqltypes.NULL)

func VitessRowToRecord(row []sqltypes.Value, sign bool) Record {
	return RowFromVitess(row).ToRecord(sign)
}

type RowBuilder struct {
	row  []byte
	idx  int
	size int
}

func NewRowBuilder(size int) RowBuilder {
	return RowBuilder{
		row:  make([]byte, (size+1)*2),
		idx:  0,
		size: size,
	}
}

func ZapRows(name string, rows []Row) zap.Field {
	rs := make([]string, 0, len(rows))
	for _, r := range rows {
		rs = append(rs, fmt.Sprintf("%v", r.ToVitess()))
	}
	return zap.Strings(name, rs)
}

func (rb *RowBuilder) Add(v Value) {
	written := uint16(len(rb.row))
	rb.row[rb.idx] = byte(written)
	rb.row[rb.idx+1] = byte(written >> 8)
	rb.row = append(rb.row, v...)
	rb.idx += 2
}

func (rb *RowBuilder) AddVitess(v sqltypes.Value) {
	written := uint16(len(rb.row))
	rb.row[rb.idx] = byte(written)
	rb.row[rb.idx+1] = byte(written >> 8)

	tt := uint16(v.Type())
	rb.row = append(rb.row, byte(tt), byte(tt>>8))
	rb.row = append(rb.row, v.Raw()...)

	rb.idx += 2
}

func (rb *RowBuilder) Finish() Row {
	written := uint16(len(rb.row))
	rb.row[rb.idx] = byte(written)
	rb.row[rb.idx+1] = byte(written >> 8)
	return Row(*(*string)(unsafe.Pointer(&rb.row)))
}
