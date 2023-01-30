package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
)

func TestRecordProcessing(t *testing.T) {
	state := NewMemoryState()
	defer func() {
		state.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	records := []sql.Record{
		sql.TestRow(1, "A").ToRecord(true),
		sql.TestRow(2, "B").ToRecord(true),
		sql.TestRow(3, "C").ToRecord(true),
		sql.TestRow(1, "A").ToRecord(false),
	}

	state.AddKey([]int{0}, sql.TestSchema(sqltypes.Int64, sqltypes.VarChar), nil, false)

	head := records[:3]
	state.ProcessRecords(head, dataflow.TagNone, nil)

	tail := records[3:]
	state.ProcessRecords(tail, dataflow.TagNone, nil)

	key := records[0].Row.ToValues()[0:1]
	res, ok := state.Lookup([]int{0}, sql.RowFromValues(key))
	require.True(t, ok)
	assert.Equal(t, 0, res.Len())

	for _, record := range records[1:3] {
		key := record.Row.ToValues()[0:1]
		res, ok := state.Lookup([]int{0}, sql.RowFromValues(key))
		require.True(t, ok)
		assert.Equal(t, record.Row, res.Collect(nil)[0])
	}
}

func TestNewIndexForOldRecords(t *testing.T) {
	state := NewMemoryState()
	defer func() {
		state.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	row := sqltypes.Row{sqltypes.NewInt64(10), sqltypes.NewVarChar("Cat")}
	schema := sql.TestSchema(sqltypes.Int64, sqltypes.VarChar)

	state.AddKey([]int{0}, schema, nil, false)
	record := []sql.Record{sql.VitessRowToRecord(row, true)}
	state.ProcessRecords(record, dataflow.TagNone, nil)
	state.AddKey([]int{1}, schema, nil, false)

	key := sql.RowFromVitess(row[1:2])
	res, ok := state.Lookup([]int{1}, key)
	assert.True(t, ok)
	assert.Equal(t, row, res.Collect(nil)[0].ToVitess())
}

func TestMemoryLeaks(t *testing.T) {
	state := NewMemoryState()
	defer func() {
		state.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	records := []sql.Record{
		sql.TestRow(1, "A").ToRecord(true),
		sql.TestRow(2, "B").ToRecord(true),
		sql.TestRow(2, "C").ToRecord(true),
		sql.TestRow(3, "C").ToRecord(true),
		sql.TestRow(1, "A").ToRecord(false),
	}

	state.AddKey([]int{0}, sql.TestSchema(sqltypes.Int64, sqltypes.VarChar), []dataflow.Tag{1}, false)

	state.MarkFilled(sql.TestRow(2), 1)

	rows, ok := state.Lookup([]int{0}, sql.TestRow(2))
	require.True(t, ok)
	require.Equal(t, 0, rows.Len())

	state.ProcessRecords(records, 1, nil)
	rows, ok = state.Lookup([]int{0}, sql.TestRow(2))
	require.True(t, ok)
	require.Equal(t, 2, rows.Len())

	state.MarkHole(sql.TestRow(2), 1)
	rows, ok = state.Lookup([]int{0}, sql.TestRow(2))
	require.False(t, ok)
	require.Nil(t, rows)
}

func TestStress(t *testing.T) {
	state := NewMemoryState()
	defer func() {
		state.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	schema := sql.TestSchema(sqltypes.Int64, sqltypes.VarChar)
	state.AddKey([]int{0}, schema, nil, false)

	for i := 0; i < 8; i++ {
		records := []sql.Record{
			sql.TestRow(i, "1").ToRecord(true),
			sql.TestRow(i, "2").ToRecord(true),
			sql.TestRow(i, "3").ToRecord(true),
			sql.TestRow(i, "4").ToRecord(true),
		}
		state.ProcessRecords(records, dataflow.TagNone, nil)
	}

	for i := 0; i < 8; i++ {
		rows, ok := state.Lookup([]int{0}, sql.TestRow(i))
		if !ok {
			t.Fatalf("unexpected miss")
		}
		if rows.Len() != 4 {
			t.Fatalf("for n=%d rows.Len() = %d", i, rows.Len())
		}
	}
}

func TestOverlapKeymap(t *testing.T) {
	cases := []struct {
		short, long []int
		match       []int
	}{
		{[]int{0, 1}, []int{1}, nil},
		{[]int{1}, []int{0, 1}, []int{1}},
		{[]int{0}, []int{0, 1}, []int{0}},
		{[]int{1}, []int{1, 0}, []int{0}},
		{[]int{0, 1}, []int{0, 1}, []int{0, 1}},
		{[]int{0, 1, 2}, []int{0, 1}, nil},
		{[]int{0, 1}, []int{0, 1, 2}, []int{0, 1}},
		{[]int{1, 2}, []int{0, 1, 2}, []int{1, 2}},
		{[]int{1, 0}, []int{0, 1, 2}, []int{1, 0}},
		{[]int{1, 2}, []int{0, 3, 1, 2}, []int{2, 3}},
		{[]int{1, 3}, []int{0, 1, 2}, nil},
	}

	for _, tc := range cases {
		require.Equalf(t, tc.match, overlapKeymap(tc.short, tc.long), "overlap(%v, %v)", tc.short, tc.long)
	}
}
