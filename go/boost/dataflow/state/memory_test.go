package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/sqltypes"
)

func TestRecordProcessing(t *testing.T) {
	state := NewMemoryState()
	defer func() {
		state.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	records := []boostpb.Record{
		boostpb.TestRow(1, "A").ToRecord(true),
		boostpb.TestRow(2, "B").ToRecord(true),
		boostpb.TestRow(3, "C").ToRecord(true),
		boostpb.TestRow(1, "A").ToRecord(false),
	}

	state.AddKey([]int{0}, boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar), nil)

	head := records[:3]
	state.ProcessRecords(&head, boostpb.TagInvalid)

	tail := records[3:]
	state.ProcessRecords(&tail, boostpb.TagInvalid)

	key := records[0].Row.ToValues()[0:1]
	res, ok := state.Lookup([]int{0}, boostpb.RowFromValues(key))
	require.True(t, ok)
	assert.Equal(t, 0, res.Len())

	for _, record := range records[1:3] {
		key := record.Row.ToValues()[0:1]
		res, ok := state.Lookup([]int{0}, boostpb.RowFromValues(key))
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
	schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar)

	state.AddKey([]int{0}, schema, nil)
	record := []boostpb.Record{boostpb.VitessRowToRecord(row, true)}
	state.ProcessRecords(&record, boostpb.TagInvalid)
	state.AddKey([]int{1}, schema, nil)

	key := boostpb.RowFromVitess(row[1:2])
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

	records := []boostpb.Record{
		boostpb.TestRow(1, "A").ToRecord(true),
		boostpb.TestRow(2, "B").ToRecord(true),
		boostpb.TestRow(2, "C").ToRecord(true),
		boostpb.TestRow(3, "C").ToRecord(true),
		boostpb.TestRow(1, "A").ToRecord(false),
	}

	state.AddKey([]int{0}, boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar), []boostpb.Tag{1})

	state.MarkFilled(boostpb.TestRow(2), 1)

	rows, ok := state.Lookup([]int{0}, boostpb.TestRow(2))
	require.True(t, ok)
	require.Equal(t, 0, rows.Len())

	state.ProcessRecords(&records, 1)
	rows, ok = state.Lookup([]int{0}, boostpb.TestRow(2))
	require.True(t, ok)
	require.Equal(t, 2, rows.Len())

	state.MarkHole(boostpb.TestRow(2), 1)
	rows, ok = state.Lookup([]int{0}, boostpb.TestRow(2))
	require.False(t, ok)
	require.Nil(t, rows)
}

func TestStress(t *testing.T) {
	state := NewMemoryState()
	defer func() {
		state.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar)
	state.AddKey([]int{0}, schema, nil)

	for i := 0; i < 8; i++ {
		records := []boostpb.Record{
			boostpb.TestRow(i, "1").ToRecord(true),
			boostpb.TestRow(i, "2").ToRecord(true),
			boostpb.TestRow(i, "3").ToRecord(true),
			boostpb.TestRow(i, "4").ToRecord(true),
		}
		state.ProcessRecords(&records, boostpb.TagInvalid)
	}

	for i := 0; i < 8; i++ {
		rows, ok := state.Lookup([]int{0}, boostpb.TestRow(i))
		if !ok {
			t.Fatalf("unexpected miss")
		}
		if rows.Len() != 4 {
			t.Fatalf("for n=%d rows.Len() = %d", i, rows.Len())
		}
	}
}
