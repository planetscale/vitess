package processing

import (
	"bytes"
	"context"
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/sql"
)

type Executor interface {
	Send(ctx context.Context, dest dataflow.DomainAddr, pkt packet.FlowPacket) error
}

type Miss struct {
	// The node we missed when looking up into.
	On dataflow.LocalNodeIdx

	/// The columns of `on` we were looking up on.
	LookupIdx []int

	/// The columns of `record` we were using for the lookup.
	LookupCols []int

	/// The columns of `record` that identify the replay key (if any).
	ReplayCols []int

	/// The record we were processing when we missed.
	Record sql.HashedRecord

	/// Indicates whether this miss should force downstream evictions.
	Flush bool

	ForceTag dataflow.Tag
}

func (miss *Miss) Compare(other *Miss) int {
	if miss.On < other.On {
		return -1
	}
	if miss.On > other.On {
		return 1
	}
	if cmp := slices.Compare(miss.ReplayCols, other.ReplayCols); cmp != 0 {
		return cmp
	}
	if cmp := slices.Compare(miss.LookupIdx, other.LookupIdx); cmp != 0 {
		return cmp
	}
	if cmp := slices.Compare(miss.LookupCols, other.LookupCols); cmp != 0 {
		return cmp
	}
	if cmp := bytes.Compare(miss.Record.Hash[:], other.Record.Hash[:]); cmp != 0 {
		return cmp
	}
	return 0
}

func (miss *Miss) ReplayKey() sql.Row {
	if miss.ReplayCols == nil {
		return ""
	}
	row := sql.NewRowBuilder(len(miss.ReplayCols))
	for _, rc := range miss.ReplayCols {
		row.Add(miss.Record.Row.ValueAt(rc))
	}
	return row.Finish()
}

func (miss *Miss) LookupKey() sql.Row {
	row := sql.NewRowBuilder(len(miss.LookupCols))
	for _, rc := range miss.LookupCols {
		row.Add(miss.Record.Row.ValueAt(rc))
	}
	return row.Finish()
}

func (miss Miss) String() string {
	return fmt.Sprintf("miss {On: %v, LookupIdx: %v, LookupCols: %v, ReplayCols: %v, Record: %v}",
		miss.On, miss.LookupIdx, miss.LookupCols, miss.ReplayCols, miss.Record)
}

type Lookup struct {
	/// The node we looked up into.
	On dataflow.LocalNodeIdx

	/// The columns of `on` we were looking up on.
	Cols []int

	/// The key used for the lookup.
	Key sql.Row
}

type Result struct {
	Records []sql.Record
	Misses  []Miss
	Lookups []Lookup
}

func (*Result) rawResult() {}

type ReplayPiece struct {
	Rows     []sql.Record
	Keys     map[sql.Row]bool
	Captured map[sql.Row]bool
}

func (*ReplayPiece) rawResult() {}

type RawResult interface {
	rawResult()
}

type FullReplay struct {
	Records []sql.Record
	Last    bool
}

func (*FullReplay) rawResult() {}

type CapturedFull struct{}

func (*CapturedFull) rawResult() {}
