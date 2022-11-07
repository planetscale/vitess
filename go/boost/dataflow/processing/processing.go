package processing

import (
	"bytes"
	"context"
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
)

type Executor interface {
	Send(ctx context.Context, dest boostpb.DomainAddr, m *boostpb.Packet) error
}

type Miss struct {
	// The node we missed when looking up into.
	On boostpb.LocalNodeIndex

	/// The columns of `on` we were looking up on.
	LookupIdx []int

	/// The columns of `record` we were using for the lookup.
	LookupCols []int

	/// The columns of `record` that identify the replay key (if any).
	ReplayCols []int

	/// The record we were processing when we missed.
	Record boostpb.HashedRecord
}

func (miss *Miss) Compare(other *Miss) int {
	if miss.On < other.On {
		return -1
	}
	if miss.On > other.On {
		return 1
	}
	if cmp := slices.Compare(miss.LookupIdx, other.LookupIdx); cmp != 0 {
		return cmp
	}
	if cmp := slices.Compare(miss.LookupCols, other.LookupCols); cmp != 0 {
		return cmp
	}
	if cmp := slices.Compare(miss.ReplayCols, other.ReplayCols); cmp != 0 {
		return cmp
	}
	if cmp := bytes.Compare(miss.Record.Hash[:], other.Record.Hash[:]); cmp != 0 {
		return cmp
	}
	return 0
}

func (miss *Miss) ReplayKey() boostpb.Row {
	if miss.ReplayCols == nil {
		return ""
	}
	row := boostpb.NewRowBuilder(len(miss.ReplayCols))
	for _, rc := range miss.ReplayCols {
		row.Add(miss.Record.Row.ValueAt(rc))
	}
	return row.Finish()
}

func (miss *Miss) LookupKey() boostpb.Row {
	row := boostpb.NewRowBuilder(len(miss.LookupCols))
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
	On boostpb.LocalNodeIndex

	/// The columns of `on` we were looking up on.
	Cols []int

	/// The key used for the lookup.
	Key boostpb.Row
}

type Result struct {
	Records []boostpb.Record
	Misses  []Miss
	Lookups []Lookup
}

func (*Result) rawResult() {}

type ReplayPiece struct {
	Rows     []boostpb.Record
	Keys     map[boostpb.Row]bool
	Captured map[boostpb.Row]bool
}

func (*ReplayPiece) rawResult() {}

type RawResult interface {
	rawResult()
}

type FullReplay struct {
	Records []boostpb.Record
	Last    bool
}

func (*FullReplay) rawResult() {}

type CapturedFull struct{}

func (*CapturedFull) rawResult() {}
