package packet

import (
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/sql"
)

var _ FlowPacket = (*ReplayPiece)(nil)

func (r *ReplayPiece) GetSrc() dataflow.LocalNodeIdx {
	return r.Link.Src
}

func (r *ReplayPiece) GetDst() dataflow.LocalNodeIdx {
	return r.Link.Dst
}

func (r *ReplayPiece) GetLink() *Link {
	return r.Link
}

func (r *ReplayPiece) IsEmpty() bool {
	return len(r.Records) == 0
}

func (r *ReplayPiece) ReplaceRecords(fn func(r []sql.Record) []sql.Record) {
	r.Records = fn(r.Records)
}

func (r *ReplayPiece) TakeRecords() []sql.Record {
	rs := r.Records
	r.Records = nil
	return rs
}

func (r *ReplayPiece) BorrowRecords() []sql.Record {
	return r.Records
}

func (r *ReplayPiece) Clone() FlowPacket {
	return &ReplayPiece{
		Link:     r.Link.Clone(),
		Tag:      r.Tag,
		Records:  slices.Clone(r.Records),
		Context:  r.Context,
		External: r.External.Clone(),
	}
}

func (r *ReplayPiece) GetTag() dataflow.Tag {
	return r.Tag
}

func (m *ReplayPiece_External) GetGtid() string {
	if m == nil {
		return ""
	}
	return m.Gtid
}

func (m *ReplayPiece_External) Clone() *ReplayPiece_External {
	if m == nil {
		return nil
	}
	return &ReplayPiece_External{
		Gtid:         m.Gtid,
		Slot:         m.Slot,
		Intermediate: m.Intermediate,
		Failed:       m.Failed,
	}
}
