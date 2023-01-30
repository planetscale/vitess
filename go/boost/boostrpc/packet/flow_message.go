package packet

import (
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/sql"
)

var _ FlowPacket = (*Message)(nil)

func (m *Message) GetSrc() dataflow.LocalNodeIdx {
	return m.Link.Src
}

func (m *Message) GetDst() dataflow.LocalNodeIdx {
	return m.Link.Dst
}

func (m *Message) GetLink() *Link {
	return m.Link
}

func (m *Message) IsEmpty() bool {
	return len(m.Records) == 0
}

func (m *Message) ReplaceRecords(fn func(r []sql.Record) []sql.Record) {
	m.Records = fn(m.Records)
}

func (m *Message) TakeRecords() []sql.Record {
	rs := m.Records
	m.Records = nil
	m.SeenTags = nil
	return rs
}

func (m *Message) BorrowRecords() []sql.Record {
	return m.Records
}

func (m *Message) Clone() FlowPacket {
	return &Message{
		Link:     m.Link.Clone(),
		Records:  slices.Clone(m.Records),
		Gtid:     m.Gtid,
		SeenTags: slices.Clone(m.SeenTags),
	}
}

func (m *Message) GetTag() dataflow.Tag {
	return dataflow.TagNone
}
