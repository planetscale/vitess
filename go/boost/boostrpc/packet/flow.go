package packet

import (
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/sql"
)

type FlowPacket interface {
	AsyncPacket

	GetSrc() dataflow.LocalNodeIdx
	GetDst() dataflow.LocalNodeIdx
	GetLink() *Link
	IsEmpty() bool
	ReplaceRecords(fn func(r []sql.Record) []sql.Record)
	TakeRecords() []sql.Record
	BorrowRecords() []sql.Record
	Clone() FlowPacket
	GetTag() dataflow.Tag
}

type ActiveFlowPacket struct {
	Inner FlowPacket
}

func (pkt *ActiveFlowPacket) Src() dataflow.LocalNodeIdx {
	return pkt.Inner.GetSrc()
}

func (pkt *ActiveFlowPacket) Dst() dataflow.LocalNodeIdx {
	return pkt.Inner.GetDst()
}

func (pkt *ActiveFlowPacket) Link() *Link {
	return pkt.Inner.GetLink()
}

func (pkt *ActiveFlowPacket) IsEmpty() bool {
	return pkt.Inner.IsEmpty()
}

func (pkt *ActiveFlowPacket) ReplaceRecords(fn func(r []sql.Record) []sql.Record) {
	pkt.Inner.ReplaceRecords(fn)
}

func (pkt *ActiveFlowPacket) FilterRecords(keep func(r sql.Record) bool) {
	pkt.Inner.ReplaceRecords(func(rs []sql.Record) []sql.Record {
		return sql.FilterRecords(rs, keep)
	})
}

func (pkt *ActiveFlowPacket) TakeRecords() []sql.Record {
	return pkt.Inner.TakeRecords()
}

func (pkt *ActiveFlowPacket) BorrowRecords() []sql.Record {
	if pkt == nil {
		return nil
	}
	return pkt.Inner.BorrowRecords()
}

func (pkt *ActiveFlowPacket) Clone() ActiveFlowPacket {
	return ActiveFlowPacket{Inner: pkt.Inner.Clone()}
}

func (pkt *ActiveFlowPacket) Take() ActiveFlowPacket {
	data := pkt.Inner
	pkt.Inner = nil
	return ActiveFlowPacket{Inner: data}
}

func (pkt *ActiveFlowPacket) Tag() dataflow.Tag {
	return pkt.Inner.GetTag()
}

func (pkt *ActiveFlowPacket) GetMessage() *Message {
	v, _ := pkt.Inner.(*Message)
	return v
}

func (pkt *ActiveFlowPacket) GetReplayPiece() *ReplayPiece {
	v, _ := pkt.Inner.(*ReplayPiece)
	return v
}

func (pkt *ActiveFlowPacket) GetInput() *Input {
	v, _ := pkt.Inner.(*Input)
	return v
}

func (pkt *ActiveFlowPacket) Clear() {
	pkt.Inner = nil
}

func (l *Link) Clone() *Link {
	return &Link{Src: l.Src, Dst: l.Dst}
}
