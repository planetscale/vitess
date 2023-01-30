package packet

import (
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/sql"
)

var _ FlowPacket = (*Input)(nil)

func (i *Input) GetSrc() dataflow.LocalNodeIdx {
	// inputs come "from" the base table too
	return i.Inner.Dst
}

func (i *Input) GetDst() dataflow.LocalNodeIdx {
	return i.Inner.Dst
}

func (i *Input) GetLink() *Link {
	panic("cannot link Input packet")
}

func (i *Input) IsEmpty() bool {
	return len(i.Inner.Data) == 0
}

func (i *Input) ReplaceRecords(_ func(rs []sql.Record) []sql.Record) {
}

func (i *Input) TakeRecords() []sql.Record {
	return nil
}

func (i *Input) BorrowRecords() []sql.Record {
	return nil
}

func (i *Input) Clone() FlowPacket {
	return i
}

func (i *Input) GetTag() dataflow.Tag {
	return dataflow.TagNone
}
