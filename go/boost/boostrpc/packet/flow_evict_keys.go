package packet

import (
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/sql"
)

var _ FlowPacket = (*EvictKeysRequest)(nil)

func (e *EvictKeysRequest) GetSrc() dataflow.LocalNodeIdx {
	return e.Link.Src
}

func (e *EvictKeysRequest) GetDst() dataflow.LocalNodeIdx {
	return e.Link.Dst
}

func (e *EvictKeysRequest) GetLink() *Link {
	return e.Link
}

func (e *EvictKeysRequest) IsEmpty() bool {
	return true
}

func (e *EvictKeysRequest) ReplaceRecords(_ func(records []sql.Record) []sql.Record) {
}

func (e *EvictKeysRequest) TakeRecords() []sql.Record {
	return nil
}

func (e *EvictKeysRequest) BorrowRecords() []sql.Record {
	return nil
}

func (e *EvictKeysRequest) Clone() FlowPacket {
	return e
}

func (e *EvictKeysRequest) GetTag() dataflow.Tag {
	return e.Tag
}
