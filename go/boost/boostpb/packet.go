package boostpb

import (
	"encoding/json"
	"fmt"
	"strings"

	"golang.org/x/exp/slices"
)

func (pkt *Packet) Src() LocalNodeIndex {
	switch pkt := pkt.Inner.(type) {
	case *Packet_Input_:
		// inputs come "from" the base table too
		return pkt.Input.Inner.Dst
	case *Packet_Message_:
		return pkt.Message.Link.Src
	case *Packet_Vstream_:
		return pkt.Vstream.Link.Src
	case *Packet_ReplayPiece_:
		return pkt.ReplayPiece.Link.Src
	}
	panic("unreachable")
}

func (pkt *Packet) Dst() LocalNodeIndex {
	switch pkt := pkt.Inner.(type) {
	case *Packet_Input_:
		return pkt.Input.Inner.Dst
	case *Packet_Message_:
		return pkt.Message.Link.Dst
	case *Packet_Vstream_:
		return pkt.Vstream.Link.Dst
	case *Packet_ReplayPiece_:
		return pkt.ReplayPiece.Link.Dst
	}
	panic("unreachable")
}

func (pkt *Packet) Link() *Link {
	switch pkt := pkt.Inner.(type) {
	case *Packet_Message_:
		return pkt.Message.Link
	case *Packet_ReplayPiece_:
		return pkt.ReplayPiece.Link
	case *Packet_EvictKeys_:
		return pkt.EvictKeys.Link
	}
	panic("unreachable")
}

func (pkt *Packet) IsEmpty() bool {
	switch pkt := pkt.Inner.(type) {
	case *Packet_Message_:
		return len(pkt.Message.Records) == 0
	case *Packet_ReplayPiece_:
		return len(pkt.ReplayPiece.Records) == 0
	}
	panic("unreachable")
}

func (pkt *Packet) MapData(callback func(records *[]Record)) {
	switch pkt := pkt.Inner.(type) {
	case *Packet_Message_:
		callback(&pkt.Message.Records)
	case *Packet_ReplayPiece_:
		callback(&pkt.ReplayPiece.Records)
	default:
		panic("unreachable")
	}
}

func (pkt *Packet) TakeData() []Record {
	switch pkt := pkt.Inner.(type) {
	case *Packet_Message_:
		data := pkt.Message.Records
		pkt.Message.Records = nil
		return data
	case *Packet_ReplayPiece_:
		data := pkt.ReplayPiece.Records
		pkt.ReplayPiece.Records = nil
		return data
	default:
		panic("unreachable")
	}
}

func (pkt *Packet) BorrowData() []Record {
	if pkt == nil {
		return nil
	}
	switch pkt := pkt.Inner.(type) {
	case *Packet_Message_:
		return pkt.Message.Records
	case *Packet_ReplayPiece_:
		return pkt.ReplayPiece.Records
	default:
		panic("unreachable")
	}
}

func (pkt *Packet) CloneData() *Packet {
	var clone Packet
	switch pkt := pkt.Inner.(type) {
	case *Packet_Message_:
		link := *pkt.Message.Link
		clone.Inner = &Packet_Message_{
			Message: &Packet_Message{
				Link:    &link,
				Records: slices.Clone(pkt.Message.Records),
				Ignored: pkt.Message.Ignored,
			},
		}
	case *Packet_ReplayPiece_:
		link := *pkt.ReplayPiece.Link
		clone.Inner = &Packet_ReplayPiece_{
			ReplayPiece: &Packet_ReplayPiece{
				Link:    &link,
				Tag:     pkt.ReplayPiece.Tag,
				Records: slices.Clone(pkt.ReplayPiece.Records),
				Context: pkt.ReplayPiece.Context,
				Gtid:    pkt.ReplayPiece.Gtid,
			},
		}
	default:
		panic("unreachable")
	}
	return &clone
}

func (pkt *Packet) Tag() Tag {
	switch pkt := pkt.Inner.(type) {
	case *Packet_ReplayPiece_:
		return pkt.ReplayPiece.Tag
	case *Packet_EvictKeys_:
		return pkt.EvictKeys.Tag
	default:
		return TagNone
	}
}

type DebugRecords []Record

func (records DebugRecords) parts() []string {
	var out []string
	var remainder int
	if len(records) > 10 {
		remainder = len(records) - 10
		records = records[:10]
	}
	for _, record := range records {
		if record.Positive {
			out = append(out, "+"+record.Row.String())
		} else {
			out = append(out, "-"+record.Row.String())
		}
	}
	if remainder > 0 {
		out = append(out, fmt.Sprintf("[...] +%d records", remainder))
	}
	return out
}

func (records DebugRecords) MarshalJSON() ([]byte, error) {
	return json.Marshal(records.parts())
}

func (records DebugRecords) String() string {
	return strings.Join(records.parts(), ", ")
}

type DebugRows []Row

func (rows DebugRows) parts() []string {
	var out []string
	var remainder int
	if len(rows) > 10 {
		remainder = len(rows) - 10
		rows = rows[:10]
	}
	for _, r := range rows {
		out = append(out, r.String())
	}
	if remainder > 0 {
		out = append(out, fmt.Sprintf("[...] +%d rows", remainder))
	}
	return out
}

func (rows DebugRows) MarshalJSON() ([]byte, error) {
	return json.Marshal(rows.parts())

}

func (rows DebugRows) String() string {
	return strings.Join(rows.parts(), ", ")

}

func (pkt *SyncPacket) Trace() any {
	if pkt == nil {
		return nil
	}

	switch pkt := pkt.Inner.(type) {
	case *SyncPacket_Ready_:
		return struct {
			Packet string
			Node   LocalNodeIndex
			Purge  bool
		}{
			"Ready",
			pkt.Ready.Node,
			pkt.Ready.Purge,
		}
	case *SyncPacket_SetupReplayPath_:
		return struct {
			Packet string
			From   LocalNodeIndex
			Tag    Tag
			Path   []*ReplayPathSegment
		}{
			"SetupReplayPath",
			pkt.SetupReplayPath.Source,
			pkt.SetupReplayPath.Tag,
			pkt.SetupReplayPath.Path,
		}
	default:
		panic(fmt.Sprintf("unsupported debug output for %T", pkt))
	}
}

func (pkt *Packet) Trace() any {
	if pkt == nil {
		return nil
	}

	switch pkt := pkt.Inner.(type) {
	case *Packet_Message_:
		return struct {
			Packet  string
			From    LocalNodeIndex
			To      LocalNodeIndex
			Records DebugRecords
		}{
			"Message",
			pkt.Message.Link.Src,
			pkt.Message.Link.Dst,
			pkt.Message.Records,
		}
	case *Packet_ReplayPiece_:
		return struct {
			Packet  string
			From    LocalNodeIndex
			To      LocalNodeIndex
			Tag     Tag
			Records DebugRecords
		}{
			"ReplayPiece",
			pkt.ReplayPiece.Link.Src,
			pkt.ReplayPiece.Link.Dst,
			pkt.ReplayPiece.Tag,
			pkt.ReplayPiece.Records,
		}
	case *Packet_Input_:
		return struct {
			Packet string
			From   LocalNodeIndex
			To     LocalNodeIndex
			Data   []*TableOperation
		}{
			"Input",
			pkt.Input.Inner.Dst,
			pkt.Input.Inner.Dst,
			pkt.Input.Inner.Data,
		}
	case *Packet_UpdateEgress_:
		return struct {
			Packet string
			Node   LocalNodeIndex
			NewTx  *Packet_UpdateEgress_Tx
			Tag    *Packet_UpdateEgress_Tag
		}{
			"UpdateEgress",
			pkt.UpdateEgress.Node,
			pkt.UpdateEgress.NewTx,
			pkt.UpdateEgress.NewTag,
		}
	case *Packet_PrepareState_:
		return struct {
			Packet string
			Node   LocalNodeIndex
		}{
			"PrepareState",
			pkt.PrepareState.Node,
		}
	case *Packet_RequestReaderReplay_:
		return struct {
			Packet  string
			Node    LocalNodeIndex
			Columns []int
			Keys    DebugRows
		}{
			"RequestReaderReplay",
			pkt.RequestReaderReplay.Node,
			pkt.RequestReaderReplay.Cols,
			pkt.RequestReaderReplay.Keys,
		}
	case *Packet_RequestPartialReplay_:
		return struct {
			Packet          string
			RequestingShard uint
			Keys            DebugRows
			Tag             Tag
			Unishard        bool
		}{
			"RequestPartialReplay",
			pkt.RequestPartialReplay.RequestingShard,
			pkt.RequestPartialReplay.Keys,
			pkt.RequestPartialReplay.Tag,
			pkt.RequestPartialReplay.Unishard,
		}
	case *Packet_EvictKeys_:
		return struct {
			Packet string
			From   LocalNodeIndex
			To     LocalNodeIndex
			Tag    Tag
			Keys   DebugRows
		}{
			"EvictKeys",
			pkt.EvictKeys.Link.Src,
			pkt.EvictKeys.Link.Dst,
			pkt.EvictKeys.Tag,
			pkt.EvictKeys.Keys,
		}
	case *Packet_StartReplay_:
		return struct {
			Packet string
			From   LocalNodeIndex
			Tag    Tag
		}{
			"StartReplay",
			pkt.StartReplay.From,
			pkt.StartReplay.Tag,
		}
	case *Packet_Finish_:
		return struct {
			Packet string
			Node   LocalNodeIndex
			Tag    Tag
		}{
			"Finish",
			pkt.Finish.Node,
			pkt.Finish.Tag,
		}
	default:
		panic(fmt.Sprintf("unsupported debug output for %T", pkt))
	}
}
