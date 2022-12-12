package flownode

import (
	"context"
	"fmt"
	"unsafe"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vthash"
)

type ExternalBase struct {
	primaryKey []int
	schema     []boostpb.Type
	keySchema  []boostpb.Type
	keyspace   string
	table      string

	recents         map[vthash.Hash]mysql.Mysql56GTIDSet
	inflight        map[*inflight]struct{}
	activeUpqueries uint64
}

type inflight struct {
	active    uint64
	key       []int
	keySchema []boostpb.Type
	selected  map[vthash.Hash]struct{}

	buffer []buffered
}

type buffered struct {
	records []boostpb.Record
	gtid    mysql.Mysql56GTIDSet
}

func (base *ExternalBase) dataflow() {}

var _ NodeImpl = (*ExternalBase)(nil)
var _ AnyBase = (*ExternalBase)(nil)

func (base *ExternalBase) Schema() []boostpb.Type {
	return base.schema
}

func (base *ExternalBase) Process(ctx context.Context, m **boostpb.Packet) (*boostpb.Packet_Message_, error) {
	switch pkt := (*m).Inner.(type) {
	case *boostpb.Packet_Vstream_:
		if pkt.Vstream.Gtid == "" {
			panic("message on ExternalBase without assigned GTID")
		}
		rs := make([]boostpb.Record, 0, 2)
		if pkt.Vstream.Before != nil {
			rs = append(rs, *pkt.Vstream.Before)
		}
		if pkt.Vstream.After != nil {
			rs = append(rs, *pkt.Vstream.After)
		}
		if len(rs) == 0 {
			panic("vstream message contains no records")
		}
		ignored, err := base.ignore(rs, pkt.Vstream.Gtid)
		if err != nil {
			return nil, err
		}

		return &boostpb.Packet_Message_{
			Message: &boostpb.Packet_Message{
				Link:    pkt.Vstream.Link,
				Records: rs,
				Ignored: ignored,
			},
		}, nil

	default:
		panic(fmt.Sprintf("unexpected packet type %T on ExternalBase", (*m).Inner))
	}
}

func (base *ExternalBase) ignore(records []boostpb.Record, incomingGTID string) (bool, error) {
	var toBuffer map[*inflight][]boostpb.Record
	var hasher vthash.Hasher

	position, err := mysql.ParseMysql56GTIDSet(incomingGTID)
	if err != nil {
		return false, err
	}

	for _, r := range records {
		h := r.Row.HashWithKeySchema(&hasher, base.primaryKey, base.keySchema)
		if sequence, ok := base.recents[h]; ok {
			if sequence.Contains(position) {
				return true, nil
			}
		} else {
			for inf := range base.inflight {
				h := r.Row.HashWithKeySchema(&hasher, inf.key, inf.keySchema)
				if _, ok := inf.selected[h]; ok {
					if toBuffer == nil {
						toBuffer = make(map[*inflight][]boostpb.Record)
					}
					toBuffer[inf] = append(toBuffer[inf], r)
					return true, nil
				}
			}
		}
	}

	for inf, records := range toBuffer {
		inf.buffer = append(inf.buffer, buffered{records: records, gtid: position})
	}
	return false, nil
}

func (base *ExternalBase) filter(ctx context.Context, records []boostpb.Record, incomingGTID mysql.Mysql56GTIDSet) (filtered []boostpb.Record) {
	var log = common.Logger(ctx).With(zap.String("node", "ExternalBase"))
	var hasher vthash.Hasher
	var toBuffer map[*inflight][]boostpb.Record

	filtered = records[:0]

skipRecord:
	for _, r := range records {
		h := r.Row.HashWithKeySchema(&hasher, base.primaryKey, base.keySchema)
		if sequence, ok := base.recents[h]; ok {
			if sequence.Contains(incomingGTID) {
				if w := log.Check(zapcore.DebugLevel, "skipping duplicate values from stream"); w != nil {
					w.Write(zap.String("incoming_gtid", incomingGTID.String()), zap.String("sequence", sequence.String()))
				}
				continue skipRecord
			}
		} else if true {
			for inf := range base.inflight {
				h := r.Row.HashWithKeySchema(&hasher, inf.key, inf.keySchema)
				if _, ok := inf.selected[h]; ok {
					if w := log.Check(zapcore.DebugLevel, "removing possible collisions from stream"); w != nil {
						w.Write(zap.String("incoming_gtid", incomingGTID.String()),
							zap.Uintptr("inflight", uintptr(unsafe.Pointer(inf))),
							zap.Uint64("inflight_active", inf.active))
					}
					if toBuffer == nil {
						toBuffer = make(map[*inflight][]boostpb.Record)
					}
					toBuffer[inf] = append(toBuffer[inf], r)
					continue skipRecord
				}
			}
		}
		filtered = append(filtered, r)
	}

	for inf, records := range toBuffer {
		inf.buffer = append(inf.buffer, buffered{records: records, gtid: incomingGTID})
	}
	return
}

func (base *ExternalBase) Keyspace() string {
	return base.keyspace
}

func (base *ExternalBase) Table() string {
	return base.table
}

func (base *ExternalBase) BeginUpquery(ctx context.Context, slot uint8, keyed []int, keys map[boostpb.Row]bool) {
	if slot == 0 {
		panic("slot 0 should be reserved")
	}

	base.activeUpqueries |= uint64(1) << (slot - 1)

	var keySchema = make([]boostpb.Type, 0, len(keyed))
	for _, col := range keyed {
		keySchema = append(keySchema, base.schema[col])
	}

	var selected = make(map[vthash.Hash]struct{})
	var hasher vthash.Hasher
	for k := range keys {
		selected[k.Hash(&hasher, keySchema)] = struct{}{}
	}

	inf := &inflight{
		active:    base.activeUpqueries,
		key:       keyed,
		keySchema: keySchema,
		selected:  selected,
		buffer:    nil,
	}
	base.inflight[inf] = struct{}{}

	log := common.Logger(ctx).With(zap.String("node", "ExternalBase"))
	log.Debug("marked inflight query",
		zap.Int("inflight_count", len(base.inflight)),
		zap.Uint8("slot", slot),
		zap.Uintptr("inflight", uintptr(unsafe.Pointer(inf))),
		zap.Uint64("active", base.activeUpqueries),
	)
}

func (base *ExternalBase) FinishUpquery(ctx context.Context, m *boostpb.Packet_ReplayPiece) {
	log := common.Logger(ctx).With(zap.String("node", "ExternalBase"))
	log.Debug("FinishUpquery", zap.String("gtid", m.Gtid), zap.Int("inflight_count", len(base.inflight)))

	if len(m.Records) > 0 {
		if m.Gtid == "" {
			panic("replay from ExternalBase without GTID")
		}

		position, err := mysql.ParseMysql56GTIDSet(m.Gtid)
		if err != nil {
			log.Error("failed to parse gtid", zap.String("gtid", m.Gtid), zap.Error(err))
			return
		}

		var hasher vthash.Hasher
		for _, r := range m.Records {
			h := r.Row.HashWithKeySchema(&hasher, base.primaryKey, base.keySchema)
			base.recents[h] = position
		}
	}

	upqueryMask := uint64(1) << (m.UpquerySlot - 1)
	base.activeUpqueries &= ^upqueryMask

	for inf := range base.inflight {
		inf.active &= ^upqueryMask
		if inf.active == 0 {
			delete(base.inflight, inf)

			var total, reinserted int
			for _, b := range inf.buffer {
				filtered := base.filter(ctx, b.records, b.gtid)
				m.Records = append(m.Records, filtered...)

				total += len(b.records)
				reinserted += len(filtered)
			}

			log.Debug("inflight request finished; re-inserting collisions",
				zap.Uintptr("inflight", uintptr(unsafe.Pointer(inf))),
				zap.Int("total_records_inflight", total),
				zap.Int("reinserted_records", reinserted),
			)
		}
	}
}

func NewExternalBase(primaryKey []int, schema []boostpb.Type, keyspace, table string) *ExternalBase {
	if primaryKey == nil {
		panic("NewExternalBase without primary key")
	}
	if schema == nil {
		panic("NewExternalBase without schema")
	}

	var keySchema = make([]boostpb.Type, 0, len(primaryKey))
	for _, col := range primaryKey {
		keySchema = append(keySchema, schema[col])
	}

	return &ExternalBase{
		primaryKey: primaryKey,
		schema:     schema,
		keySchema:  keySchema,
		keyspace:   keyspace,
		table:      table,
		inflight:   map[*inflight]struct{}{},
		recents:    map[vthash.Hash]mysql.Mysql56GTIDSet{},
	}
}

func (base *ExternalBase) ToProto() *boostpb.Node_ExternalBase {
	pbase := &boostpb.Node_ExternalBase{
		PrimaryKey: base.primaryKey,
		Keyspace:   base.keyspace,
		Table:      base.table,
		Schema:     base.schema,
	}
	return pbase
}

func NewExternalBaseFromProto(base *boostpb.Node_ExternalBase) *ExternalBase {
	// TODO: handle defaultValues
	return NewExternalBase(base.PrimaryKey, base.Schema, base.Keyspace, base.Table)
}
