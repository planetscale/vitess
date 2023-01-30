package flownode

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vthash"
)

type GtidTracker struct {
	fullParentSchema []sql.Type
	fullNodeSchema   []sql.Type
	paths            map[dataflow.Tag]*gtidPath

	inflight        map[*inflight]struct{}
	activeUpqueries uint64
}

// seenExpiry is the time we keep things in the recently seen
// GTID cache. This is a very long timeout to be on the safe
// side for now. We can tune this later when we better know
// actual memory usage of the recent GTID cache.
const seenExpiry = 1 * time.Hour

type gtidPath struct {
	nodeKey    []int
	nodeSchema []sql.Type

	parentKey    []int
	parentSchema []sql.Type

	recents     map[vthash.Hash]mysql.Mysql56GTIDSet
	recentQueue gtidQueue
}

type inflight struct {
	active   uint64
	selected map[vthash.Hash]struct{}
	buffered []*packet.Message
}

func NewGtidTracker(parentSchema []sql.Type, nodeSchema []sql.Type) *GtidTracker {
	return &GtidTracker{
		fullParentSchema: parentSchema,
		fullNodeSchema:   nodeSchema,
		inflight:         make(map[*inflight]struct{}),
		paths:            make(map[dataflow.Tag]*gtidPath),
	}
}

func (gt *GtidTracker) AddTag(self *Node, tag dataflow.Tag, key []int) error {
	if _, ok := gt.paths[tag]; ok {
		panic(fmt.Sprintf("duplicate GTID replay path for %d", tag))
	}

	var path = &gtidPath{
		nodeKey: key,
		recents: make(map[vthash.Hash]mysql.Mysql56GTIDSet),
	}

	path.nodeSchema = make([]sql.Type, 0, len(path.nodeKey))
	for _, col := range path.nodeKey {
		path.nodeSchema = append(path.nodeSchema, gt.fullNodeSchema[col])
	}

	if impl := self.AsInternal(); impl != nil {
		for _, k := range key {
			pcols := impl.ParentColumns(k)
			if len(pcols) != 1 {
				panic(fmt.Sprintf("parent columns length must be 1, got: %v", pcols))
			}
			if pcols[0].Column < 0 {
				panic(fmt.Sprintf("missing column in parent: %v", pcols))
			}
			path.parentKey = append(path.parentKey, pcols[0].Column)
		}
	} else {
		path.parentKey = key
	}

	path.parentSchema = make([]sql.Type, 0, len(path.parentKey))
	for _, col := range path.parentKey {
		path.parentSchema = append(path.parentSchema, gt.fullParentSchema[col])
	}

	gt.paths[tag] = path
	return nil
}

func (gt *GtidTracker) tagRecords(pkt *packet.Message) bool {
	var hasher vthash.Hasher

	position, err := mysql.ParseMysql56GTIDSet(pkt.Gtid)
	if err != nil {
		// this should never happen because GTIDs are parsed at the vstream level before they get here;
		// for safety, ignore these records anyway
		return true
	}

	for _, r := range pkt.Records {
		for tag, path := range gt.paths {
			h := r.Row.HashWithKeySchema(&hasher, path.parentKey, path.parentSchema)
			if sequence, ok := path.recents[h]; ok {
				if sequence.Contains(position) {
					if r.Offset < 0 {
						panic("GtidTracker: tried to tag record without offset")
					}
					pkt.SeenTags = append(pkt.SeenTags, packet.Message_Seen{Tag: tag, Offset: r.Offset})
				}
			} else {
				for inf := range gt.inflight {
					if _, ok := inf.selected[h]; ok {
						// Remove any tags we've already set since
						// we're delaying the packet for reprerocessing
						// later, and we'll tag then again appropriately.
						pkt.SeenTags = nil
						inf.buffered = append(inf.buffered, pkt)
						return true
					}
				}
			}
		}
	}

	return false
}

func (gt *GtidTracker) BeginUpquery(ctx context.Context, tag dataflow.Tag, slot uint8, keys map[sql.Row]bool) {
	if slot == 0 {
		panic("slot 0 should be reserved")
	}

	path := gt.paths[tag]
	if path == nil {
		panic("missing tag for upquery?")
	}
	if len(path.nodeSchema) == 0 && len(keys) > 0 {
		panic("BeginUpquery with keys for fully materialized node")
	}

	gt.activeUpqueries |= uint64(1) << (slot - 1)

	var selected = make(map[vthash.Hash]struct{}, len(keys))
	var hasher vthash.Hasher
	for k := range keys {
		selected[k.Hash(&hasher, path.nodeSchema)] = struct{}{}
	}

	inf := &inflight{
		active:   gt.activeUpqueries,
		selected: selected,
	}
	gt.inflight[inf] = struct{}{}

	log := common.Logger(ctx).With(zap.String("node", "Table"))
	log.Debug("marked inflight query",
		zap.Int("inflight_count", len(gt.inflight)),
		zap.Uint8("slot", slot),
		zap.Uintptr("inflight", uintptr(unsafe.Pointer(inf))),
		zap.Uint64("active", gt.activeUpqueries),
	)
}

func (gt *GtidTracker) FinishUpquery(ctx context.Context, m *packet.ReplayPiece) []*packet.Message {
	ext := m.External
	if ext == nil {
		panic("GTID tracked replay without external source")
	}

	log := common.Logger(ctx).With(zap.String("node", "Table"))
	log.Debug("FinishUpquery", zap.String("gtid", ext.Gtid), zap.Int("inflight_count", len(gt.inflight)))

	if len(m.Records) > 0 {
		if ext.Gtid == "" {
			panic("replay from Table without GTID")
		}

		position, err := mysql.ParseMysql56GTIDSet(ext.Gtid)
		if err != nil {
			log.Error("failed to parse gtid", zap.String("gtid", ext.Gtid), zap.Error(err))
			return nil
		}

		path := gt.paths[m.Tag]
		if path == nil {
			panic("missing tag for upquery?")
		}

		var hasher vthash.Hasher
		for _, r := range m.Records {
			h := r.Row.HashWithKeySchema(&hasher, path.nodeKey, path.nodeSchema)
			path.recents[h] = position
			path.recentQueue.Push(h)
		}
	}

	if ext.Slot == 0 {
		panic("called FinishUpquery without UpquerySlot")
	}

	upqueryMask := uint64(1) << (ext.Slot - 1)
	gt.activeUpqueries &= ^upqueryMask

	var buffered []*packet.Message
	for inf := range gt.inflight {
		inf.active &= ^upqueryMask
		if inf.active == 0 {
			buffered = append(buffered, inf.buffered...)
			delete(gt.inflight, inf)
		}
	}
	return buffered
}

func (gt *GtidTracker) Process(ctx context.Context, m *packet.ActiveFlowPacket) error {
	for _, path := range gt.paths {
		for {
			h, ok := path.recentQueue.Pop(seenExpiry)
			if !ok {
				break
			}
			delete(path.recents, h)
		}
	}

	switch pkt := m.Inner.(type) {
	case *packet.Message:
		if pkt.Gtid == "" {
			return nil
		}
		if gt.tagRecords(pkt) {
			pkt.Records = nil
		}
	default:
		panic(fmt.Sprintf("unexpected packet type %T on gtid tracker", m.Inner))
	}
	return nil
}

type recentGtid struct {
	hash vthash.Hash
	time hack.MonotonicTime
}

type gtidQueue struct {
	data []recentGtid
}

func (q *gtidQueue) Push(hash vthash.Hash) {
	q.data = append(q.data, recentGtid{hash: hash, time: hack.Monotonic()})
	up(q.data, len(q.data)-1)
}

func (q *gtidQueue) Pop(expire time.Duration) (vthash.Hash, bool) {
	if len(q.data) == 0 {
		return vthash.Hash{}, false
	}

	if q.data[0].time.Elapsed() < expire {
		return vthash.Hash{}, false
	}

	hash := q.data[0].hash

	q.data[0] = q.data[len(q.data)-1]
	q.data = q.data[:len(q.data)-1]
	down(q.data, 0)
	return hash, true
}

func down(q []recentGtid, i int) {
	for {
		left, right := 2*i+1, 2*i+2
		if left >= len(q) || left < 0 { // `left < 0` in case of overflow
			break
		}

		// find the smallest child
		j := left
		if right < len(q) && q[right].time < q[left].time {
			j = right
		}

		if q[j].time >= q[i].time {
			break
		}

		q[i], q[j] = q[j], q[i]
		i = j
	}
}

func up(q []recentGtid, i int) {
	for {
		parent := (i - 1) / 2
		if i == 0 || q[i].time >= q[parent].time {
			break
		}

		q[i], q[parent] = q[parent], q[i]
		i = parent
	}
}
