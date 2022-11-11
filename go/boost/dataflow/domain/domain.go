package domain

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vthash"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/common/deque"
	"vitess.io/vitess/go/boost/dataflow/backlog"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/dataflow/trace"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type Executor interface {
	StreamExecute(ctx context.Context, method string, safeSession *vtgate.SafeSession, sql string, bindVars map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error
	VSchema() *vindexes.VSchema
}

type Replaying struct {
	To       boostpb.LocalNodeIndex
	Buffered *deque.Deque[*boostpb.Packet]
	Passes   int
}

type tagshard struct {
	Tag   boostpb.Tag
	Shard uint
}

type bufferedReplay struct {
	Keys     map[boostpb.Row]bool
	Unishard bool
}

type injection struct {
	start    int
	defaults []boostpb.Value
}

// When a replay misses while being processed, it triggers a replay to backfill the hole that it
// missed in. We need to ensure that when this happens, we re-run the original replay to fill the
// hole we *originally* were trying to fill.
//
// This comes with some complexity:
//
//   - If two replays both hit the *same* hole, we should only request a backfill of it once, but
//     need to re-run *both* replays when the hole is filled.
//   - If one replay hits two *different* holes, we should backfill both holes, but we must ensure
//     that we only re-run the replay once when both holes have been filled.
//
// To keep track of this, we use the `waiting` structure below. One is created for every node with
// at least one outstanding backfill, and contains the necessary bookkeeping to ensure the two
// behaviors outlined above.
//
// Note that in the type aliases above, we have chosen to use Vec<usize> instead of Tag to
// identify a hole. This is because there may be more than one Tag used to fill a given hole, and
// the set of columns uniquely identifies the set of tags.
type waiting struct {
	holes map[redo]int
	redos map[hole]map[redo]struct{}
}

type redo struct {
	tag             boostpb.Tag
	replayKey       boostpb.Row
	requestingShard uint
	unishard        bool
}

type hole struct {
	columns common.ColumnSet
	key     boostpb.Row
}

type syncPacket struct {
	pkt  *boostpb.SyncPacket
	done chan struct{}
}

type Domain struct {
	log      *zap.Logger
	executor Executor

	// aborted signals when Domain.Reactor has been canceled by context.
	aborted chan struct{}

	index   boostpb.DomainIndex
	shard   *uint
	nshards uint

	nodes    *flownode.Map
	state    *state.Map
	memstats *common.MemoryStats
	rng      *rand.Rand

	notReady      map[boostpb.LocalNodeIndex]struct{}
	ingressInject map[boostpb.LocalNodeIndex]injection
	// TODO: persistence

	replaying       *Replaying
	waiting         map[boostpb.LocalNodeIndex]*waiting
	replayPaths     map[boostpb.Tag]*replay.Path
	readerTriggered map[boostpb.LocalNodeIndex]common.RowSet
	finishedReplays chan boostpb.Tag

	replayPathsByDst map[boostpb.LocalNodeIndex]map[common.ColumnSet][]boostpb.Tag

	concurrentReplays    int
	maxConcurrentReplays int
	replayRequestQueue   *deque.Deque[replay.ReplayRequest]

	readers     *common.SyncMap[ReaderID, *backlog.Reader]
	coordinator *boostrpc.ChannelCoordinator

	bufferedReplayRequests map[tagshard]*bufferedReplay
	replayBatchTimeout     time.Duration

	chanPackets        chan *boostpb.Packet
	chanSyncPackets    chan *syncPacket
	chanDelayedPackets chan *boostpb.Packet
	chanElapsedReplays chan tagshard
	dirtyReaderNodes   map[boostpb.LocalNodeIndex]struct{}

	upquerySlots chan uint8
	upqueryMode  boostpb.UpqueryMode
}

func (d *Domain) Index() boostpb.DomainIndex {
	return d.index
}

func (d *Domain) Shard() *uint {
	return d.shard
}

func (d *Domain) Nodes() []*flownode.Node {
	return d.nodes.Values()
}

func (d *Domain) ProcessAsync(ctx context.Context, pkt *boostpb.Packet) error {
	d.chanPackets <- pkt
	return nil
}

func (d *Domain) ProcessSync(ctx context.Context, pkt *boostpb.SyncPacket) error {
	switch pkt.Inner.(type) {
	// special case: if we're waiting for a replay to finish, we won't want to block
	// the reactor loop. we do the waiting right here (where the calling goroutine
	// is the RPC one, not our reactor loop) and return right away
	case *boostpb.SyncPacket_WaitForReplay:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.aborted:
			return errDomainAborted
		case <-d.finishedReplays:
			return nil
		}
	}

	sync := &syncPacket{
		pkt:  pkt,
		done: make(chan struct{}),
	}
	d.chanSyncPackets <- sync

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.aborted:
		return errDomainAborted
	case <-sync.done:
		return nil
	}
}

func (d *Domain) delayForSelf(m *boostpb.Packet) {
	select {
	case d.chanDelayedPackets <- m:
	default:
		d.log.Error("Domain.delayForSelf: too many queued packets")
	}
}

// errDomainAborted is returned in RPCs when Domain.Reactor is canceled and RPCs
// are in-flight.
var errDomainAborted = errors.New("domain reactor aborted")

func (d *Domain) Reactor(ctx context.Context, executor processing.Executor) {
	// On completion, signal that RPCs should also abort.
	defer close(d.aborted)

	ctx = common.ContextWithLogger(ctx, d.log)
	if trace.T {
		ctx = trace.WithRoot(ctx, d.index, d.shard)
	}

	for ctx.Err() == nil {
		d.reactor(ctx, executor)
	}
}

func (d *Domain) reactor(ctx context.Context, executor processing.Executor) {
	defer func() {
		if err := recover(); err != nil {
			d.log.Error("PANIC while processing in reactor", zap.Any("recover", err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case pkt := <-d.chanPackets:
			if trace.T {
				trace.GetSpan(ctx).FinishFlow("Packet RPC", pkt.Id)
			}

			err := d.handle(ctx, pkt, executor)
			if err != nil {
				d.log.Error("failed to handle packet", zap.Error(err))
			}

		case pkt := <-d.chanSyncPackets:
			err := d.handleSync(ctx, pkt.pkt, pkt.done, executor)
			if err != nil {
				d.log.Error("failed to handle sync packet", zap.Error(err))
			}

		case pkt := <-d.chanDelayedPackets:
			if trace.T {
				ctx, _ = trace.WithSpan(ctx, "Domain.Reactor.delayedPackets", nil)
			}

			err := d.handle(ctx, pkt, executor)
			if err != nil {
				d.log.Error("failed to handle delayed packet", zap.Error(err))
			}

		moreDelayed:
			for {
				select {
				case pkt := <-d.chanDelayedPackets:
					err := d.handle(ctx, pkt, executor)
					if err != nil {
						d.log.Error("failed to handle delayed packet", zap.Error(err))
					}
				default:
					break moreDelayed
				}
			}

			for dirty := range d.dirtyReaderNodes {
				r := d.nodes.Get(dirty).AsReader()
				if w := r.Writer(); w != nil {
					w.Swap()
				}
				delete(d.dirtyReaderNodes, dirty)
			}

			if trace.T {
				trace.GetSpan(ctx).Close()
			}

		case ts := <-d.chanElapsedReplays:
			if trace.T {
				ctx, _ = trace.WithSpan(ctx, "Domain.Reactor.elapsedReplays", nil)
			}

			type elapsedreplay struct {
				tag        boostpb.Tag
				requesting uint
				keys       map[boostpb.Row]bool
				unishard   bool
			}

			getElapsedReplay := func(ts tagshard) elapsedreplay {
				rpl := d.bufferedReplayRequests[ts]
				delete(d.bufferedReplayRequests, ts)
				return elapsedreplay{
					tag:        ts.Tag,
					requesting: ts.Shard,
					keys:       rpl.Keys,
					unishard:   rpl.Unishard,
				}
			}

			var elapsedReplays []elapsedreplay
			elapsedReplays = append(elapsedReplays, getElapsedReplay(ts))

		moreElapsed:
			for {
				select {
				case ts := <-d.chanElapsedReplays:
					elapsedReplays = append(elapsedReplays, getElapsedReplay(ts))
				default:
					break moreElapsed
				}
			}

			for _, er := range elapsedReplays {
				err := d.seedAll(ctx, er.tag, er.requesting, er.keys, er.unishard, executor)
				if err != nil {
					d.log.Error("failed to seed queued replays", zap.Error(err))
				}
			}

			if trace.T {
				trace.GetSpan(ctx).Arg("elapsed_replay_count", len(elapsedReplays)).Close()
			}
		}
	}
}

func (d *Domain) handleSync(ctx context.Context, m *boostpb.SyncPacket, done chan struct{}, executor processing.Executor) error {
	var err error

	if trace.T {
		var span *trace.Span
		_, span = trace.WithSpan(ctx, "Domain.handleSync", m)
		defer span.Close()
	}

	switch pkt := m.Inner.(type) {
	case *boostpb.SyncPacket_Ready_:
		err = d.handleReady(pkt.Ready, done)
	case *boostpb.SyncPacket_SetupReplayPath_:
		err = d.handleSetupReplayPath(pkt.SetupReplayPath, done)
	default:
		panic(fmt.Sprintf("unsupported SYNC packet: %T", pkt))
	}
	return err
}

func (d *Domain) handle(ctx context.Context, m *boostpb.Packet, executor processing.Executor) error {
	var err error

	if trace.T {
		var span *trace.Span
		ctx, span = trace.WithSpan(ctx, "Domain.handle", m)
		defer span.Close()
	}

	switch pkt := m.Inner.(type) {
	case *boostpb.Packet_Message_, *boostpb.Packet_Input_, *boostpb.Packet_Vstream_:
		err = d.dispatch(ctx, m, executor)
	case *boostpb.Packet_ReplayPiece_:
		err = d.handleReplay(ctx, m, executor)
	case *boostpb.Packet_StartReplay_:
		err = d.handleStartReplay(ctx, pkt.StartReplay, executor)
	case *boostpb.Packet_Finish_:
		err = d.handleFinishReplay(ctx, pkt.Finish.Tag, pkt.Finish.Node, executor)
	case *boostpb.Packet_Evict_:
		err = d.handleEvictAny(ctx, pkt.Evict, executor)
	case *boostpb.Packet_EvictKeys_:
		err = d.handleEvictKeys(ctx, pkt.EvictKeys, executor)
	case *boostpb.Packet_Purge_:
		err = d.handlePurge(ctx, pkt.Purge)
	case *boostpb.Packet_PrepareState_:
		err = d.prepareState(ctx, pkt.PrepareState)
	case *boostpb.Packet_UpdateEgress_:
		err = d.handleUpdateEgress(pkt.UpdateEgress)
	case *boostpb.Packet_UpdateSharder_:
		err = d.handleUpdateSharder(pkt.UpdateSharder)
	case *boostpb.Packet_RequestReaderReplay_:
		err = d.handleReaderReplay(ctx, pkt.RequestReaderReplay)
	case *boostpb.Packet_RequestPartialReplay_:
		err = d.handlePartialReplay(pkt.RequestPartialReplay, executor)
	case *boostpb.Packet_AddNode_:
		err = d.handleAddNode(pkt.AddNode)
	case *boostpb.Packet_RemoveNodes_:
		err = d.handleRemoveNodes(pkt.RemoveNodes)
	default:
		panic(fmt.Sprintf("unsupported packet: %T", pkt))
	}
	return err
}

func (d *Domain) handleReady(rdy *boostpb.SyncPacket_Ready, done chan struct{}) error {
	node := rdy.Node

	if d.replaying != nil {
		d.log.Panic("called Ready on replaying node", node.Zap())
	}

	d.nodes.Get(node).Purge = rdy.Purge

	if len(rdy.Index) > 0 {
		n := d.nodes.Get(node)
		externalBase := n.AsExternalBase()

		if externalBase != nil {
			d.log.Debug("setting up external base")
		} else {
			s := state.NewMemoryState()
			for _, idx := range rdy.Index {
				s.AddKey(idx.Key, n.Schema(), nil)
			}
			d.state.Insert(node, s)
			d.memstats.Register(d.index, d.shard, n.GlobalAddr(), s.StateSizeAtomic())
		}
	}
	// NOTE: just because index_on is None does *not* mean we're not
	// materialized

	delete(d.notReady, node)

	{
		n := d.nodes.Get(node)
		if r := n.AsReader(); r != nil {
			if state := r.Writer(); state != nil {
				state.Swap()
			}
		}
	}

	d.log.Info("readying local node", node.Zap())
	close(done)
	return nil
}

func (d *Domain) dispatch(ctx context.Context, m *boostpb.Packet, executor processing.Executor) error {
	if trace.T {
		var span *trace.Span
		ctx, span = trace.WithSpan(ctx, "Domain.dispatch", m)
		defer span.Close()
	}

	src := m.Src()
	me := m.Dst()

	if fwd := d.replaying; fwd != nil {
		if fwd.To == me {
			if trace.T {
				trace.GetSpan(ctx).CloseWithReason("dispatch buffered")
			}
			fwd.Buffered.PushBack(m)
			return nil
		}
	}

	if _, notready := d.notReady[me]; notready {
		if trace.T {
			trace.GetSpan(ctx).CloseWithReason("not ready")
		}
		d.log.Warn("dispatch not ready", me.Zap())
		return nil
	}

	var input = flownode.FlowInput{
		Packet:     m,
		KeyedBy:    nil,
		State:      d.state,
		Nodes:      d.nodes,
		OnShard:    d.shard,
		Swap:       true,
		ReplayPath: nil,
		Ex:         executor,
	}
	var output flownode.FlowOutput

	n := d.nodes.Get(me)
	if err := n.Process(ctx, &input, &output); err != nil {
		return err
	}

	if trace.T {
		trace.GetSpan(ctx).Arg("after_process", input.Packet)
	}

	if input.Packet == nil {
		// no need to deal with our children if we're not sending them anything
		if trace.T {
			trace.GetSpan(ctx).CloseWithReason("no output")
		}
		return nil
	}

	// normally, we ignore misses during regular forwarding.
	// however, we have to be a little careful in the case of joins.
	var evictions map[boostpb.Tag]map[boostpb.Row]bool
	if n.IsInternal() && n.IsJoin() && len(output.Misses) > 0 {
		// there are two possible cases here:
		//
		//  - this is a write that will hit a hole in every downstream materialization.
		//    dropping it is totally safe!
		//  - this is a write that will update an entry in some downstream materialization.
		//    this is *not* allowed! we *must* ensure that downstream remains up to date.
		//    but how can we? we missed in the other side of the join, so we can't produce
		//    the necessary output record... what we *can* do though is evict from any
		//    downstream, and then we guarantee that we're in case 1!
		//
		// if you're curious about how we may have ended up in case 2 above, here are two
		// ways:
		//
		//  - some downstream view is partial over the join key. some time in the past, it
		//    requested a replay of key k. that replay produced *no* rows from the side
		//    that was replayed. this in turn means that no lookup was performed on the
		//    other side of the join, and so k wasn't replayed to that other side (which
		//    then still has a hole!). in that case, any subsequent write with k in the
		//    join column from the replay side will miss in the other side.
		//  - some downstream view is partial over a column that is *not* the join key. in
		//    the past, it replayed some key k, which means that we aren't allowed to drop
		//    any write with k in that column. now, a write comes along with k in that
		//    replay column, but with some hitherto unseen key z in the join column. if the
		//    replay of k never caused a lookup of z in the other side of the join, then
		//    the other side will have a hole. thus, we end up in the situation where we
		//    need to forward a write through the join, but we miss.
		//
		// unfortunately, we can't easily distinguish between the case where we have to
		// evict and the case where we don't (at least not currently), so we *always* need
		// to evict when this happens. this shouldn't normally be *too* bad, because writes
		// are likely to be dropped before they even reach the join in most benign cases
		// (e.g., in an ingress). this can be remedied somewhat in the future by ensuring
		// that the first of the two causes outlined above can't happen (by always doing a
		// lookup on the replay key, even if there are now rows). then we know that the
		// *only* case where we have to evict is when the replay key != the join key.
		//
		// but, for now, here we go:
		// first, what partial replay paths go through this node?
		from := d.nodes.Get(src).GlobalAddr()

		type tagkeys struct {
			tag  boostpb.Tag
			keys []int
		}

		var deps []tagkeys

		for tag, rp := range d.replayPaths {
		nextSegment:
			for _, rps := range rp.Path {
				if rps.Node == me {
					keys := rps.PartialKey
					// we need to find the *input* column that produces that output.
					//
					// if one of the columns for this replay path's keys does not
					// resolve into the ancestor we got the update from, we don't need
					// to issue an eviction for that path. this is because we *missed*
					// on the join column in the other side, so we *know* it can't have
					// forwarded anything related to the write we're now handling.
					var cols []int
					for _, k := range keys {
						parentColumns := n.ParentColumns(k)
						idx := slices.IndexFunc(parentColumns, func(nc flownode.NodeColumn) bool { return nc.Node == from })
						if idx < 0 {
							continue nextSegment
						}
						cols = append(cols, parentColumns[idx].Column)
					}
					deps = append(deps, tagkeys{tag: tag, keys: cols})
				}
			}
		}

		// map[boostpb.Row]: safe, used for iteration
		evictions = make(map[boostpb.Tag]map[boostpb.Row]bool)
		for _, miss := range output.Misses {
			for _, tk := range deps {
				rb, ok := evictions[tk.tag]
				if !ok {
					rb = make(map[boostpb.Row]bool)
					evictions[tk.tag] = rb
				}

				var misskey = boostpb.NewRowBuilder(len(tk.keys))
				for _, key := range tk.keys {
					misskey.Add(miss.Record.Row.ValueAt(key))
				}
				rb[misskey.Finish()] = true
			}
		}
	}

	for tag, keys := range evictions {
		var pkt = &boostpb.Packet_EvictKeys{
			Keys: maps.Keys(keys),
			Link: &boostpb.Link{Src: src, Dst: me},
			Tag:  tag,
		}
		if err := d.handleEvictKeys(ctx, pkt, executor); err != nil {
			return err
		}
	}

	switch input.Packet.Inner.(type) {
	case *boostpb.Packet_Message_:
		if input.Packet.IsEmpty() {
			if trace.T {
				trace.GetSpan(ctx).CloseWithReason("dispatch empty packet")
			}
			return nil
		}
	case *boostpb.Packet_ReplayPiece_:
		panic("replay should never go through dispatch")
	default:
		panic("???")
	}

	for _, child := range d.nodes.Get(me).Children() {
		childIsMerger := d.nodes.Get(child).IsShardMerger()

		m := input.Packet.CloneData()
		link := m.Link()

		if childIsMerger {
			// we need to preserve the egress src (which includes shard identifier)
		} else {
			link.Src = me
		}
		link.Dst = child
		if err := d.dispatch(ctx, m, executor); err != nil {
			return err
		}
	}

	return nil
}

func (d *Domain) memstate(node boostpb.LocalNodeIndex) *state.Memory {
	var memstate *state.Memory
	if !d.state.ContainsKey(node) {
		memstate = state.NewMemoryState()
		d.log.Debug("allocate NewMemoryState", node.Zap())
		d.state.Insert(node, memstate)
		d.memstats.Register(d.index, d.shard, d.nodes.Get(node).GlobalAddr(), memstate.StateSizeAtomic())
	} else {
		memstate = d.state.Get(node)
	}
	return memstate
}

func partialGlobalState(
	ctx context.Context,
	coord *boostrpc.ChannelCoordinator,
	node boostpb.LocalNodeIndex,
	schema []boostpb.Type,
	st *boostpb.Packet_PrepareState_PartialGlobal) (
	*backlog.Reader, *backlog.Writer, error,
) {
	triggerDomain := st.TriggerDomain.Domain
	shards := st.TriggerDomain.Shards
	k := st.Key

	var txs []chan []boostpb.Row
	for shard := uint(0); shard < shards; shard++ {
		tx := make(chan []boostpb.Row, 8) // TODO: size
		sender, err := coord.GetClient(triggerDomain, shard)
		if err != nil {
			return nil, nil, err
		}

		log := common.Logger(ctx).With(
			zap.Uint("upquery_target_shard", shard),
			triggerDomain.ZapField("upquery_target_domain"))

		go func() {
			defer sender.Close()

			for {
				select {
				case <-ctx.Done():
					return
				case miss := <-tx:
					var pkt = &boostpb.Packet{
						Inner: &boostpb.Packet_RequestReaderReplay_{
							RequestReaderReplay: &boostpb.Packet_RequestReaderReplay{
								Node: node,
								Cols: k,
								Keys: miss,
							},
						},
					}
					if err := sender.ProcessAsync(ctx, pkt); err != nil {
						log.Error("failed to process upquery", zap.Error(err))
					}
				}
			}
		}()

		txs = append(txs, tx)
	}

	var (
		shardCount = uint(len(txs))
		onMiss     func(misses []boostpb.Row) bool
	)

	if shardCount == 1 {
		onMiss = func(misses []boostpb.Row) bool {
			if len(misses) == 0 {
				return true
			}
			select {
			case txs[0] <- misses:
				return true
			default:
				return false
			}
		}
	} else {
		var hasher vthash.Hasher
		var shardKeyType = schema[k[0]]

		onMiss = func(misses []boostpb.Row) bool {
			perShard := make(map[uint][]boostpb.Row)
			for _, miss := range misses {
				if miss.Len() != 1 {
					panic("miss of wrong size")
				}
				shard := miss.ShardValue(&hasher, 0, shardKeyType, shardCount)
				perShard[shard] = append(perShard[shard], miss)
			}
			if len(perShard) == 0 {
				return true
			}
			for shard, m := range perShard {
				select {
				case txs[shard] <- m:
				default:
					return false
				}
			}
			return true
		}
	}

	r, w := backlog.New(k, schema, onMiss)
	return r, w, nil
}

func (d *Domain) prepareState(ctx context.Context, pkt *boostpb.Packet_PrepareState) error {
	node := pkt.Node

	switch st := pkt.State.(type) {
	case *boostpb.Packet_PrepareState_PartialLocal_:
		memstate := d.memstate(node)
		for _, idx := range st.PartialLocal.Index {
			memstate.AddKey(idx.Key, d.nodes.Get(node).Schema(), idx.Tags)
		}

	case *boostpb.Packet_PrepareState_IndexedLocal_:
		memstate := d.memstate(node)
		for _, idx := range st.IndexedLocal.Index {
			memstate.AddKey(idx.Key, d.nodes.Get(node).Schema(), nil)
		}

	case *boostpb.Packet_PrepareState_PartialGlobal_:
		n := d.nodes.Get(node)
		r, w, err := partialGlobalState(ctx, d.coordinator, node, n.Schema(), st.PartialGlobal)
		if err != nil {
			return err
		}
		d.readers.Set(ReaderID{st.PartialGlobal.Gid, d.shardn()}, r)
		d.memstats.Register(d.index, d.shard, n.GlobalAddr(), w.StateSizeAtomic())
		n.AsReader().SetWriteHandle(w)

	case *boostpb.Packet_PrepareState_Global_:
		n := d.nodes.Get(node)
		r, w := backlog.New(st.Global.Key, n.Schema(), nil)
		d.readers.Set(ReaderID{st.Global.Gid, d.shardn()}, r)
		d.memstats.Register(d.index, d.shard, n.GlobalAddr(), w.StateSizeAtomic())
		n.AsReader().SetWriteHandle(w)
	}

	return nil
}

func (d *Domain) shardn() uint {
	if d.shard == nil {
		return 0
	}
	return *d.shard
}

func (d *Domain) handleReplay(ctx context.Context, pkt *boostpb.Packet, executor processing.Executor) error {
	if trace.T {
		var span *trace.Span
		ctx, span = trace.WithSpan(ctx, "Domain.handleReplay", pkt)
		defer span.Close()
	}

	tag := pkt.Tag()
	rp := d.replayPaths[pkt.Tag()]
	pathLast := rp.Path[len(rp.Path)-1]
	m := pkt.Inner.(*boostpb.Packet_ReplayPiece_).ReplayPiece

	if d.nodes.Get(pathLast.Node).IsDropped() {
		if trace.T {
			trace.GetSpan(ctx).CloseWithReason("replay on dropped node")
		}
		return nil
	}

	if src := d.nodes.Get(pkt.Src()); src != nil {
		if external := src.AsExternalBase(); external != nil {
			var finish, exit bool

			switch m.Context.(type) {
			case *boostpb.Packet_ReplayPiece_Failed:
				finish = true
				exit = true
			case *boostpb.Packet_ReplayPiece_Partial:
				finish = true
			}

			if finish && m.UpquerySlot > 0 {
				d.upquerySlots <- uint8(m.UpquerySlot)
				external.FinishUpquery(ctx, m)
			}
			if exit {
				return nil
			}
		}
	}

	if _, failed := m.Context.(*boostpb.Packet_ReplayPiece_Failed); failed {
		panic("failed upquery sourced from non-external base")
	}

	type needreplay struct {
		node              boostpb.LocalNodeIndex
		whileReplayingKey boostpb.Row
		missKey           boostpb.Row
		missCols          []int
		unishard          bool
		requestingShard   uint
		tag               boostpb.Tag
	}
	type finished struct {
		tag     boostpb.Tag
		ni      boostpb.LocalNodeIndex
		forKeys map[boostpb.Row]bool
	}

	var needReplay []*needreplay
	var finishedReplay *finished
	var finishedPartial int

	switch {
	case d.replaying == nil && rp.NotifyDone:
		// this is the first message we receive for this tagged replay path. only at
		// this point should we start buffering messages for the target node. since the
		// node is not yet marked ready, all previous messages for this node will
		// automatically be discarded by dispatch(). the reason we should ignore all
		// messages preceeding the first replay message is that those have already been
		// accounted for in the state we are being replayed. if we buffered them and
		// applied them after all the state has been replayed, we would double-apply
		// those changes, which is bad.
		d.replaying = &Replaying{
			To:       pathLast.Node,
			Buffered: deque.New[*boostpb.Packet](),
			Passes:   0,
		}
	case d.replaying == nil:
		// we're replaying to forward to another domain
	case d.replaying != nil:
		// another packet the local state we are constructing
	}

	// let's collect some information about the destination of this replay
	dst := pathLast.Node
	dstIsReader := false
	if r := d.nodes.Get(dst).AsReader(); r != nil {
		dstIsReader = r.IsMaterialized()
	}
	dstIsTarget := !d.nodes.Get(dst).IsSender()

	if trace.T {
		trace.GetSpan(ctx).Arg("dst_is_target", dstIsTarget)
	}

	if dstIsTarget {
		if partial, ok := m.Context.(*boostpb.Packet_ReplayPiece_Partial); ok {
			partial := partial.Partial
			had := len(partial.ForKeys)
			partialKeys := common.NewColumnSet(pathLast.PartialKey)

			if w, ok := d.waiting[dst]; ok {
				for r := range partial.ForKeys {
					_, found := w.redos[hole{partialKeys, r}]
					if !found {
						delete(partial.ForKeys, r)
					}
				}
			} else if prev, ok := d.readerTriggered[dst]; ok {
				for r := range partial.ForKeys {
					if !prev.Contains(r) {
						delete(partial.ForKeys, r)
					}
				}
			} else {
				if trace.T {
					trace.GetSpan(ctx).CloseWithReason("no waiting nor triggered")
				}
				return nil
			}

			if len(partial.ForKeys) == 0 {
				if trace.T {
					trace.GetSpan(ctx).CloseWithReason("partial.ForKeys == 0")
				}
				return nil
			} else if len(partial.ForKeys) != had {
				// discard records in data associated with the keys we weren't
				// waiting for
				// note that we need to use the partial_keys column IDs from the
				// *start* of the path here, as the records haven't been processed
				// yet
				partialKeys := rp.Path[0].PartialKey

				var records = m.Records[:0]
				for _, r := range m.Records {
					var anyRowIsEqual bool

					for k := range partial.ForKeys {
						var rowIsEqual = true
						for i, c := range partialKeys {
							if r.Row.ValueAt(c).Cmp(k.ValueAt(i)) != 0 {
								rowIsEqual = false
								break
							}
						}
						if rowIsEqual {
							anyRowIsEqual = true
							break
						}
					}

					if anyRowIsEqual {
						records = append(records, r)
					}
				}

				m.Records = records
			}
		}
	}

	var input = flownode.FlowInput{
		Packet: &boostpb.Packet{
			Inner: &boostpb.Packet_ReplayPiece_{
				ReplayPiece: &boostpb.Packet_ReplayPiece{
					Link:    m.Link,
					Tag:     m.Tag,
					Records: m.Records,
					Context: m.Context,
					Gtid:    m.Gtid,
				},
			},
		},
		KeyedBy:    nil,
		State:      d.state,
		Nodes:      d.nodes,
		OnShard:    d.shard,
		Swap:       false,
		ReplayPath: rp,
		Ex:         executor,
	}

	// forward the current message through all local nodes.
	for i, segment := range rp.Path {
		if segment.ForceTagTo != boostpb.TagNone {
			if rpiece, ok := input.Packet.Inner.(*boostpb.Packet_ReplayPiece_); ok {
				rpiece.ReplayPiece.Tag = segment.ForceTagTo
			}
		}
		n := d.nodes.Get(segment.Node)
		isReader := false
		if r := n.AsReader(); r != nil {
			isReader = r.IsMaterialized()
		}

		// keep track of whether we're filling any partial holes
		partialKeyCols := segment.PartialKey

		// keep a copy of the partial keys from before we process
		// we need this because n.process may choose to reduce the set of keys
		// (e.g., because some of them missed), in which case we need to know what
		// keys to _undo_.
		var backfillKeys map[boostpb.Row]bool
		if partial := input.Packet.GetReplayPiece().GetPartial(); partial != nil {
			backfillKeys = maps.Clone(partial.ForKeys)
		}

		// figure out if we're the target of a partial replay.
		// this is the case either if the current node is waiting for a replay,
		// *or* if the target is a reader. the last case is special in that when a
		// client requests a replay, the Reader isn't marked as "waiting".
		target := backfillKeys != nil && i == len(rp.Path)-1 && (isReader || !n.IsSender())

		// are we about to fill a hole?
		if target {
			// mark the state for the key being replayed as *not* a hole otherwise
			// we'll just end up with the same "need replay" response that
			// triggered this replay initially.
			if st := d.state.Get(segment.Node); st != nil {
				for key := range backfillKeys {
					st.MarkFilled(key, m.Tag)
				}
			} else {
				r := n.AsReader()
				if wh := r.Writer(); wh != nil {
					for key := range backfillKeys {
						wh.WithKey(key).MarkFilled()
					}
				}
			}
		}

		input.KeyedBy = segment.PartialKey
		var output flownode.FlowOutput
		if err := n.Process(ctx, &input, &output); err != nil {
			return err
		}

		slices.SortFunc(output.Misses, func(a, b processing.Miss) bool {
			return a.Compare(&b) < 0
		})
		slices.CompactFunc(output.Misses, func(a, b processing.Miss) bool {
			return a.Compare(&b) == 0
		})

		var missedOn common.RowSet
		if backfillKeys != nil {
			missedOn = common.NewRowSet(n.Schema())
			for _, miss := range output.Misses {
				missedOn.Add(miss.ReplayKey())
			}
		}

		if target {
			if len(output.Misses) > 0 {
				// we missed while processing
				// it's important that we clear out any partially-filled holes.
				if st := d.state.Get(segment.Node); st != nil {
					missedOn.ForEach(func(miss boostpb.Row) {
						st.MarkHole(miss, tag)
					})
				} else {
					if w := n.AsReader().Writer(); w != nil {
						missedOn.ForEach(func(miss boostpb.Row) {
							w.WithKey(miss).MarkHole()
						})
					}
				}
			} else if isReader {
				r := n.AsReader()
				if wh := r.Writer(); wh != nil {
					wh.Swap()
				}
				if prev, ok := d.readerTriggered[segment.Node]; ok {
					for key := range backfillKeys {
						prev.Remove(key)
					}
				}
			}
		}

		if target && len(output.Captured) > 0 {
			// materialized union ate some of our keys,
			// so we didn't *actually* fill those keys after all!
			if st := d.state.Get(segment.Node); st != nil {
				for key := range output.Captured {
					st.MarkHole(key, m.Tag)
				}
			} else {
				r := n.AsReader()
				if wh := r.Writer(); wh != nil {
					for key := range output.Captured {
						wh.WithKey(key).MarkHole()
					}
				}
			}
		}

		if input.Packet == nil {
			// eaten full replay
			goto finished
		}

		// we need to track how many replays we completed, and we need to do so
		// *before* we prune keys that missed. these conditions are all important,
		// so let's walk through them
		//
		//  1. this applies only to partial backfills
		//  2. we should only set finished_partial if it hasn't already been set.
		//     this is important, as misses will cause backfill_keys to be pruned
		//     over time, which would cause finished_partial to hold the wrong
		//     value!
		//  3. if the last node on this path is a reader, or is a ::End (so we
		//     triggered the replay) then we need to decrement the concurrent
		//     replay count! note that it's *not* sufficient to check if the
		//     *current* node is a target/reader, because we could miss during a
		//     join along the path.
		if backfillKeys != nil && finishedPartial == 0 && (dstIsReader || dstIsTarget) {
			finishedPartial = len(backfillKeys)
		}

		if rpiece, ok := input.Packet.Inner.(*boostpb.Packet_ReplayPiece_); ok {
			if partial, ok := rpiece.ReplayPiece.Context.(*boostpb.Packet_ReplayPiece_Partial); ok {
				partial := partial.Partial
				for k := range backfillKeys {
					if !partial.ForKeys[k] {
						delete(backfillKeys, k)
					}
				}
			}
		}

		// if we missed during replay, we need to do another replay
		if backfillKeys != nil && len(output.Misses) > 0 {
			// so, in theory, unishard can be changed by n.process. however, it
			// will only ever be changed by a union, which can't cause misses.
			// since we only enter this branch in the cases where we have a miss,
			// it is okay to assume that unishard _hasn't_ changed, and therefore
			// we can use the value that's in m.

			partial := input.Packet.Inner.(*boostpb.Packet_ReplayPiece_).ReplayPiece.Context.(*boostpb.Packet_ReplayPiece_Partial).Partial

			for _, miss := range output.Misses {
				needReplay = append(needReplay, &needreplay{
					node:              miss.On,
					whileReplayingKey: miss.ReplayKey(),
					missKey:           miss.LookupKey(),
					missCols:          miss.LookupIdx,
					unishard:          partial.Unishard,
					requestingShard:   partial.RequestingShard,
					tag:               m.Tag,
				})
			}

			// we should only finish the replays for keys that *didn't* miss
			for k := range backfillKeys {
				if missedOn.Contains(k) {
					delete(backfillKeys, k)
				}
			}

			// prune all replayed records for keys where any replayed record for
			// that key missed.
			input.Packet.MapData(func(records *[]boostpb.Record) {
				retained := (*records)[:0]
				for _, r := range *records {
					partialRow := r.Row.IndexWith(partialKeyCols)
					if !missedOn.Contains(partialRow) {
						retained = append(retained, r)
					}
				}
				*records = retained
			})
		}

		if backfillKeys != nil && len(backfillKeys) == 0 {
			goto finished
		}

		// we successfully processed some upquery responses!
		//
		// at this point, we can discard the state that the replay used in n's
		// ancestors if they are beyond the materialization frontier (and thus
		// should not be allowed to amass significant state).
		//
		// we want to make sure we only remove state once it will no longer be
		// looked up into though. consider this dataflow graph:
		//
		//  (a)     (b)
		//   |       |
		//   |       |
		//  (q)      |
		//   |       |
		//   `--(j)--`
		//       |
		//
		// where j is a join, a and b are materialized, q is query-through. if we
		// removed state the moment a replay has passed through the next operator,
		// then the following could happen: a replay then comes from a, passes
		// through q, q then discards state from a and forwards to j. j misses in
		// b. replay happens to b, and re-triggers replay from a. however, state in
		// a is discarded, so replay to a needs to happen a second time. that's not
		// _wrong_, and we will eventually make progress, but it is pretty
		// inefficient.
		//
		// insted, we probably want the join to do the eviction. we achieve this by
		// only evicting from a after the replay has passed the join (or, more
		// generally, the operator that might perform lookups into a)
		if backfillKeys != nil {
			// first and foremost -- evict the source of the replay (if we own it).
			// we only do this when the replay has reached its target, or if it's
			// about to leave the domain, otherwise we might evict state that a
			// later operator (like a join) will still do lookups into.
			if i == len(rp.Path)-1 {
				if rp.Source != boostpb.InvalidLocalNode {
					n := d.nodes.Get(rp.Source)
					if n.BeyondMatFrontier() {
						st := d.state.Get(rp.Source)
						if st == nil {
							panic("replay sourced at non-materialized node")
						}
						for r := range backfillKeys {
							st.MarkHole(r, m.Tag)
						}
					}
				}
			}

			// next, evict any state that we had to look up to process this replay.
			var pnsFor = boostpb.InvalidLocalNode
			var evictTag = boostpb.TagNone
			var pns []boostpb.LocalNodeIndex
			var tmp []boostpb.LocalNodeIndex

			for _, lookup := range output.Lookups {
				// don't evict from our own state
				if lookup.On == segment.Node {
					continue
				}

				if pnsFor != lookup.On {
					pns = pns[:0]
					tmp = append(tmp, lookup.On)

					for len(tmp) > 0 {
						pn := tmp[len(tmp)-1]
						tmp = tmp[:len(tmp)-1]
						pnn := d.nodes.Get(pn)

						if d.state.ContainsKey(pn) {
							if pnn.BeyondMatFrontier() {
								//we should evict from this
								pns = append(pns, pn)
							}
							// Otherwise we should not evict from this
							continue
						}

						if !pnn.CanQueryThrough() {
							panic("lookup into non-materialized, non-query-through node")
						}
						tmp = append(tmp, pnn.Parents()...)
					}

					pnsFor = lookup.On
				}

				tagMatch := func(rp *replay.Path, pn boostpb.LocalNodeIndex) bool {
					lastp := rp.Path[len(rp.Path)-1]
					return lastp.Node == pn && slices.Equal(lastp.PartialKey, lookup.Cols)
				}

				for _, pn := range pns {
					st := d.state.Get(pn)

					// this is a node that we were doing lookups into as part of
					// the replay -- make sure we evict any state we may have added
					// there.
					if evictTag != boostpb.TagNone {
						if !tagMatch(d.replayPaths[evictTag], pn) {
							// can't reuse
							evictTag = boostpb.TagNone
						}
					}

					if evictTag == boostpb.TagNone {
						if cs, ok := d.replayPathsByDst[pn]; ok {
							if tags, ok := cs[common.NewColumnSet(lookup.Cols)]; ok {
								// this is the tag we would have used to
								// fill a lookup hole in this ancestor, so
								// this is the tag we need to evict from.
								if len(tags) != 1 {
									panic("TODO: multiple tags on eviction")
								}
								evictTag = tags[0]
							}
						}
					}

					if evictTag != boostpb.TagNone {
						st.MarkHole(lookup.Key, evictTag)
					} else {
						panic("no tag found for lookup target")
					}
				}

			}
		}

		// we're all good -- continue propagating
		if rpiece := input.Packet.GetReplayPiece(); rpiece != nil {
			if len(rpiece.Records) == 0 {
				if regular := rpiece.GetRegular(); regular != nil && !regular.Last {
					// don't continue processing empty updates, *except* if this is the
					// last replay batch. in that case we need to send it so that the
					// next domain knows that we're done
					// TODO: we *could* skip ahead to path.last() here
					break
				}
			}
		}

		if i < len(rp.Path)-1 {
			// update link for next iteration
			if d.nodes.Get(rp.Path[i+1].Node).IsShardMerger() {
				// we need to preserve the egress src for shard mergers
				// (which includes shard identifier)
			} else {
				input.Packet.Link().Src = segment.Node
			}
			input.Packet.Link().Dst = rp.Path[i+1].Node
		}

		if rpiece := input.Packet.GetReplayPiece(); rpiece != nil {
			if partial := rpiece.GetPartial(); partial != nil {
				partial.ForKeys = backfillKeys
			}
		}
	}

	switch replayctx := input.Packet.GetReplayPiece().Context.(type) {
	case *boostpb.Packet_ReplayPiece_Regular:
		regular := replayctx.Regular
		if regular.Last {
			if rp.NotifyDone {
				finishedReplay = &finished{tag: m.Tag, ni: dst, forKeys: nil}
			}
		}
	case *boostpb.Packet_ReplayPiece_Partial:
		partial := replayctx.Partial
		if dstIsReader {
			if d.nodes.Get(dst).BeyondMatFrontier() {
				// make sure we eventually evict these from here
				time.AfterFunc(50*time.Millisecond, func() {
					d.delayForSelf(&boostpb.Packet{Inner: &boostpb.Packet_Purge_{
						Purge: &boostpb.Packet_Purge{
							View: dst,
							Tag:  tag,
							Keys: partial.ForKeys,
						},
					}})
				})
			}
		} else if dstIsTarget {
			if finishedPartial == 0 {
				if len(partial.ForKeys) != 0 {
					panic("expected ForKeys to be empty")
				}
			}
			finishedReplay = &finished{m.Tag, dst, partial.ForKeys}
		} // otherwise we're just on the replay path
	}

finished:
	if finishedPartial != 0 {
		if err := d.finishedPartialReplay(ctx, tag, finishedPartial); err != nil {
			return err
		}
	}

	for _, nr := range needReplay {
		if err := d.onReplayMiss(ctx, nr.node, nr.missCols, nr.whileReplayingKey, nr.missKey, nr.unishard, nr.requestingShard, nr.tag); err != nil {
			return err
		}
	}

	if finishedReplay != nil {
		d.log.Debug("partial replay finished", finishedReplay.ni.Zap())
		if waiting, ok := d.waiting[finishedReplay.ni]; ok {
			delete(d.waiting, finishedReplay.ni)

			rp := d.replayPaths[finishedReplay.tag]
			keyCols := common.NewColumnSet(rp.Path[len(rp.Path)-1].PartialKey)

			// we got a partial replay result that we were waiting for. it's time we let any
			// downstream nodes that missed in us on that key know that they can (probably)
			// continue with their replays.
			for key := range finishedReplay.forKeys {
				h := hole{keyCols, key}
				replay, ok := waiting.redos[h]
				if !ok {
					panic("got backfill for unnecessary key")
				}
				delete(waiting.redos, h)

				var replayvec []redo
				for taggedReplayKey := range replay {
					waiting.holes[taggedReplayKey]--
					left := waiting.holes[taggedReplayKey]

					if left == 0 {
						d.log.Debug("filled last hole for key, triggering replay",
							zap.Any("key", taggedReplayKey))
						delete(waiting.holes, taggedReplayKey)
						replayvec = append(replayvec, taggedReplayKey)
					} else {
						d.log.Debug("filled hole for key, not triggering replay",
							zap.Any("key", taggedReplayKey), zap.Int("left", left))
					}
				}

				for _, rd := range replayvec {
					var request = &boostpb.Packet_RequestPartialReplay{
						Tag:             rd.tag,
						Unishard:        rd.unishard,
						RequestingShard: rd.requestingShard,
						Keys:            []boostpb.Row{rd.replayKey},
					}

					pkt := &boostpb.Packet{
						Inner: &boostpb.Packet_RequestPartialReplay_{RequestPartialReplay: request},
					}
					d.delayForSelf(pkt)
				}
			}

			if len(waiting.holes) > 0 {
				// there are still holes, so there must still be pending redos
				d.waiting[finishedReplay.ni] = waiting
			} else {
				if len(waiting.redos) > 0 {
					panic("there should be no redos left")
				}
			}
		} else if finishedReplay.forKeys != nil {
			panic("unreachable")
		} else {
			// must be a full replay
			// NOTE: node is now ready, in the sense that it shouldn't ignore all updates since
			// replaying_to is still set, "normal" dispatch calls will continue to be buffered, but
			// this allows finish_replay to dispatch into the node by overriding replaying_to.
			delete(d.notReady, finishedReplay.ni)

			pkt := &boostpb.Packet{
				Inner: &boostpb.Packet_Finish_{
					Finish: &boostpb.Packet_Finish{
						Tag:  tag,
						Node: finishedReplay.ni,
					},
				},
			}
			d.delayForSelf(pkt)
		}
	}

	return nil
}

func (d *Domain) evictionWalkPath(ctx context.Context, path []*boostpb.ReplayPathSegment, keys *[]boostpb.Row, tag boostpb.Tag, ex processing.Executor) error {
	from := path[0].Node
	for _, seg := range path {
		if err := d.nodes.Get(seg.Node).ProcessEviction(ctx, from, seg.PartialKey, keys, tag, d.shard, ex); err != nil {
			return err
		}
		from = seg.Node
	}
	return nil
}

func (d *Domain) triggerDownstreamEvictions(ctx context.Context, keyCols []int, keys []boostpb.Row, node boostpb.LocalNodeIndex, ex processing.Executor) error {
	for tag, path := range d.replayPaths {
		if path.Source == node {
			// Check whether this replay path is for the same key.
			switch trigger := path.Trigger.(type) {
			case *replay.TriggerLocal:
				if !slices.Equal(trigger.Columns, keyCols) {
					continue
				}
			case *replay.TriggerStart:
				if !slices.Equal(trigger.Columns, keyCols) {
					continue
				}
			default:
				panic("unreachable")
			}

			keys := slices.Clone(keys)
			if err := d.evictionWalkPath(ctx, path.Path, &keys, tag, ex); err != nil {
				return err
			}

			if _, isLocal := path.Trigger.(*replay.TriggerLocal); isLocal {
				target := path.Path[len(path.Path)-1]
				if d.nodes.Get(target.Node).IsReader() {
					// already evicted from in walk_path
					continue
				}
				if !d.state.ContainsKey(target.Node) {
					// this is probably because
					if _, notready := d.notReady[target.Node]; !notready {
						d.log.Warn("got eviction for ready but stateless node", target.Node.Zap())
					}
					continue
				}

				d.log.Debug("downstream eviction for node", target.Node.Zap(), tag.Zap(), zap.Int("key_count", len(keys)))

				d.state.Get(target.Node).EvictKeys(tag, keys)
				if err := d.triggerDownstreamEvictions(ctx, target.PartialKey, keys, target.Node, ex); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *Domain) handleEvictAny(ctx context.Context, pkt *boostpb.Packet_Evict, ex processing.Executor) error {
	type nodesize struct {
		node boostpb.LocalNodeIndex
		size int64
	}
	var nodes []nodesize

	if pkt.Node == boostpb.InvalidLocalNode {
		d.nodes.ForEach(func(n *flownode.Node) bool {
			local := n.LocalAddr()
			if r := n.AsReader(); r != nil && r.IsPartial() {
				if sz := r.StateSizeAtomic().Load(); sz > 0 {
					nodes = append(nodes, nodesize{local, sz})
				}
			} else if st := d.state.Get(local); st != nil && st.IsPartial() {
				if sz := st.StateSizeAtomic().Load(); sz > 0 {
					nodes = append(nodes, nodesize{local, sz})
				}
			}
			return true
		})

		slices.SortFunc(nodes, func(a, b nodesize) bool {
			return a.size > b.size
		})

		// TODO: we don't want to evict from all nodes in this domain; Noria just truncates
		// 	the list of nodes to 3, but can we be smarter about this?

		n := int64(len(nodes))
		for i := len(nodes) - 1; i >= 0; i-- {
			target := &nodes[i]
			share := (pkt.NumBytes + n - 1) / n

			if n > 1 && target.size/2 < share {
				target.size = target.size / 2
			} else {
				target.size = share
			}

			pkt.NumBytes -= target.size
			n--
		}
	} else {
		nodes = append(nodes, nodesize{node: pkt.Node, size: pkt.NumBytes})
	}

	for _, target := range nodes {
		var n = d.nodes.Get(target.node)

		if n.IsDropped() {
			continue
		}

		d.log.Debug("random eviction from node", target.node.Zap(), zap.Int64("memory", target.size))

		if r := n.AsReader(); r != nil {
			if !r.IsPartial() {
				panic("trying to evict from a fully materialized node")
			}
			r.EvictRandomKeys(d.rng, target.size)
		} else {
			st := d.state.Get(target.node)
			if !st.IsPartial() {
				panic("trying to evict from a fully materialized node")
			}

			keyCols, keys := st.EvictRandomKeys(d.rng, target.size)
			if len(keys) > 0 {
				if err := d.triggerDownstreamEvictions(ctx, keyCols, keys, target.node, ex); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (d *Domain) handleEvictKeys(ctx context.Context, pkt *boostpb.Packet_EvictKeys, executor processing.Executor) error {
	var dst = pkt.Link.Dst
	var keys = &pkt.Keys
	var tag = pkt.Tag

	var trigger replay.TriggerEndpoint
	var path []*boostpb.ReplayPathSegment
	if rp, ok := d.replayPaths[tag]; ok {
		trigger = rp.Trigger
		path = rp.Path
	} else {
		d.log.Warn("eviction for tag that has not yet been finalized", tag.Zap())
		return nil
	}

	i := slices.IndexFunc(path, func(ps *boostpb.ReplayPathSegment) bool { return ps.Node == dst })
	if err := d.evictionWalkPath(ctx, path[i:], keys, tag, executor); err != nil {
		return err
	}

	switch trigger.(type) {
	case *replay.TriggerEnd, *replay.TriggerLocal:
		// This path terminates inside the domain. Find the target node, evict
		// from it, and then propagate the eviction further downstream.
		target := path[len(path)-1].Node
		if d.nodes.Get(target).IsReader() {
			return nil
		}
		// No need to continue if node was dropped.
		if d.nodes.Get(target).IsDropped() {
			return nil
		}
		if evicted := d.state.Get(target).EvictKeys(tag, *keys); evicted != nil {
			return d.triggerDownstreamEvictions(ctx, evicted, *keys, target, executor)
		}
	}
	return nil
}

func (d *Domain) handleUpdateEgress(egress *boostpb.Packet_UpdateEgress) error {
	node := egress.Node
	e := d.nodes.Get(node).AsEgress()

	if newtx := egress.NewTx; newtx != nil {
		e.AddTx(graph.NodeIdx(newtx.Node), boostpb.LocalNodeIndex(newtx.Local),
			boostpb.DomainAddr{
				Domain: newtx.Domain.Domain,
				Shard:  newtx.Domain.Shard,
			})
	}
	if newtag := egress.NewTag; newtag != nil {
		e.AddTag(newtag.Tag, newtag.Node)
	}
	return nil
}

func (d *Domain) handleUpdateSharder(update *boostpb.Packet_UpdateSharder) error {
	n := d.nodes.Get(update.Node)
	if sharder := n.AsSharder(); sharder != nil {
		for _, tx := range update.NewTxs {
			sharder.AddShardedTx(tx)
		}
	}
	return nil
}

func (d *Domain) handleReaderReplay(ctx context.Context, replay *boostpb.Packet_RequestReaderReplay) error {
	// the reader could have raced with us filling in the key after some
	// *other* reader requested it, so let's double check that it indeed still
	// misses!

	node := replay.Node
	r := d.nodes.Get(node).AsReader()
	if r == nil {
		panic("reader replay requested for non-reader node")
	}

	w := r.Writer()
	w.Swap()

	keys := make([]boostpb.Row, 0, len(replay.Keys))
	for _, k := range replay.Keys {
		if !w.WithKey(k).Found() {
			keys = append(keys, k)
		}
	}

	missingKeys := keys[:0]
	for _, k := range keys {
		allrows, ok := d.readerTriggered[node]
		if !ok {
			allrows = common.NewRowSet(r.KeySchema())
			d.readerTriggered[node] = allrows
		}
		if allrows.Add(k) {
			missingKeys = append(missingKeys, k)
		}
	}
	if len(missingKeys) > 0 {
		return d.findTagsAndReplay(ctx, missingKeys, replay.Cols, node)
	}
	return nil
}

func (d *Domain) findTagsAndReplay(ctx context.Context, missKeys []boostpb.Row, missCols []int, missIn boostpb.LocalNodeIndex) error {
	missColumns := common.NewColumnSet(missCols)

	var tags []boostpb.Tag
	if candidates, ok := d.replayPathsByDst[missIn]; ok {
		if ts, ok := candidates[missColumns]; ok {
			tags = ts
		}
	}

	for _, tag := range tags {
		// send a message to the source domain(s) responsible
		// for the chosen tag so they'll start replay.
		if _, local := d.replayPaths[tag].Trigger.(*replay.TriggerLocal); local {
			// *in theory* we could just call self.seed_replay, and everything would be good.
			// however, then we start recursing, which could get us into sad situations where
			// we break invariants where some piece of code is assuming that it is the only
			// thing processing at the time (think, e.g., borrow_mut()).
			//
			// for example, consider the case where two misses occurred on the same key.
			// normally, those requests would be deduplicated so that we don't get two replay
			// responses for the same key later. however, the way we do that is by tracking
			// keys we have requested in self.waiting.redos (see `redundant` in
			// `on_replay_miss`). in particular, on_replay_miss is called while looping over
			// all the misses that need replays, and while the first miss of a given key will
			// trigger a replay, the second will not. if we call `seed_replay` directly here,
			// that might immediately fill in this key and remove the entry. when the next miss
			// (for the same key) is then hit in the outer iteration, it will *also* request a
			// replay of that same key, which gets us into trouble with `State::mark_filled`.
			//
			// so instead, we simply keep track of the fact that we have a replay to handle,
			// and then get back to it after all processing has finished (at the bottom of
			// `Self::handle()`)
			var replay = &boostpb.Packet_RequestPartialReplay{
				Tag:      tag,
				Unishard: true,
				Keys:     missKeys,
			}
			if d.shard != nil {
				replay.RequestingShard = *d.shard
			}

			pkt := &boostpb.Packet{
				Inner: &boostpb.Packet_RequestPartialReplay_{
					RequestPartialReplay: replay,
				},
			}
			d.delayForSelf(pkt)
			continue
		}

		// NOTE: due to max_concurrent_replays, it may be that we only replay from *some* of
		// these ancestors now, and some later. this will cause more of the replay to be
		// buffered up at the union above us, but that's probably fine.
		if err := d.requestPartialReplay(ctx, tag, missKeys); err != nil {
			return err
		}
	}

	if len(tags) == 0 {
		panic("no tag found to fill missing value")
	}
	return nil
}

func (d *Domain) requestPartialReplay(ctx context.Context, tag boostpb.Tag, keys []boostpb.Row) error {
	if d.concurrentReplays < d.maxConcurrentReplays {
		return d.sendPartialReplayRequest(ctx, tag, keys)
	}
	d.replayRequestQueue.PushBack(replay.ReplayRequest{Tag: tag, Data: keys})
	return nil
}

func makePartialRequest(shard *uint, tag boostpb.Tag, keys []boostpb.Row, unishard bool) *boostpb.Packet {
	var partialRequest = &boostpb.Packet_RequestPartialReplay{
		Tag:             tag,
		Keys:            keys,
		Unishard:        unishard,
		RequestingShard: 0,
	}
	if shard != nil {
		partialRequest.RequestingShard = *shard
	}
	return &boostpb.Packet{
		Inner: &boostpb.Packet_RequestPartialReplay_{RequestPartialReplay: partialRequest},
	}
}

func (d *Domain) sendPartialReplayRequest(ctx context.Context, tag boostpb.Tag, keys []boostpb.Row) error {
	if trace.T {
		var span *trace.Span
		ctx, span = trace.WithSpan(ctx, "Domain.sendPartialReplayRequest", nil)
		span.Arg("tag", tag)
		span.Arg("keys", keys)
		defer span.Close()
	}

	path, ok := d.replayPaths[tag]
	if !ok {
		panic("asked to replay along non-existing path")
	}

	trigger := path.Trigger.(*replay.TriggerEnd)

	var askShardByKeyI *int
	switch trigger.Source.Kind {
	case replay.SourceSelectionAllShards:
	case replay.SourceSelectionSameShard:
		// note that we "ask all" here because we're not indexing the vector by the
		// key's shard index. unipath will still be set to true though, since
		// options.len() == 1.
	case replay.SourceSelectionKeyShard:
		askShardByKeyI = &trigger.Source.KeyItoShard
	}

	if askShardByKeyI == nil && len(trigger.Options) != 1 {
		d.concurrentReplays++
		request := makePartialRequest(d.shard, tag, keys, false)

		for _, tri := range trigger.Options {
			if err := tri.ProcessAsync(ctx, request); err != nil {
				// we're shutting down, it's fine
				d.log.Error("shard call failed", zap.Error(err))
				return nil
			}
		}
		return nil
	}

	d.concurrentReplays++

	if len(trigger.Options) == 1 {
		request := makePartialRequest(d.shard, tag, keys, true)
		if err := trigger.Options[0].ProcessAsync(ctx, request); err != nil {
			// we're shutting down, it's fine
			d.log.Error("shard call failed", zap.Error(err))
			return nil
		}
	} else if askShardByKeyI != nil {
		var hasher vthash.Hasher
		schema := d.nodes.Get(path.Path[0].Node).Schema()
		shards := make(map[uint][]boostpb.Row)
		for _, key := range keys {
			shard := key.ShardValue(&hasher, *askShardByKeyI, schema[*askShardByKeyI], uint(len(trigger.Options)))
			shards[shard] = append(shards[shard], key)
		}
		for shard, keys := range shards {
			request := makePartialRequest(d.shard, tag, keys, true)
			if err := trigger.Options[shard].ProcessAsync(ctx, request); err != nil {
				// we're shutting down, it's fine
				d.log.Error("shard call failed", zap.Error(err))
				return nil
			}
		}
	} else {
		panic("unreachable")
	}
	return nil
}

func (d *Domain) handleSetupReplayPath(setup *boostpb.SyncPacket_SetupReplayPath, done chan struct{}) error {
	// let coordinator know that we've registered the tagged path
	// TODO@vmg: the original Noria code notifies right at the start of the function;
	// 		is there a reason why this doesn't happen at the end?
	close(done)

	d.log.Debug("SetupReplayPath", setup.Source.Zap(), setup.Tag.Zap())

	trigger, err := replay.TriggerEndpointFromProto(setup.Trigger, d.coordinator, d.shard)
	if err != nil {
		return err
	}

	tag := setup.Tag
	path := setup.Path

	switch trigger.(type) {
	case *replay.TriggerEnd, *replay.TriggerLocal:
		last := path[len(path)-1]

		rpath, ok := d.replayPathsByDst[last.Node]
		if !ok {
			rpath = make(map[common.ColumnSet][]boostpb.Tag)
			d.replayPathsByDst[last.Node] = rpath
		}

		partialKey := common.NewColumnSet(last.PartialKey)
		rpath[partialKey] = append(rpath[partialKey], tag)
	}

	d.replayPaths[tag] = &replay.Path{
		Source:                setup.Source,
		Path:                  path,
		NotifyDone:            setup.NotifyDone,
		PartialUnicastSharder: setup.PartialUnicastSharder,
		Trigger:               trigger,
	}
	return nil
}

func (d *Domain) handlePartialReplay(request *boostpb.Packet_RequestPartialReplay, ex processing.Executor) error {
	for _, key := range request.Keys {
		err := d.seedReplay(request.Tag, key, request.Unishard, request.RequestingShard, ex)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Domain) seedReplay(tag boostpb.Tag, key boostpb.Row, unishard bool, requestingShard uint, ex processing.Executor) error {
	rpath := d.replayPaths[tag]
	switch rpath.Trigger.(type) {
	case *replay.TriggerStart, *replay.TriggerLocal:
		// TODO: maybe delay this seed request so that we can batch respond later?

		ts := tagshard{tag, requestingShard}
		buf, ok := d.bufferedReplayRequests[ts]
		if !ok {
			buf = &bufferedReplay{
				Keys:     make(map[boostpb.Row]bool),
				Unishard: unishard,
			}
			d.bufferedReplayRequests[ts] = buf

			time.AfterFunc(d.replayBatchTimeout, func() {
				d.chanElapsedReplays <- ts
			})
		}

		// TODO: if timer has expired, call seed_all(tag, _, executor) immediately
		buf.Keys[key] = true
		return nil
	}

	// TODO@vmg wtf?
	panic("unreachable")
}

const callerIDUser = "root"

// Caller ID used for upqueries.
var vitessCallerID = &vtrpc.CallerID{
	Principal:    callerIDUser,
	Component:    "vtboost",
	Subcomponent: "upquery",
}

var vtgateCallerID = &querypb.VTGateCallerID{
	Username: callerIDUser,
}

func (d *Domain) buildUpquery(query *strings.Builder, n *flownode.Node) {
	query.WriteString("SELECT ")
	if d.upqueryMode == boostpb.UpqueryMode_SELECT_GTID {
		query.WriteString("@@global.gtid_executed, ")
	}
	for i, f := range n.Fields() {
		if i > 0 {
			query.WriteString(", ")
		}
		sqlescape.WriteEscapeID(query, f)
	}
	query.WriteString(" FROM ")
	sqlescape.WriteEscapeID(query, n.Name)
}

func (d *Domain) buildUpqueryFiltered(n *flownode.Node, cols []int, keys map[boostpb.Row]bool) (string, map[string]*querypb.BindVariable) {
	var (
		query  strings.Builder
		bvars  = make(map[string]*querypb.BindVariable, len(cols))
		fields = n.Fields()
	)

	d.buildUpquery(&query, n)
	query.WriteString(" WHERE ")

	switch {
	case len(cols) == 1 && len(keys) > 1:
		sqlescape.WriteEscapeID(&query, fields[cols[0]])
		query.WriteString(" IN ::tuple0")

		values := make([]*querypb.Value, 0, len(keys))
		for key := range keys {
			values = append(values, sqltypes.ValueToProto(key.ValueAt(0).ToVitessUnsafe()))
		}

		bvars["tuple0"] = &querypb.BindVariable{Type: sqltypes.Tuple, Values: values}

	default:
		var keycount int
		for key := range keys {
			if keycount > 0 {
				query.WriteString(" OR ")
			}
			for i, col := range cols {
				if i > 0 {
					query.WriteString(" AND ")
				}

				sqlescape.WriteEscapeID(&query, fields[col])

				bvar := fmt.Sprintf("arg%d", len(bvars))
				query.WriteString(" = :")
				query.WriteString(bvar)

				bvars[bvar] = sqltypes.ValueBindVariable(key.ValueAt(i).ToVitessUnsafe())
			}
			keycount++
		}
	}

	return query.String(), bvars
}

func (d *Domain) performUpquery(ctx context.Context, queryStr string, bvars map[string]*querypb.BindVariable, callback func(string, []boostpb.Record) error) error {
	d.log.Debug("sending upquery", zap.String("query", queryStr))

	var records []boostpb.Record
	var gtid string
	var session = vtgate.NewSafeSession(nil)

	ctx = callerid.NewContext(ctx, vitessCallerID, vtgateCallerID)

	switch d.upqueryMode {
	case boostpb.UpqueryMode_SELECT_GTID:
		err := d.executor.StreamExecute(ctx, "Boost.Upquery", session, queryStr, bvars, func(result *sqltypes.Result) error {
			records = records[:0]

			for i, row := range result.Rows {
				if i == 0 {
					gtid = row[0].RawStr()
				} else {
					if row[0].RawStr() != gtid {
						return fmt.Errorf("unstable GTID in upquery")
					}
				}
				records = append(records, boostpb.RowFromVitess(row[1:]).ToRecord(true))
			}
			return callback(gtid, records)
		})
		if err != nil {
			return fmt.Errorf("failed to start upquery transaction: %w", err)
		}

	case boostpb.UpqueryMode_TRACK_GTID:
		session.Options = &querypb.ExecuteOptions{
			IncludedFields:       querypb.ExecuteOptions_TYPE_ONLY,
			Workload:             querypb.ExecuteOptions_OLAP,
			TransactionIsolation: querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY,
		}
		err := d.executor.StreamExecute(ctx, "Boost.Upquery", session, queryStr, bvars, func(result *sqltypes.Result) error {
			records = records[:0]

			if gtid == "" {
				gtid = result.SessionStateChanges
			}
			for _, row := range result.Rows {
				records = append(records, boostpb.RowFromVitess(row).ToRecord(true))
			}
			return callback(gtid, records)
		})

		if err != nil {
			return fmt.Errorf("failed to start upquery transaction: %w", err)
		}
	}
	return nil
}

func (d *Domain) seedAllFromExternalBase(ctx context.Context, tag boostpb.Tag, repl *replay.Path, requestingShard uint, keys map[boostpb.Row]bool, unishard bool, executor processing.Executor) error {
	var cols []int

	switch trigger := repl.Trigger.(type) {
	case *replay.TriggerStart:
		cols = trigger.Columns
	case *replay.TriggerLocal:
		cols = trigger.Columns
	default:
		panic("replay path should be Local or Start")
	}

	n := d.nodes.Get(repl.Source)

	slot := <-d.upquerySlots
	externalBase := n.AsExternalBase()
	externalBase.BeginUpquery(ctx, slot, cols, keys)

	go func() {
		var err error
		var replayPiece = &boostpb.Packet_ReplayPiece{
			Link:    &boostpb.Link{Src: repl.Source, Dst: repl.Path[0].Node},
			Tag:     tag,
			Records: nil,
			Context: &boostpb.Packet_ReplayPiece_Partial{Partial: &boostpb.Packet_ReplayPiece_ContextPartial{
				ForKeys:         keys,
				RequestingShard: requestingShard,
				Unishard:        unishard,
				Ignore:          unishard,
			}},
			Gtid:        "",
			UpquerySlot: uint32(slot),
		}

		upquerySQL, upqueryBvars := d.buildUpqueryFiltered(n, cols, keys)

		err = d.performUpquery(ctx, upquerySQL, upqueryBvars, func(gtid string, records []boostpb.Record) error {
			replayPiece.Gtid = gtid
			replayPiece.Records = append(replayPiece.Records, records...)
			return nil
		})
		if err != nil {
			replayPiece.Context = &boostpb.Packet_ReplayPiece_Failed{
				Failed: err.Error(),
			}
			d.log.Error("upquery failed", zap.Error(err))
		}

		d.delayForSelf(&boostpb.Packet{Inner: &boostpb.Packet_ReplayPiece_{ReplayPiece: replayPiece}})
	}()
	return nil
}

func (d *Domain) seedAll(ctx context.Context, tag boostpb.Tag, requestingShard uint, keys map[boostpb.Row]bool, unishard bool, executor processing.Executor) error {
	if trace.T {
		var span *trace.Span
		ctx, span = trace.WithSpan(ctx, "Domain.seedAll", nil)
		span = span.Arg("tag", tag).Arg("requestingShard", requestingShard).Arg("unishard", unishard)
		defer span.Close()
	}

	repl, ok := d.replayPaths[tag]
	if !ok {
		panic("missing replay path")
	}

	if d.nodes.Get(repl.Source).IsExternalBase() {
		return d.seedAllFromExternalBase(ctx, tag, repl, requestingShard, keys, unishard, executor)
	}

	var cols []int

	switch trigger := repl.Trigger.(type) {
	case *replay.TriggerStart:
		cols = trigger.Columns
	case *replay.TriggerLocal:
		cols = trigger.Columns
	default:
		panic("replay path should be Local or Start")
	}

	stat := d.state.Get(repl.Source)
	if stat == nil {
		panic("migration replay path started with non-materialized node")
	}

	var rs []boostpb.Record
	var misses = make(map[boostpb.Row]bool)

	if log := d.log.Check(zapcore.DebugLevel, "looking up in memory"); log != nil {
		log.Write(zap.Uint32("source", uint32(repl.Source)), boostpb.ZapRows("keys", maps.Keys(keys)))
	}

	for key := range keys {
		res, ok := stat.Lookup(cols, key)
		if !ok {
			if log := d.log.Check(zapcore.DebugLevel, "missed lookup"); log != nil {
				log.Write(zap.Uint32("source", uint32(repl.Source)), key.Zap("key"))
			}
			delete(keys, key)
			misses[key] = true
			continue
		}

		if log := d.log.Check(zapcore.DebugLevel, "succeeded lookup"); log != nil {
			log.Write(zap.Int("len", res.Len()), zap.Uint32("source", uint32(repl.Source)), key.Zap("key"))
		}

		res.ForEach(func(r boostpb.Row) {
			rs = append(rs, d.seedRow(repl.Source, r))
		})
	}
	if len(misses) == 0 {
		misses = nil
	}

	var m *boostpb.Packet
	if len(keys) > 0 {
		m = &boostpb.Packet{
			Inner: &boostpb.Packet_ReplayPiece_{ReplayPiece: &boostpb.Packet_ReplayPiece{
				Link:    &boostpb.Link{Src: repl.Source, Dst: repl.Path[0].Node},
				Tag:     tag,
				Records: rs,
				Context: &boostpb.Packet_ReplayPiece_Partial{Partial: &boostpb.Packet_ReplayPiece_ContextPartial{
					ForKeys:         keys,
					RequestingShard: requestingShard,
					Unishard:        unishard,
					Ignore:          unishard,
				}},
			}},
		}
	}

	for key := range misses {
		// we have missed in our lookup, so we have a partial replay through a partial replay
		// trigger a replay to source node, and enqueue this request.
		if err := d.onReplayMiss(ctx, repl.Source, cols, key, key, unishard, requestingShard, tag); err != nil {
			return err
		}
	}

	if m != nil {
		return d.handleReplay(ctx, m, executor)
	}
	return nil
}

func (d *Domain) onReplayMiss(ctx context.Context, missIn boostpb.LocalNodeIndex, missColumns []int, replayKey, missKey boostpb.Row, unishard bool, shard uint, neededFor boostpb.Tag) error {
	// when the replay eventually succeeds, we want to re-do the replay.
	w, ok := d.waiting[missIn]
	if ok {
		delete(d.waiting, missIn)
	} else {
		w = &waiting{
			holes: map[redo]int{},
			redos: map[hole]map[redo]struct{}{},
		}
	}

	redundant := false
	redoEntry := redo{
		tag:             neededFor,
		replayKey:       replayKey,
		requestingShard: shard,
		unishard:        unishard,
	}

	holeEntry := hole{common.NewColumnSet(missColumns), missKey}
	e, found := w.redos[holeEntry]
	if found {
		// we have already requested backfill of this key
		// remember to notify this Redo when backfill completes
		if _, existing := e[redoEntry]; !existing {
			e[redoEntry] = struct{}{}
			// this Redo should wait for this backfill to complete before redoing
			w.holes[redoEntry]++
		}
		redundant = true
	} else {
		// we haven't already requested backfill of this key
		// remember to notify this Redo when backfill completes
		e = map[redo]struct{}{redoEntry: {}}
		w.redos[holeEntry] = e
		// this Redo should wait for this backfill to complete before redoing
		w.holes[redoEntry]++
	}

	d.waiting[missIn] = w
	if redundant {
		return nil
	}

	return d.findTagsAndReplay(ctx, []boostpb.Row{missKey}, missColumns, missIn)
}

func (d *Domain) seedRow(source boostpb.LocalNodeIndex, row boostpb.Row) boostpb.Record {
	if inj, ok := d.ingressInject[source]; ok {
		var v = make([]boostpb.Value, 0, inj.start+len(inj.defaults))
		v = append(v, row.ToValues()...)
		v = append(v, inj.defaults...)
		return boostpb.NewRecord(v, true)
	}

	n := d.nodes.Get(source)
	if b := n.AsBase(); b != nil {
		record := row.ToRecord(true)
		return b.FixRecord(record)
	}
	return row.ToRecord(true)
}

func (d *Domain) finishedPartialReplay(ctx context.Context, tag boostpb.Tag, num int) error {
	if trace.T {
		var span *trace.Span
		ctx, span = trace.WithSpan(ctx, "Domain.finishedPartialReplay", nil)
		span.Arg("tag", tag)
		defer span.Close()
	}

	rp := d.replayPaths[tag]
	switch rp.Trigger.(type) {
	case *replay.TriggerEnd:
		// A backfill request we made to another domain was just satisfied!
		// We can now issue another request from the concurrent replay queue.
		// However, since unions require multiple backfill requests, but produce only one
		// backfill reply, we need to check how many requests we're now free to issue. If
		// we just naively release one slot here, a union with two parents would mean that
		// `self.concurrent_replays` constantly grows by +1 (+2 for the backfill requests,
		// -1 when satisfied), which would lead to a deadlock!
		requestsSatisfied := 0
		last := rp.Path[len(rp.Path)-1]

		if cs, ok := d.replayPathsByDst[last.Node]; ok {
			if tags, ok := cs[common.NewColumnSet(last.PartialKey)]; ok {
				for _, tag := range tags {
					if _, end := d.replayPaths[tag].Trigger.(*replay.TriggerEnd); end {
						requestsSatisfied++
					}
				}
			}
		}

		// we also sent that many requests *per key*.
		requestsSatisfied = requestsSatisfied * num

		d.concurrentReplays -= requestsSatisfied
		if d.concurrentReplays < 0 {
			// TODO: figure out why this can underflow
			d.concurrentReplays = 0
		}

		var perTag = make(map[boostpb.Tag][]boostpb.Row)
		for d.concurrentReplays < d.maxConcurrentReplays {
			if d.replayRequestQueue.Len() == 0 {
				break
			}
			queued := d.replayRequestQueue.PopFront()
			perTag[queued.Tag] = append(perTag[queued.Tag], queued.Data...)
		}

		for tag, keys := range perTag {
			if err := d.sendPartialReplayRequest(ctx, tag, keys); err != nil {
				return err
			}
		}
	case *replay.TriggerLocal:
	// didn't count against our quote, so we're not decrementing
	default:
		panic("unreachable")
	}
	return nil
}

func (d *Domain) handleAddNode(addNode *boostpb.Packet_AddNode) error {
	node := flownode.NodeFromProto(addNode.Node)
	addr := node.LocalAddr()
	d.notReady[addr] = struct{}{}

	for _, p := range addNode.Parents {
		d.nodes.Get(p).AddChild(addr)
	}
	d.nodes.Insert(addr, node)
	return nil
}

func (d *Domain) handleReplayFromInternalNode(ctx context.Context, recordsToReplay []boostpb.Record, tx boostrpc.DomainClient, tag boostpb.Tag, link *boostpb.Link) {
	const BatchSize = 256

	for ctx.Err() == nil && len(recordsToReplay) > 0 {
		var last bool
		var records []boostpb.Record

		if len(recordsToReplay) >= BatchSize {
			records = slices.Clone(recordsToReplay[:BatchSize])
			recordsToReplay = recordsToReplay[BatchSize:]
		} else {
			last = true
			records = slices.Clone(recordsToReplay)
			recordsToReplay = nil
		}

		var pkt = &boostpb.Packet{
			Inner: &boostpb.Packet_ReplayPiece_{ReplayPiece: &boostpb.Packet_ReplayPiece{
				Link:    link.Clone(),
				Tag:     tag,
				Records: records,
				Context: &boostpb.Packet_ReplayPiece_Regular{
					Regular: &boostpb.Packet_ReplayPiece_ContextRegular{
						Last: last,
					},
				},
			}},
		}

		if err := tx.ProcessAsync(ctx, pkt); err != nil {
			d.log.Error("failed to perform replay from internal node", zap.Error(err), tag.Zap())
			return
		}
	}
}

func (d *Domain) handleReplayFromExternalBase(ctx context.Context, n *flownode.Node, tx boostrpc.DomainClient, tag boostpb.Tag, link *boostpb.Link) {
	var upquery strings.Builder
	d.buildUpquery(&upquery, n)

	err := d.performUpquery(ctx, upquery.String(), nil, func(gtid string, records []boostpb.Record) error {
		if len(records) == 0 {
			return nil
		}
		var pkt = &boostpb.Packet{
			Inner: &boostpb.Packet_ReplayPiece_{ReplayPiece: &boostpb.Packet_ReplayPiece{
				Link:    link.Clone(),
				Tag:     tag,
				Gtid:    gtid,
				Records: slices.Clone(records),
				Context: &boostpb.Packet_ReplayPiece_Regular{
					Regular: &boostpb.Packet_ReplayPiece_ContextRegular{
						Last: false,
					},
				},
			}},
		}
		return tx.ProcessAsync(ctx, pkt)
	})

	if err == nil {
		var pkt = &boostpb.Packet{
			Inner: &boostpb.Packet_ReplayPiece_{ReplayPiece: &boostpb.Packet_ReplayPiece{
				Link: link.Clone(),
				Tag:  tag,
				Context: &boostpb.Packet_ReplayPiece_Regular{
					Regular: &boostpb.Packet_ReplayPiece_ContextRegular{
						Last: true,
					}},
			}},
		}
		err = tx.ProcessAsync(ctx, pkt)
	}
	if err != nil {
		d.log.Error("failed to perform replay from external base", zap.Error(err), tag.Zap())
	}
}

func (d *Domain) handleStartReplay(ctx context.Context, startReplay *boostpb.Packet_StartReplay, ex processing.Executor) error {
	if trace.T {
		var span *trace.Span
		ctx, span = trace.WithSpan(ctx, "Domain.handleStartReplay",
			&boostpb.Packet{Inner: &boostpb.Packet_StartReplay_{StartReplay: startReplay}})
		defer span.Close()
	}

	n := d.nodes.Get(startReplay.From)
	external := n.IsExternalBase()

	var recordsToReplay []boostpb.Record
	if !external {
		// we know that the node is materialized, as the migration coordinator
		// picks path that originate with materialized nodes. if this weren't the
		// case, we wouldn't be able to do the replay, and the entire migration
		// would fail.
		//
		// we clone the entire state so that we can continue to occasionally
		// process incoming updates to the domain without disturbing the state that
		// is being replayed.
		recordsToReplay = d.state.Get(startReplay.From).ClonedState()
	}

	link := &boostpb.Link{Src: startReplay.From, Dst: d.replayPaths[startReplay.Tag].Path[0].Node}

	// we're been given an entire state snapshot, but we need to digest it
	// piece by piece spawn off a thread to do that chunking. however, before
	// we spin off that thread, we need to send a single Replay message to tell
	// the target domain to start buffering everything that follows. we can't
	// do that inside the thread, because by the time that thread is scheduled,
	// we may already have processed some other messages that are not yet a
	// part of state.
	var pkt boostpb.Packet
	pkt.Inner = &boostpb.Packet_ReplayPiece_{ReplayPiece: &boostpb.Packet_ReplayPiece{
		Link:    link,
		Tag:     startReplay.Tag,
		Records: nil,
		Context: &boostpb.Packet_ReplayPiece_Regular{
			Regular: &boostpb.Packet_ReplayPiece_ContextRegular{
				Last: len(recordsToReplay) == 0 && !external,
			}},
	}}

	if err := d.handleReplay(ctx, &pkt, ex); err != nil {
		return err
	}

	if external || len(recordsToReplay) > 0 {
		replayTx, err := d.coordinator.GetClient(d.index, d.shardn())
		if err != nil {
			return err
		}

		if external {
			go d.handleReplayFromExternalBase(ctx, n, replayTx, startReplay.Tag, link)
		} else {
			go d.handleReplayFromInternalNode(ctx, recordsToReplay, replayTx, startReplay.Tag, link)
		}
	}

	return nil
}

func (d *Domain) handleFinishReplay(ctx context.Context, tag boostpb.Tag, node boostpb.LocalNodeIndex, ex processing.Executor) error {
	replaying := d.replaying
	d.replaying = nil

	var finished bool
	if replaying != nil {
		replaying.Passes++

		handle := replaying.Buffered.Len()
		if handle > 100 {
			handle /= 2
		}

		for replaying.Buffered.Len() > 0 {
			m := replaying.Buffered.PopFront()

			// some updates were propagated to this node during the migration. we need to
			// replay them before we take even newer updates. however, we don't want to
			// completely block the domain data channel, so we only process a few backlogged
			// updates before yielding to the main loop (which might buffer more things).

			d.replaying = nil
			if err := d.dispatch(ctx, m, ex); err != nil {
				return err
			}

			handle--
			if handle == 0 {
				// we want to make sure we actually drain the backlog we've accumulated
				// but at the same time we don't want to completely stall the system
				// therefore we only handle half the backlog at a time
				break
			}
		}

		finished = replaying.Buffered.Len() == 0
	} else {
		panic("received FinishReplay but nothing was being replayed")
	}

	d.replaying = replaying

	if finished {
		replaying := d.replaying
		d.replaying = nil

		d.log.Info("node is fully up to date", node.Zap(), zap.Int("passes", replaying.Passes), tag.Zap())

		if d.replayPaths[tag].NotifyDone {
			d.finishedReplays <- tag
		}
	} else {
		d.delayForSelf(&boostpb.Packet{
			Inner: &boostpb.Packet_Finish_{
				Finish: &boostpb.Packet_Finish{
					Tag:  tag,
					Node: node,
				},
			},
		})
	}
	return nil
}

func (d *Domain) handleRemoveNodes(pkt *boostpb.Packet_RemoveNodes) error {
	for _, node := range pkt.Nodes {
		d.nodes.Get(node).Remove()
		d.nodes.Remove(node)
		d.log.Debug("node removed", node.Zap())
	}

	for _, node := range pkt.Nodes {
		d.nodes.ForEach(func(n *flownode.Node) bool {
			n.TryRemoveChild(node)
			// NOTE: since nodes are always removed leaves-first, it's not
			// important to update parent pointers here
			return true
		})
	}

	return nil
}

func (d *Domain) handlePurge(_ context.Context, purge *boostpb.Packet_Purge) error {
	node := d.nodes.Get(purge.View)
	d.log.Debug("eagerly purging state from reader", node.GlobalAddr().Zap())

	reader := node.AsReader()
	if reader == nil {
		panic("called Purge on non-Reader node")
	}

	if w := reader.Writer(); w != nil {
		for key := range purge.Keys {
			w.WithKey(key).MarkHole()
		}
		d.dirtyReaderNodes[purge.View] = struct{}{}
	}
	return nil
}

type ReaderID struct {
	Node  graph.NodeIdx
	Shard uint
}

type Readers = common.SyncMap[ReaderID, *backlog.Reader]

const MaxUpqueries = 32

// FromProto creates a Domain from proto data.
func FromProto(
	log *zap.Logger,
	builder *boostpb.DomainBuilder,
	readers *Readers,
	coord *boostrpc.ChannelCoordinator,
	gateway Executor,
	memstats *common.MemoryStats,
) *Domain {
	var nodemap = flownode.NewMapFromProto(builder.Nodes)
	var notReady = make(map[boostpb.LocalNodeIndex]struct{})

	nodemap.ForEach(func(n *flownode.Node) bool {
		notReady[n.LocalAddr()] = struct{}{}
		return true
	})

	var shard *uint
	if builder.NumShards > 1 {
		shard = &builder.Shard
	}

	var upquerySlots = make(chan byte, MaxUpqueries)
	for s := byte(1); s <= MaxUpqueries; s++ {
		upquerySlots <- s
	}

	return &Domain{
		log: log.With(
			zap.Uint64("domain", uint64(builder.Index)),
			zap.Uintp("shard", shard),
		),
		executor: gateway,
		aborted:  make(chan struct{}),

		index:   builder.Index,
		shard:   shard,
		nshards: builder.NumShards,
		nodes:   nodemap,

		state:                  new(state.Map),
		memstats:               memstats,
		rng:                    rand.New(rand.NewSource(time.Now().UnixNano())),
		readerTriggered:        make(map[boostpb.LocalNodeIndex]common.RowSet),
		replayPathsByDst:       make(map[boostpb.LocalNodeIndex]map[common.ColumnSet][]boostpb.Tag),
		replayPaths:            make(map[boostpb.Tag]*replay.Path),
		bufferedReplayRequests: make(map[tagshard]*bufferedReplay),
		concurrentReplays:      0,
		maxConcurrentReplays:   builder.Config.ConcurrentReplays,
		replayBatchTimeout:     builder.Config.ReplayBatchTimeout,
		replayRequestQueue:     deque.New[replay.ReplayRequest](),
		notReady:               notReady,
		ingressInject:          nil,
		replaying:              nil,
		readers:                readers,
		coordinator:            coord,
		waiting:                make(map[boostpb.LocalNodeIndex]*waiting),

		chanPackets:        make(chan *boostpb.Packet, 128),
		chanSyncPackets:    make(chan *syncPacket, 16),
		chanDelayedPackets: make(chan *boostpb.Packet, 128),
		chanElapsedReplays: make(chan tagshard, 16),
		finishedReplays:    make(chan boostpb.Tag, 16),
		dirtyReaderNodes:   make(map[boostpb.LocalNodeIndex]struct{}),

		upquerySlots: upquerySlots,
		upqueryMode:  builder.Config.UpqueryMode,
	}
}

func ToProto(idx boostpb.DomainIndex, shard, numShards uint, nodes *flownode.Map, config *boostpb.DomainConfig) *boostpb.DomainBuilder {
	pbuilder := &boostpb.DomainBuilder{
		Index:     idx,
		Shard:     shard,
		NumShards: numShards,
		Nodes:     nodes.ToProto(),
		Config:    config,
	}
	return pbuilder
}

type BuilderFn func(idx boostpb.DomainIndex, shard, numShards uint, nodes *flownode.Map, config *boostpb.DomainConfig) *boostpb.DomainBuilder
