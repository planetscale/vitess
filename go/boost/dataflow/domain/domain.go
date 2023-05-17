package domain

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/boostrpc/service"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/common/deque"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/dataflow/view"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Executor interface {
	StreamExecute(ctx context.Context, method string, safeSession *vtgate.SafeSession, sql string, bindVars map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error
	CloseSession(ctx context.Context, safeSession *vtgate.SafeSession) error
	VSchema() *vindexes.VSchema
}

type Replaying struct {
	To       dataflow.LocalNodeIdx
	Buffered *deque.Deque[packet.ActiveFlowPacket]
	Passes   int
}

type tagshard struct {
	Tag   dataflow.Tag
	Shard uint
}

type bufferedReplay struct {
	Keys     map[sql.Row]bool
	Unishard bool
}

type injection struct {
	start    int
	defaults []sql.Value
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
	tag             dataflow.Tag
	replayKey       sql.Row
	requestingShard uint
	unishard        bool
	waiters         bool
}

type hole struct {
	columns common.Columns
	key     sql.Row
}

type syncPacket struct {
	pkt  packet.SyncPacket
	done chan struct{}
}

type Domain struct {
	log      *zap.Logger
	executor Executor

	// aborted signals when Domain.Reactor has been canceled by context.
	aborted chan struct{}

	index   dataflow.DomainIdx
	shard   *uint
	nshards uint

	nodes    *flownode.Map
	state    *state.Map
	memstats *common.MemoryStats
	rng      *rand.Rand

	notReady      map[dataflow.LocalNodeIdx]struct{}
	ingressInject map[dataflow.LocalNodeIdx]injection

	replaying       *Replaying
	waiting         map[dataflow.LocalNodeIdx]*waiting
	replayPaths     map[dataflow.Tag]*replay.Path
	readerTriggered map[dataflow.LocalNodeIdx]*sql.RowSet
	finishedReplays chan dataflow.Tag

	replayPathsByDst      map[dataflow.LocalNodeIdx]map[common.Columns][]dataflow.Tag
	filterReplayPathByDst map[dataflow.LocalNodeIdx]map[dataflow.Tag]bool

	concurrentReplays    int
	maxConcurrentReplays int
	replayRequestQueue   *deque.Deque[replay.ReplayRequest]

	readers     *common.SyncMap[ReaderID, view.Reader]
	coordinator *boostrpc.ChannelCoordinator

	bufferedReplayRequests map[tagshard]*bufferedReplay
	replayBatchTimeout     time.Duration

	chanPackets        chan packet.AsyncPacket
	chanSyncPackets    chan *syncPacket
	chanDelayedPackets chan packet.AsyncPacket
	chanElapsedReplays chan tagshard

	upquerySlots chan uint8
	upqueryMode  config.UpqueryMode
	gtidTrackers *dataflow.Map[flownode.GtidTracker]

	stats domainStats
}

func (d *Domain) Index() dataflow.DomainIdx {
	return d.index
}

func (d *Domain) Shard() *uint {
	return d.shard
}

func (d *Domain) Nodes() []*flownode.Node {
	return d.nodes.Values()
}

func (d *Domain) shardn() uint {
	if d.shard == nil {
		return 0
	}
	return *d.shard
}

func (d *Domain) OnDeploy() error {
	var errs []error
	d.nodes.ForEach(func(_ dataflow.LocalNodeIdx, n *flownode.Node) bool {
		if err := n.OnDeploy(); err != nil {
			errs = append(errs, err)
		}
		return true
	})
	return multierr.Combine(errs...)
}

type ReaderID struct {
	Node  graph.NodeIdx
	Shard uint
}

type Readers = common.SyncMap[ReaderID, view.Reader]

const MaxUpqueries = 32

// FromProto creates a Domain from proto data.
func FromProto(
	log *zap.Logger,
	worker uuid.UUID,
	builder *service.DomainBuilder,
	readers *Readers,
	coord *boostrpc.ChannelCoordinator,
	gateway Executor,
	memstats *common.MemoryStats,
) *Domain {
	var nodemap = flownode.MapFromProto(builder.Nodes)
	var notReady = make(map[dataflow.LocalNodeIdx]struct{})

	nodemap.ForEach(func(local dataflow.LocalNodeIdx, _ *flownode.Node) bool {
		notReady[local] = struct{}{}
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
		readerTriggered:        make(map[dataflow.LocalNodeIdx]*sql.RowSet),
		replayPathsByDst:       make(map[dataflow.LocalNodeIdx]map[common.Columns][]dataflow.Tag),
		filterReplayPathByDst:  make(map[dataflow.LocalNodeIdx]map[dataflow.Tag]bool),
		replayPaths:            make(map[dataflow.Tag]*replay.Path),
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
		waiting:                make(map[dataflow.LocalNodeIdx]*waiting),

		chanPackets:        make(chan packet.AsyncPacket, 128),
		chanSyncPackets:    make(chan *syncPacket, 16),
		chanDelayedPackets: make(chan packet.AsyncPacket, 128),
		chanElapsedReplays: make(chan tagshard, 16),
		finishedReplays:    make(chan dataflow.Tag, 16),

		upquerySlots: upquerySlots,
		upqueryMode:  builder.Config.UpqueryMode,
		gtidTrackers: new(dataflow.Map[flownode.GtidTracker]),

		stats: domainStats{
			labels: []string{worker.String(), fmt.Sprintf("%d-%d", builder.Index, builder.Shard)},
		},
	}
}

func ToProto(idx dataflow.DomainIdx, shard, numShards uint, nodes *flownode.Map, config *config.Domain) (*service.DomainBuilder, error) {
	pbuilder := &service.DomainBuilder{
		Index:     idx,
		Shard:     shard,
		NumShards: numShards,
		Nodes:     flownode.MapToProto(nodes),
		Config:    config,
	}
	return pbuilder, nil
}

type BuilderFn func(idx dataflow.DomainIdx, shard, numShards uint, nodes *flownode.Map, config *config.Domain) (*service.DomainBuilder, error)
