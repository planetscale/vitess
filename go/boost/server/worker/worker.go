package worker

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"

	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/boostrpc/service"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain"
	"vitess.io/vitess/go/boost/dataflow/view"
	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/boost/sql"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/vthash"
)

type Worker struct {
	service.DRPCReaderUnimplementedServer

	log      *zap.Logger
	executor domain.Executor
	topo     *toposerver.Server
	readers  *domain.Readers
	coord    *boostrpc.ChannelCoordinator
	domains  *common.SyncMap[dataflow.DomainAddr, *domain.Domain]
	stream   *EventProcessor
	memstats *common.MemoryStats

	epoch service.Epoch
	uuid  uuid.UUID
	stats *workerStats

	ctx    context.Context
	cancel context.CancelFunc

	domainListenAddr string
	readerListenAddr string

	readTimeout time.Duration
}

func (w *Worker) Stop() {
	w.cancel()
}

func (w *Worker) reader(node dataflow.NodeIdx, shard uint) (view.Reader, error) {
	w.stats.onRead()
	reader, ok := w.readers.Get(domain.ReaderID{Node: node, Shard: shard})
	if !ok {
		return nil, fmt.Errorf("missing reader for node=%d shard=%d", node, shard)
	}
	return reader, nil
}

func (w *Worker) ViewRead(ctx context.Context, req *service.ViewReadRequest) (*service.ViewReadResponse, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, w.readTimeout)
	defer cancel()

	reader, err := w.reader(req.TargetNode, req.TargetShard)
	if err != nil {
		return nil, err
	}

	if log := w.log.Check(zapcore.DebugLevel, "ViewRead"); log != nil {
		log.Write(req.Key.Zap("key"), req.TargetNode.Zap())
	}

	switch reader := reader.(type) {
	case *view.MapReader:
		return w.viewReadMap(ctx, reader, req.Key, req.Block)
	case *view.TreeReader:
		return w.viewReadRange(ctx, reader, req.Key)
	default:
		panic("unexpected reader type")
	}
}

func (w *Worker) viewReadRange(_ context.Context, reader *view.TreeReader, key sql.Row) (*service.ViewReadResponse, error) {
	lower, upper, err := reader.Bounds(key)
	if err != nil {
		return nil, err
	}
	var rs []sql.Row
	reader.LookupRange(lower, upper, func(rows view.Rows) {
		rs = rows.Collect(rs)
	})
	return &service.ViewReadResponse{Rows: rs, Hits: 1}, nil
}

func (w *Worker) viewReadMap(ctx context.Context, reader *view.MapReader, key sql.Row, block bool) (*service.ViewReadResponse, error) {
	var (
		hasher vthash.Hasher
		rs     []sql.Row
	)
	ok := reader.Lookup(&hasher, key, func(rows view.Rows) {
		rs = rows.Collect(rs)
	})
	if ok {
		return &service.ViewReadResponse{Rows: rs, Hits: 1}, nil
	}

	if log := w.log.Check(zapcore.DebugLevel, "ViewRead trigger replay"); log != nil {
		log.Write(key.Zap("key"))
	}

	reader.Trigger([]sql.Row{key})
	if !block {
		return &service.ViewReadResponse{Hits: 0}, nil
	}

	for ctx.Err() == nil {
		ok = reader.BlockingLookup(ctx, &hasher, key, func(rows view.Rows) {
			rs = rows.Collect(rs)
		})
		if ok {
			return &service.ViewReadResponse{Rows: rs}, nil
		}
	}

	w.log.Warn("ViewRead request timed out", zap.Error(ctx.Err()), key.Zap("key"))
	return nil, ctx.Err()
}

func (w *Worker) ViewReadMany(ctx context.Context, req *service.ViewReadManyRequest) (*service.ViewReadResponse, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, w.readTimeout)
	defer cancel()

	var (
		hasher  vthash.Hasher
		pending []sql.Row
		results service.ViewReadResponse
	)

	anyreader, err := w.reader(req.TargetNode, req.TargetShard)
	if err != nil {
		return nil, err
	}

	reader := anyreader.(*view.MapReader)

	if log := w.log.Check(zapcore.DebugLevel, "ViewReadMany"); log != nil {
		log.Write(sql.ZapRows("keys", req.Keys))
	}

	pending = reader.LookupMany(&hasher, req.Keys, func(rows view.Rows) {
		results.Rows = rows.Collect(results.Rows)
		results.Hits++
	})
	if len(pending) == 0 {
		return &results, nil
	}

	if log := w.log.Check(zapcore.DebugLevel, "ViewReadMany trigger replay"); log != nil {
		log.Write(sql.ZapRows("keys", pending))
	}

	reader.Trigger(pending)
	if !req.Block {
		return &results, nil
	}

	for ctx.Err() == nil && len(pending) > 0 {
		key := pending[len(pending)-1]
		ok := reader.BlockingLookup(ctx, &hasher, key, func(rows view.Rows) {
			results.Rows = rows.Collect(results.Rows)
		})
		if ok {
			pending = pending[:len(pending)-1]
		}
	}

	w.log.Debug("ViewReadMany finished", zap.Int("pending", len(pending)), zap.Int("results", len(results.Rows)))
	if len(pending) == 0 {
		return &results, nil
	}
	return nil, ctx.Err()
}

func (w *Worker) ViewSize(context.Context, *service.ViewSizeRequest) (*service.ViewSizeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ViewSize not implemented")
}

func (w *Worker) PlaceDomain(builder *service.DomainBuilder) (*service.AssignDomainResponse, error) {
	domainListener, err := net.Listen("tcp", w.domainListenAddr)
	if err != nil {
		return nil, err
	}

	idx := builder.Index
	shard := builder.Shard

	d := domain.FromProto(w.log, builder, w.readers, w.coord, w.executor, w.memstats)

	if err := d.OnDeploy(); err != nil {
		return nil, err
	}

	replica := NewReplica(d, w.coord)

	// need to register the domain with the local channel coordinator.
	// local first to ensure that we don't unnecessarily give away remote for a
	// local thing if there's a race
	w.coord.InsertLocal(idx, shard, replica)
	w.coord.InsertRemote(idx, shard, domainListener.Addr().String())

	// store the domain itself in our list of domains
	w.domains.Set(dataflow.DomainAddr{Domain: idx, Shard: shard}, d)

	go replica.Reactor(w.ctx)

	go func() {
		m := drpcmux.New()
		err := packet.DRPCRegisterDomain(m, replica)
		if err != nil {
			w.log.Error("packet.DRPCRegisterDomain() failed", zap.Error(err))
			return
		}

		if err := drpcserver.New(m).Serve(w.ctx, domainListener); err != nil {
			w.log.Error("domainServer.Serve() failed", zap.Error(err))
		}
	}()

	return &service.AssignDomainResponse{
		Shard: shard,
		Addr:  domainListener.Addr().String(),
	}, nil
}

func (w *Worker) start(ctx context.Context, globalCfg *config.Config, globalAddress string) error {
	w.ctx, w.cancel = context.WithCancel(ctx)

	readerListener, err := net.Listen("tcp", w.readerListenAddr)
	if err != nil {
		return err
	}

	go func() {
		m := drpcmux.New()
		err := service.DRPCRegisterReader(m, w)
		if err != nil {
			panic(err)
		}

		if err := drpcserver.New(m).Serve(w.ctx, readerListener); err != nil {
			w.log.Error("grpc.Serve failed", zap.Error(err))
		}
	}()

	w.log.Debug("started inner worker",
		zap.String("reader_addr", readerListener.Addr().String()),
		zap.String("ctrl_addr", globalAddress),
	)
	err = w.topo.CreateWorker(w.ctx, int64(w.epoch), &vtboostpb.TopoWorkerEntry{
		Uuid:       w.uuid.String(),
		AdminAddr:  globalAddress,
		ReaderAddr: readerListener.Addr().String(),
	})
	if err != nil {
		w.log.Error("failed to register worker with topology", zap.Error(err))
		return err
	}

	if globalCfg.HeartbeatEvery != 0 {
		go func() {
			tick := time.NewTicker(globalCfg.HeartbeatEvery)
			defer tick.Stop()

			for {
				select {
				case <-w.ctx.Done():
					return
				case <-tick.C:
					err := w.topo.WorkerHeartbeat(w.ctx, w.uuid.String())
					if err != nil {
						w.log.Error("worker heartbeat failed", zap.Error(err))
					}
				}
			}
		}()
	}

	return nil
}

func (w *Worker) AssignTables(request *service.AssignStreamRequest) error {
	return w.stream.AssignTables(w.ctx, request.Tables)
}
