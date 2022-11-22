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

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow/backlog"
	"vitess.io/vitess/go/boost/dataflow/domain"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/vthash"
)

type Worker struct {
	boostpb.DRPCReaderUnimplementedServer

	log      *zap.Logger
	executor domain.Executor
	topo     *toposerver.Server
	readers  *domain.Readers
	coord    *boostrpc.ChannelCoordinator
	domains  *common.SyncMap[boostpb.DomainAddr, *domain.Domain]
	stream   *EventProcessor
	memstats *common.MemoryStats

	epoch boostpb.Epoch
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

func (w *Worker) ViewRead(ctx context.Context, req *boostpb.ViewReadRequest) (*boostpb.ViewReadResponse, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, w.readTimeout)
	defer cancel()

	w.stats.onRead()
	reader, ok := w.readers.Get(domain.ReaderID{Node: req.TargetNode, Shard: req.TargetShard})
	if !ok {
		return nil, fmt.Errorf("missing reader for node=%d shard=%d", req.TargetNode, req.TargetShard)
	}

	if log := w.log.Check(zapcore.DebugLevel, "ViewRead"); log != nil {
		log.Write(req.Key.Zap("key"))
	}

	var (
		hasher vthash.Hasher
		rs     []boostpb.Row
	)
	ok = backlog.Lookup(reader, &hasher, req.Key, func(rows backlog.Rows) {
		rs = rows.Collect(rs)
	})
	if ok {
		return &boostpb.ViewReadResponse{Rows: rs, Hits: 1}, nil
	}

	if log := w.log.Check(zapcore.DebugLevel, "ViewRead trigger replay"); log != nil {
		log.Write(req.Key.Zap("key"))
	}

	reader.Trigger([]boostpb.Row{req.Key})
	if !req.Block {
		return &boostpb.ViewReadResponse{Hits: 0}, nil
	}

	for ctx.Err() == nil {
		ok = backlog.BlockingLookup(ctx, reader, &hasher, req.Key, func(rows backlog.Rows) {
			rs = rows.Collect(rs)
		})
		if ok {
			return &boostpb.ViewReadResponse{Rows: rs}, nil
		}
	}

	w.log.Warn("ViewRead request timed out", zap.Error(ctx.Err()))
	return nil, ctx.Err()
}

func (w *Worker) ViewReadMany(ctx context.Context, req *boostpb.ViewReadManyRequest) (*boostpb.ViewReadResponse, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, w.readTimeout)
	defer cancel()

	w.stats.onRead()
	reader, ok := w.readers.Get(domain.ReaderID{Node: req.TargetNode, Shard: req.TargetShard})
	if !ok {
		return nil, fmt.Errorf("missing reader for node=%d shard=%d", req.TargetNode, req.TargetShard)
	}

	var (
		hasher  vthash.Hasher
		pending []boostpb.Row
		results boostpb.ViewReadResponse
	)

	if log := w.log.Check(zapcore.DebugLevel, "ViewReadMany"); log != nil {
		log.Write(boostpb.ZapRows("keys", req.Keys))
	}

	pending = backlog.LookupMany(reader, &hasher, req.Keys, func(rows backlog.Rows) {
		results.Rows = rows.Collect(results.Rows)
		results.Hits++
	})
	if len(pending) == 0 {
		return &results, nil
	}

	if log := w.log.Check(zapcore.DebugLevel, "ViewReadMany trigger replay"); log != nil {
		log.Write(boostpb.ZapRows("keys", pending))
	}

	reader.Trigger(pending)
	if !req.Block {
		return &results, nil
	}

	for ctx.Err() == nil && len(pending) > 0 {
		key := pending[len(pending)-1]
		ok = backlog.BlockingLookup(ctx, reader, &hasher, key, func(rows backlog.Rows) {
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

func (w *Worker) ViewSize(context.Context, *boostpb.ViewSizeRequest) (*boostpb.ViewSizeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ViewSize not implemented")
}

type domainNopCloser struct {
	*domain.Domain
}

func (domainNopCloser) Close() {}

func (w *Worker) PlaceDomain(builder *boostpb.DomainBuilder) (*boostpb.AssignDomainResponse, error) {
	domainListener, err := net.Listen("tcp", w.domainListenAddr)
	if err != nil {
		return nil, err
	}

	idx := builder.Index
	shard := builder.Shard

	d := domain.FromProto(w.log, builder, w.readers, w.coord, w.executor, w.memstats)

	// need to register the domain with the local channel coordinator.
	// local first to ensure that we don't unnecessarily give away remote for a
	// local thing if there's a race
	w.coord.InsertLocal(idx, shard, domainNopCloser{d})
	w.coord.InsertRemote(idx, shard, domainListener.Addr().String())

	// store the domain itself in our list of domains
	w.domains.Set(boostpb.DomainAddr{Domain: idx, Shard: shard}, d)

	replica := NewReplica(d, w.coord)
	go replica.Reactor(w.ctx)

	go func() {
		m := drpcmux.New()
		err := boostpb.DRPCRegisterInnerDomain(m, replica)
		if err != nil {
			panic(err)
		}

		if err := drpcserver.New(m).Serve(w.ctx, domainListener); err != nil {
			w.log.Error("domainServer.Serve() failed", zap.Error(err))
		}
	}()

	return &boostpb.AssignDomainResponse{
		Shard: shard,
		Addr:  domainListener.Addr().String(),
	}, nil
}

func (w *Worker) start(ctx context.Context, globalCfg *boostpb.Config, globalAddress string) error {
	w.ctx, w.cancel = context.WithCancel(ctx)

	readerListener, err := net.Listen("tcp", w.readerListenAddr)
	if err != nil {
		return err
	}

	go func() {
		m := drpcmux.New()
		err := boostpb.DRPCRegisterReader(m, w)
		if err != nil {
			panic(err)
		}

		if err := drpcserver.New(m).Serve(w.ctx, readerListener); err != nil {
			w.log.Error("grpc.Serve failed", zap.Error(err))
		}
	}()

	w.log.Info("started inner worker",
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

func (w *Worker) AssignTables(request *boostpb.AssignStreamRequest) error {
	return w.stream.AssignTables(w.ctx, request.Tables)
}
