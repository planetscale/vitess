package worker

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
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
}

func (w *Worker) Stop() {
	w.cancel()
}

const FailFast = false

func (w *Worker) ViewRead(ctx context.Context, req *boostpb.ViewReadRequest) (*boostpb.ViewReadResponse, error) {
	serializeRows := func(rows backlog.Rows) []boostpb.Row {
		return rows.Collect(nil)
	}

	w.stats.onRead()
	reader, ok := w.readers.Get(domain.ReaderID{Node: req.TargetNode, Shard: req.TargetShard})
	if !ok {
		return nil, fmt.Errorf("missing reader for node=%d shard=%d", req.TargetNode, req.TargetShard)
	}

	var hasher vthash.Hasher
	rs, ok := backlog.Lookup(reader, &hasher, req.Key, serializeRows)
	if ok {
		return &boostpb.ViewReadResponse{Rows: rs, Hit: true}, nil
	}

	var misses = []boostpb.Row{req.Key}
	w.log.Debug("trigger initial reader", boostpb.ZapRows("keys", misses))
	reader.Trigger(misses)
	if !req.Block {
		return &boostpb.ViewReadResponse{Hit: false}, nil
	}

	for ctx.Err() == nil {
		rs, ok = backlog.BlockingLookup(ctx, reader, &hasher, req.Key, serializeRows)
		if ok {
			return &boostpb.ViewReadResponse{Rows: rs}, nil
		}
	}

	w.log.Warn("request timed out", zap.Error(ctx.Err()))
	return nil, ctx.Err()
}

func (w *Worker) ViewReadMany(ctx context.Context, req *boostpb.ViewReadManyRequest) (*boostpb.ViewReadManyResponse, error) {
	w.stats.onRead()
	reader, ok := w.readers.Get(domain.ReaderID{Node: req.TargetNode, Shard: req.TargetShard})
	if !ok {
		return nil, fmt.Errorf("missing reader for node=%d shard=%d", req.TargetNode, req.TargetShard)
	}

	var (
		hasher  vthash.Hasher
		pending []int
		keys    []boostpb.Row
		results = make([]*boostpb.ViewReadResponse, len(req.Keys))
	)

	for i, key := range req.Keys {
		rs, ok := backlog.Lookup(reader, &hasher, key, func(rows backlog.Rows) *boostpb.ViewReadResponse {
			return &boostpb.ViewReadResponse{Rows: rows.Collect(nil), Hit: true}
		})
		if ok {
			results[i] = rs
		} else {
			pending = append(pending, i)
			keys = append(keys, key)
		}
	}

	if len(keys) == 0 {
		return &boostpb.ViewReadManyResponse{Results: results}, nil
	}

	w.log.Debug("trigger initial reader", boostpb.ZapRows("keys", keys))
	reader.Trigger(keys)
	if !req.Block {
		return &boostpb.ViewReadManyResponse{Results: results}, nil
	}

	for ctx.Err() == nil && len(pending) > 0 {
		i := pending[len(pending)-1]
		key := keys[len(keys)-1]

		rs, ok := backlog.BlockingLookup(ctx, reader, &hasher, key, func(rows backlog.Rows) *boostpb.ViewReadResponse {
			return &boostpb.ViewReadResponse{Rows: rows.Collect(nil)}
		})
		if ok {
			results[i] = rs
			pending = pending[:len(pending)-1]
			keys = keys[:len(keys)-1]
		} else {
			break
		}
	}

	if len(pending) == 0 {
		return &boostpb.ViewReadManyResponse{Results: results}, nil
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
