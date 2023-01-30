package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/boostrpc/service"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain"
	"vitess.io/vitess/go/boost/dataflow/view"
	"vitess.io/vitess/go/boost/server/controller/config"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	"vitess.io/vitess/go/netutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/srvtopo"
)

type Server struct {
	service.DRPCWorkerServiceUnimplementedServer

	uuid       uuid.UUID
	globalAddr string

	log      *zap.Logger
	cfg      *config.Config
	topo     *toposerver.Server
	active   *Worker
	coord    *boostrpc.ChannelCoordinator
	executor domain.Executor
	resolver Resolver
	mu       sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

func NewServer(log *zap.Logger, id uuid.UUID, topo *toposerver.Server, cfg *config.Config) *Server {
	worker := &Server{
		log:    log.With(zap.String("worker_id", id.String())),
		uuid:   id,
		active: nil,
		coord:  boostrpc.NewChannelCoordinator(),
		topo:   topo,
		cfg:    cfg,
	}
	worker.ctx, worker.cancel = context.WithCancel(context.Background())
	return worker
}

func (srv *Server) SetGlobalAddress(addr string) {
	srv.globalAddr = addr
}

func (srv *Server) SetResolver(resolver Resolver) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.resolver = resolver
}

func (srv *Server) SetExecutor(executor domain.Executor) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.executor = executor
}

func (srv *Server) AssignStream(ctx context.Context, request *service.AssignStreamRequest) (*service.AssignStreamResponse, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.active == nil {
		return nil, fmt.Errorf("cannot assign stream without an active worker")
	}
	if err := srv.active.AssignTables(request); err != nil {
		return nil, err
	}
	return &service.AssignStreamResponse{}, nil
}

func (srv *Server) AssignDomain(ctx context.Context, request *service.AssignDomainRequest) (*service.AssignDomainResponse, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.active == nil {
		return nil, fmt.Errorf("AssignDomain on stopped worker")
	}
	if request.From.Epoch != srv.active.epoch {
		return nil, fmt.Errorf("AssignDomain from wrong epoch")
	}
	resp, err := srv.active.PlaceDomain(request.Domain)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (srv *Server) DomainBooted(ctx context.Context, req *service.DomainBootedRequest) (*service.DomainBootedResponse, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if st := srv.active; st != nil {
		if req.From.Epoch == st.epoch {
			srv.log.Info("found about new domain", req.Domain.Zap())
			srv.coord.InsertRemote(req.Domain.Id, req.Domain.Shard, req.Domain.Addr)
		}
	}
	return &service.DomainBootedResponse{}, nil
}

func (srv *Server) LeaderChange(st *vtboostpb.ControllerState) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.active != nil {
		srv.active.Stop()
		srv.active = nil
	}

	if srv.ctx.Err() != nil {
		return
	}

	srv.log.Info("LeaderChange", zap.Int64("new_epoch", st.Epoch))

	stats := newWorkerStats(srv.uuid)

	host, _, err := netutil.SplitHostPort(srv.globalAddr)
	if err != nil {
		srv.log.Error("failed to parse listening address", zap.Error(err))
		return
	}
	srv.active = &Worker{
		log:              srv.log.With(zap.Int64("epoch", st.Epoch)),
		uuid:             srv.uuid,
		epoch:            service.Epoch(st.Epoch),
		readers:          common.NewSyncMap[domain.ReaderID, *view.Reader](),
		domains:          common.NewSyncMap[dataflow.DomainAddr, *domain.Domain](),
		memstats:         common.NewMemStats(),
		coord:            srv.coord,
		executor:         srv.executor,
		topo:             srv.topo,
		stats:            stats,
		stream:           NewEventProcessor(srv.log, stats, srv.coord, srv.resolver),
		domainListenAddr: netutil.JoinHostPort(host, 0),
		readerListenAddr: netutil.JoinHostPort(host, 0),
		readTimeout:      srv.cfg.WorkerReadTimeout,
	}

	if err := srv.active.start(srv.ctx, srv.cfg, srv.globalAddr); err != nil {
		srv.log.Error("failed to start worker on leader change", zap.Error(err))
		srv.active = nil
	}
}

func (srv *Server) ActiveDomains() *common.SyncMap[dataflow.DomainAddr, *domain.Domain] {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.active != nil {
		return srv.active.domains
	}
	return nil
}

func (srv *Server) MemoryStats(_ context.Context, _ *service.MemoryStatsRequest) (*service.MemoryStatsResponse, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.active == nil {
		return &service.MemoryStatsResponse{}, nil
	}
	return srv.active.memstats.ToProto(), nil
}

func (srv *Server) UUID() uuid.UUID {
	return srv.uuid
}

func (srv *Server) Stop() {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.cancel()
}

func (srv *Server) IsReady() bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.active != nil
}

type Resolver interface {
	GetAllShards(ctx context.Context, keyspace string, tabletType topodatapb.TabletType) ([]*srvtopo.ResolvedShard, *topodatapb.SrvKeyspace, error)
}
