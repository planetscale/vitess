package server

import (
	"context"
	"net"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/server/controller"
	"vitess.io/vitess/go/boost/server/worker"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtboost"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate"
	vtschema "vitess.io/vitess/go/vt/vtgate/schema"

	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

type Server struct {
	Topo       *toposerver.Server
	Controller *controller.Server
	Worker     *worker.Server

	cleanup func()
}

func (s *Server) StartLeadershipCampaign(ctx context.Context, state *vtboost.ControllerState) {
	s.Controller.StartLeaderCampaign(ctx, state)
}

func (s *Server) NewLeader(state *vtboost.ControllerState) {
	s.Worker.LeaderChange(state)
}

func (s *Server) Stop() {
	s.Controller.Stop()
	s.Worker.Stop()
	if s.cleanup != nil {
		s.cleanup()
	}
}

func NewBoostInstance(log *zap.Logger, ts *topo.Server, tmc toposerver.TabletManager, config *boostpb.Config, clusterID string) *Server {
	if config == nil {
		config = boostpb.DefaultConfig()
	}

	tp := toposerver.NewTopoServer(log, ts, tmc, clusterID)
	instanceID := uuid.New()

	server := &Server{
		Topo:       tp,
		Controller: controller.NewServer(log, instanceID, tp, config),
		Worker:     worker.NewServer(log, instanceID, tp, config),
	}
	return server
}

func addKeyspaceToTracker(ctx context.Context, log *zap.Logger, srvResolver *srvtopo.Resolver, st *vtschema.Tracker, gw *vtgate.TabletGateway) {
	keyspaces, err := srvResolver.GetAllKeyspaces(ctx)
	if err != nil {
		log.Warn("unable to get all keyspaces", zap.Error(err))
		return
	}
	if len(keyspaces) == 0 {
		log.Info("no keyspace to load")
	}
	for _, keyspace := range keyspaces {
		resolveAndLoadKeyspace(ctx, log, srvResolver, st, gw, keyspace)
	}
}

func resolveAndLoadKeyspace(ctx context.Context, log *zap.Logger, srvResolver *srvtopo.Resolver, st *vtschema.Tracker, gw *vtgate.TabletGateway, keyspace string) {
	dest, err := srvResolver.ResolveDestination(ctx, keyspace, topodatapb.TabletType_PRIMARY, key.DestinationAllShards{})
	if err != nil {
		log.Warn("unable to resolve destination", zap.Error(err))
		return
	}

	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			log.Warn("unable to get initial schema reload for keyspace", zap.String("ks", keyspace))
			return
		case <-time.After(500 * time.Millisecond):
			for _, shard := range dest {
				err := st.AddNewKeyspace(gw, shard.Target)
				if err == nil {
					return
				}
			}
		}
	}
}

func (s *Server) ConfigureVitessExecutor(ctx context.Context, log *zap.Logger, ts *topo.Server, localCell string, schemaTrackingUser string) error {
	resilientServer := srvtopo.NewResilientServer(ts, "ResilientSrvTopoServer")
	log.Info("configuring external gateway for upqueries", zap.String("cell", localCell), zap.String("cells_to_watch", *vtgate.CellsToWatch))
	gateway := vtgate.NewTabletGateway(ctx, nil, resilientServer, localCell)

	tabletTypesToWait := []topodatapb.TabletType{topodatapb.TabletType_PRIMARY}
	srvResolver := srvtopo.NewResolver(resilientServer, gateway, localCell)

	if err := gateway.WaitForTablets(tabletTypesToWait); err != nil {
		return err
	}

	const DefaultNormalizeQueries = true
	const DefaultWarnSharedOnly = false
	const DefaultStreamBufferSize = 32 * 1024
	const DefaultNoScatter = false
	const DefaultPlannerVersion = querypb.ExecuteOptions_Gen4
	const DefaultTxMode = vtgatepb.TransactionMode_MULTI

	tracker := vtschema.NewTracker(gateway.HealthCheck().Subscribe(), &schemaTrackingUser)
	addKeyspaceToTracker(ctx, log, srvResolver, tracker, gateway)

	tc := vtgate.NewTxConn(gateway, DefaultTxMode)
	sc := vtgate.NewScatterConn("", tc, gateway)
	resolver := vtgate.NewResolver(srvResolver, resilientServer, localCell, sc)

	executor := vtgate.NewExecutor(ctx,
		resilientServer,
		localCell,
		resolver,
		DefaultNormalizeQueries,
		DefaultWarnSharedOnly,
		DefaultStreamBufferSize,
		cache.DefaultConfig,
		tracker,
		DefaultNoScatter,
		DefaultPlannerVersion,
	)

	s.Worker.SetExecutor(executor)
	s.Worker.SetResolver(srvResolver)

	s.cleanup = func() {
		executor.Close()
		tracker.Stop()
		_ = gateway.Close(context.Background())
	}

	tracker.Start()
	return nil
}

func (s *Server) Serve(ctx context.Context, listen net.Listener) error {
	m := drpcmux.New()
	if err := vtboost.DRPCRegisterControllerService(m, s.Controller); err != nil {
		return err
	}
	if err := boostpb.DRPCRegisterWorkerService(m, s.Worker); err != nil {
		return err
	}

	s.Worker.SetGlobalAddress(listen.Addr().String())

	var wg errgroup.Group
	wg.Go(func() error {
		return s.Topo.WatchLeadership(ctx, s, listen.Addr().String())
	})
	wg.Go(func() error {
		return drpcserver.New(m).Serve(ctx, listen)
	})

	defer s.Stop()
	return wg.Wait()
}
