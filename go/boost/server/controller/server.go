package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
)

type Server struct {
	vtboostpb.DRPCControllerServiceUnimplementedServer

	topo *toposerver.Server

	mu     sync.Mutex
	log    *zap.Logger
	inner  *Controller
	cancel context.CancelFunc

	uuid uuid.UUID
	cfg  *boostpb.Config

	wg errgroup.Group
}

func NewServer(log *zap.Logger, id uuid.UUID, ts *toposerver.Server, config *boostpb.Config) *Server {
	return &Server{topo: ts, log: log, uuid: id, cfg: config}
}

func (srv *Server) RegisterWorker(ctx context.Context, worker *vtboostpb.TopoWorkerEntry) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.inner == nil {
		return
	}

	srv.inner.RegisterWorker(worker)
	srv.checkForReady(ctx)
}

func (srv *Server) checkForReady(ctx context.Context) {
	if srv.inner.workersReady {
		return
	}
	if srv.inner.IsReady() {
		srv.inner.workersReady = true
		srv.wg.Go(func() error {
			return srv.topo.WatchRecipeChanges(ctx, srv)
		})
	}
}

func (srv *Server) waitForClusterState(ctx context.Context) error {
	srv.log.Debug("waiting for Self in cluster state...")

	state, err := srv.topo.WaitForSelfInClusterState(ctx)
	if err != nil {
		return err
	}

	srv.log.Info("found self in ClusterStates",
		zap.String("self_uuid", state.Uuid),
		zap.Uint32("expected_worker_count", state.ExpectedWorkerCount))

	srv.mu.Lock()
	defer srv.mu.Unlock()

	if ctx.Err() == nil && srv.inner != nil {
		srv.inner.expectedWorkerCount = int(state.ExpectedWorkerCount)
		srv.checkForReady(ctx)
	}

	return nil
}

// GetTableDescriptor_ returns the internal table descriptor for the given table.
// This function is only exported to be usable by integration tests
func (srv *Server) GetTableDescriptor_(name string) (*boostpb.TableDescriptor, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	bld := srv.inner.tableDescriptor(name)
	if bld == nil {
		return nil, fmt.Errorf("unknown table: %q", name)
	}
	return bld, nil
}

// GetViewDescriptor_ returns the internal view descriptor for the given view.
// This function is only exported to be usable by integration tests
func (srv *Server) GetViewDescriptor_(name string) (*vtboostpb.Materialization_ViewDescriptor, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	bld := srv.inner.viewDescriptorForName(name)
	if bld == nil {
		return nil, fmt.Errorf("unknown view: %q", name)
	}
	return bld, nil
}

func (srv *Server) Migrate(ctx context.Context, perform func(ctx context.Context, mig *Migration) error) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.inner == nil {
		panic("tried to migrate without being a leader")
	}

	mig := NewMigration(srv.inner)
	if err := perform(ctx, mig); err != nil {
		return err
	}
	return mig.Commit(ctx, nil)
}

func (srv *Server) IsReady() bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.inner != nil && srv.inner.IsReady()
}

func (srv *Server) Inner() *Controller {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.inner
}

func (srv *Server) ReadyCheck(context.Context, *vtboostpb.ReadyRequest) (*vtboostpb.ReadyResponse, error) {
	return &vtboostpb.ReadyResponse{Ready: srv.IsReady()}, nil
}

func (srv *Server) StartLeaderCampaign(ctx context.Context, state *vtboostpb.ControllerState) {
	srv.log.Info("Starting leader campaign", zap.Int64("epoch", state.Epoch))

	srv.mu.Lock()
	srv.inner = NewController(srv.log, srv.uuid, srv.cfg, state, srv.topo)
	ctx, srv.cancel = context.WithCancel(ctx)
	srv.mu.Unlock()

	srv.wg.Go(func() error {
		return srv.topo.WatchWorkers(ctx, srv, state.Epoch)
	})

	srv.wg.Go(func() error {
		return srv.waitForClusterState(ctx)
	})

	srv.wg.Go(func() error {
		srv.campaign(ctx)
		return nil
	})

	// Wait as long as the leadership campaign lasts
	<-ctx.Done()

	srv.mu.Lock()
	srv.inner = nil
	srv.cancel = nil
	srv.mu.Unlock()
}

func (srv *Server) Stop() {
	srv.mu.Lock()
	if srv.cancel != nil {
		srv.cancel()
	}
	srv.mu.Unlock()
	_ = srv.wg.Wait()
}

func (srv *Server) campaign(ctx context.Context) {
	evict := time.NewTimer(srv.cfg.EvictEvery)
	defer evict.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-evict.C:
			if _, err := srv.PerformDistributedEviction(ctx, nil); err != nil {
				srv.log.Warn("distributed eviction failed", zap.Error(err))
			}
		}
	}
}

func (srv *Server) GetMaterializations(_ context.Context, _ *vtboostpb.MaterializationsRequest) (*vtboostpb.MaterializationsResponse, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	mats, err := srv.inner.GetMaterializations()
	if err != nil {
		return nil, err
	}
	return &vtboostpb.MaterializationsResponse{Materializations: mats}, nil
}

func (srv *Server) PutRecipe(ctx context.Context, recipe *vtboostpb.Recipe) error {
	return srv.PutRecipeWithOptions(ctx, recipe, nil)
}

func (srv *Server) defaultSchemaInfo() *boostplan.SchemaInformation {
	return &boostplan.SchemaInformation{Schema: boostplan.NewDDLSchema(srv.topo)}
}

func (srv *Server) PutRecipeWithOptions(ctx context.Context, recipepb *vtboostpb.Recipe, si *boostplan.SchemaInformation) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.inner == nil {
		return fmt.Errorf("not the leader")
	}

	if si == nil {
		si = srv.defaultSchemaInfo()
	}

	_, err := srv.inner.ModifyRecipe(ctx, recipepb, si)
	return err
}

func (srv *Server) TryPlan(keyspace, sql string, si *boostplan.SchemaInformation) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if si == nil {
		si = srv.defaultSchemaInfo()
	}

	planner := boostplan.NewTestIncorporator(si)
	return planner.AddQuery(keyspace, sql)
}

func (srv *Server) GetRecipe(context.Context, *vtboostpb.GetRecipeRequest) (*vtboostpb.GetRecipeResponse, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.inner == nil {
		return nil, fmt.Errorf("not the leader")
	}
	recipe := &vtboostpb.Recipe{
		Queries: srv.inner.recipe.ToProto(),
	}
	return &vtboostpb.GetRecipeResponse{Recipe: recipe}, nil
}

func (srv *Server) Graphviz(ctx context.Context, request *vtboostpb.GraphvizRequest) (*vtboostpb.GraphvizResponse, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	var dot strings.Builder
	if err := srv.inner.Graphviz(ctx, &dot, request); err != nil {
		return nil, err
	}
	return &vtboostpb.GraphvizResponse{Dot: dot.String()}, nil
}

func (srv *Server) PerformDistributedEviction(ctx context.Context, forceLimits map[string]int64) (*EvictionPlan, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.inner == nil {
		return nil, fmt.Errorf("not the leader")
	}
	return srv.inner.PerformDistributedEviction(ctx, forceLimits)
}
