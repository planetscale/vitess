package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/boost/server/controller/materialization"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
)

type Server struct {
	vtboostpb.UnimplementedControllerServiceServer

	topo *toposerver.Server

	mu     sync.Mutex
	log    *zap.Logger
	inner  *Controller
	cancel context.CancelFunc

	uuid uuid.UUID
	cfg  *config.Config

	wg errgroup.Group
}

func NewServer(log *zap.Logger, id uuid.UUID, ts *toposerver.Server, cfg *config.Config) *Server {
	return &Server{topo: ts, log: log, uuid: id, cfg: cfg}
}

func (srv *Server) UpdateWorker(ctx context.Context, worker *vtboostpb.TopoWorkerEntry) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.inner == nil {
		return
	}

	srv.inner.updateWorker(ctx, worker)
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

	srv.log.Debug("found self in ClusterStates",
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

// GetViewDescriptor_ returns the internal view descriptor for the given view.
// This function is only exported to be usable by integration tests
func (srv *Server) GetViewDescriptor_(id string) (*vtboostpb.Materialization_ViewDescriptor, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	return srv.inner.viewDescriptorForPublicID(id)
}

// Migrate_ runs a migration on this controller.
// This function is only exported to be usable by integration tests
func (srv *Server) Migrate_(ctx context.Context, perform func(mig Migration) error) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.inner == nil {
		return errors.New("tried to migrate without being a leader")
	}

	mig := NewMigration(ctx, srv.inner.log, srv.inner)
	if err := perform(mig); err != nil {
		return err
	}
	return mig.Commit()
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

func (srv *Server) StartLeaderCampaign(ctx context.Context, state *vtboostpb.ControllerState) error {
	srv.log.Debug("Starting leader campaign", zap.Int64("epoch", state.Epoch))

	srv.mu.Lock()
	var err error
	srv.inner, err = NewController(srv.log, srv.uuid, srv.cfg, state, srv.topo)
	if err != nil {
		srv.mu.Unlock()
		return err
	}
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
	srv.inner.Close()
	srv.inner = nil
	srv.cancel = nil
	srv.mu.Unlock()
	return nil
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

var errNoLeader = fmt.Errorf("cannot access Boost API; instance is not the cluster leader")

func (srv *Server) GetMaterializations(_ context.Context, _ *vtboostpb.MaterializationsRequest) (*vtboostpb.MaterializationsResponse, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.inner == nil {
		return nil, errNoLeader
	}

	mats, err := srv.inner.GetMaterializations()
	if err != nil {
		return nil, err
	}
	return &vtboostpb.MaterializationsResponse{Materializations: mats}, nil
}

func (srv *Server) PutRecipe(ctx context.Context, recipe *vtboostpb.Recipe) error {
	return srv.PutRecipeWithOptions(ctx, recipe, nil)
}

func (srv *Server) defaultSchemaInfo() (*boostplan.SchemaInformation, error) {
	ddl, err := boostplan.NewDDLSchema(srv.topo)
	if err != nil {
		return nil, err
	}
	return &boostplan.SchemaInformation{Schema: ddl}, nil
}

func (srv *Server) PutRecipeWithOptions(ctx context.Context, recipepb *vtboostpb.Recipe, si *boostplan.SchemaInformation) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.inner == nil {
		return errNoLeader
	}

	if si == nil {
		var err error
		si, err = srv.defaultSchemaInfo()
		if err != nil {
			return err
		}
	}
	_, err := srv.inner.PutRecipe(ctx, recipepb, si)
	return err
}

func (srv *Server) GetRecipe(context.Context, *vtboostpb.GetRecipeRequest) (*vtboostpb.GetRecipeResponse, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.inner == nil {
		return nil, errNoLeader
	}
	recipe := &vtboostpb.Recipe{
		Queries: srv.inner.recipe.ToProto(),
	}
	return &vtboostpb.GetRecipeResponse{Recipe: recipe}, nil
}

func (srv *Server) Graphviz(ctx context.Context, request *vtboostpb.GraphvizRequest) (*vtboostpb.GraphvizResponse, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.inner == nil {
		return nil, errNoLeader
	}

	var dot strings.Builder
	if err := srv.inner.Graphviz(ctx, &dot, request); err != nil {
		return nil, err
	}
	return &vtboostpb.GraphvizResponse{Dot: dot.String()}, nil
}

func (srv *Server) PerformDistributedEviction(ctx context.Context, forceLimits map[string]int64) (*materialization.EvictionPlan, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.inner == nil {
		return nil, errNoLeader
	}
	return srv.inner.PerformDistributedEviction(ctx, forceLimits)
}
