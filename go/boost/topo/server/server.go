package server

import (
	"context"
	"fmt"
	"path"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	boosttopo "vitess.io/vitess/go/boost/topo/internal/topo"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/topo"
)

type TabletManager interface {
	// GetSchema asks the remote tablet for its database schema
	GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdata.GetSchemaRequest) (*tabletmanagerdata.SchemaDefinition, error)

	// Close will be called when this TabletManagerClient won't be
	// used any more. It can be used to free any resource.
	Close()
}

type Server struct {
	log       *zap.Logger
	srv       *topo.Server
	tmc       TabletManager
	clusterID string
}

func NewTopoServer(log *zap.Logger, srv *topo.Server, tmc TabletManager, clusterID string) *Server {
	return &Server{
		log:       log.With(zap.String("topo", "server")),
		srv:       srv,
		tmc:       tmc,
		clusterID: clusterID,
	}
}

type LeadershipWatcher interface {
	StartLeadershipCampaign(ctx context.Context, state *vtboostpb.ControllerState) error
	NewLeader(state *vtboostpb.ControllerState)
}

type WorkerWatcher interface {
	RegisterWorker(ctx context.Context, worker *vtboostpb.TopoWorkerEntry)
}

func (ts *Server) Close() {
	ts.srv.Close()
}

func (ts *Server) watchWorkersPoll(ctx context.Context, watcher WorkerWatcher, epoch int64) error {
	ts.log.Info("watching for workers (poll)", zap.Int64("epoch", epoch))

	var activeWorkers = make(map[string]*vtboostpb.TopoWorkerEntry)
	var ticker = time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		workers, err := ts.GetWorkersForEpoch(ctx, epoch)
		if err == nil {
			var newworkers int
			for _, worker := range workers {
				if _, exists := activeWorkers[worker.Uuid]; !exists {
					activeWorkers[worker.Uuid] = worker
					watcher.RegisterWorker(ctx, worker)
					newworkers++
				}
			}

			if newworkers > 0 {
				ts.log.Debug("found new workers", zap.Int("count", newworkers))
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (ts *Server) WatchWorkers(ctx context.Context, watcher WorkerWatcher, epoch int64) error {
	conn, err := ts.srv.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return err
	}

	initial, changes, err := conn.WatchRecursive(ctx, boosttopo.PathWorker(ts.clusterID, epoch))
	if err != nil {
		if topo.IsErrType(err, topo.NoImplementation) {
			return ts.watchWorkersPoll(ctx, watcher, epoch)
		}
		return err
	}

	ts.log.Info("watching for workers (recursive)", zap.Int64("epoch", epoch))

	for _, d := range initial {
		var entry vtboostpb.TopoWorkerEntry
		if err := proto.Unmarshal(d.Contents, &entry); err != nil {
			ts.log.Error("invalid Worker entry in topo server", zap.Error(err))
			continue
		}
		watcher.RegisterWorker(ctx, &entry)
	}

	for chg := range changes {
		if chg.Err != nil {
			continue
		}

		var entry vtboostpb.TopoWorkerEntry
		if err := proto.Unmarshal(chg.Contents, &entry); err != nil {
			ts.log.Error("invalid Worker entry in topo server", zap.Error(err))
			continue
		}
		watcher.RegisterWorker(ctx, &entry)
	}
	return nil
}

func (ts *Server) onControllerChange(rw *topo.WatchData, watcher LeadershipWatcher, currentLeader *string) error {
	if rw.Err != nil {
		if topo.IsErrType(rw.Err, topo.Interrupted) {
			return nil
		}
		return rw.Err
	}
	var state vtboostpb.ControllerState
	if err := proto.Unmarshal(rw.Contents, &state); err != nil {
		return err
	}
	if state.Leader == *currentLeader {
		return nil
	}
	*currentLeader = state.Leader
	watcher.NewLeader(&state)
	return nil
}

func (ts *Server) WatchLeadership(ctx context.Context, watcher LeadershipWatcher, self string) error {
	conn, err := ts.srv.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return err
	}

	leader, err := conn.NewLeaderParticipation(boosttopo.PathLeader(ts.clusterID), self)
	if err != nil {
		return err
	}

	var wg errgroup.Group

	wg.Go(func() error {
		var currentLeader string
		ts.log.Debug("waiting for leader election")

		for ctx.Err() == nil {
			l, newl, err := conn.Watch(ctx, boosttopo.PathControllerState(ts.clusterID))
			if err != nil {
				if topo.IsErrType(err, topo.NoNode) {
					time.Sleep(100 * time.Millisecond)
				} else {
					ts.log.Warn("failed to Watch active controller state; retrying", zap.Error(err))
					time.Sleep(1 * time.Second)
				}
				continue
			}

			if err := ts.onControllerChange(l, watcher, &currentLeader); err != nil {
				ts.log.Error("failed to process controller state change", zap.Error(err))
			}
			for l := range newl {
				if err := ts.onControllerChange(l, watcher, &currentLeader); err != nil {
					ts.log.Error("failed to process controller state change", zap.Error(err))
				}
			}
		}
		return nil
	})

	wg.Go(func() error {
		for {
			if ctx.Err() != nil {
				return nil
			}

			ts.log.Debug("wait for leadership")
			leadershipCtx, err := leader.WaitForLeadership()
			switch {
			case err == nil:
				state, err := ts.UpdateControllerState(leadershipCtx, func(state *vtboostpb.ControllerState) error {
					if state.Leader == "" {
						state.RecipeVersion = -1
					}
					state.Leader = self
					state.Epoch++
					return nil
				})
				if err != nil {
					return err
				}
				// we're actually shutting down
				if ctx.Err() != nil {
					return nil
				}
				err = watcher.StartLeadershipCampaign(leadershipCtx, state)
				if err != nil {
					return err
				}
				ts.log.Info("leadership campaign finished")
			case topo.IsErrType(err, topo.Interrupted):
				return nil
			default:
				ts.log.Error("error while waiting for primary", zap.Error(err))
				time.Sleep(5 * time.Second)
			}
		}
	})
	<-ctx.Done()
	leader.Stop()
	return wg.Wait()
}

// GetControllerState returns the controller state
func (ts *Server) GetControllerState(ctx context.Context) (*vtboostpb.ControllerState, error) {
	return boosttopo.Load[vtboostpb.ControllerState](ctx, ts.srv, boosttopo.PathControllerState(ts.clusterID))
}

func (ts *Server) GetWorkersForEpoch(ctx context.Context, epoch int64) ([]*vtboostpb.TopoWorkerEntry, error) {
	conn, err := ts.srv.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return nil, err
	}

	data, err := conn.List(ctx, boosttopo.PathWorker(ts.clusterID, epoch))
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return nil, nil
		}
		return nil, err
	}

	var workers []*vtboostpb.TopoWorkerEntry
	for _, d := range data {
		var entry vtboostpb.TopoWorkerEntry
		if err := proto.Unmarshal(d.Value, &entry); err != nil {
			return nil, err
		}
		workers = append(workers, &entry)
	}
	return workers, nil
}

func (ts *Server) CreateWorker(ctx context.Context, epoch int64, worker *vtboostpb.TopoWorkerEntry) error {
	conn, err := ts.srv.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(worker)
	if err != nil {
		return err
	}

	ts.log.Debug("registered new worker", zap.Any("worker", worker))

	prefix := path.Join(boosttopo.PathWorker(ts.clusterID, epoch), worker.Uuid)
	_, err = conn.Create(ctx, prefix, data)
	return err
}

func (ts *Server) UpdateControllerState(ctx context.Context, update func(state *vtboostpb.ControllerState) error) (*vtboostpb.ControllerState, error) {
	return boosttopo.Update(ctx, ts.srv, boosttopo.PathControllerState(ts.clusterID), update)
}

func (ts *Server) GetSchema(ctx context.Context, keyspace string) (*tabletmanagerdata.SchemaDefinition, error) {
	shards, err := ts.srv.FindAllShardsInKeyspace(ctx, keyspace)
	if err != nil {
		return nil, err
	}

	for _, shard := range shards {
		ti, err := ts.srv.GetTablet(ctx, shard.PrimaryAlias)
		if err != nil {
			return nil, err
		}

		req := &tabletmanagerdata.GetSchemaRequest{Tables: []string{}, ExcludeTables: []string{}, TableSchemaOnly: true}
		schema, err := ts.tmc.GetSchema(ctx, ti.Tablet, req)
		if err != nil {
			return nil, err
		}

		return schema, nil
	}

	return nil, fmt.Errorf("no shards found in keyspace %q", keyspace)
}

func (ts *Server) WorkerHeartbeat(ctx context.Context, uuid string) error {
	// TODO
	return nil
}

type RecipeWatcher interface {
	PutRecipe(ctx context.Context, recipe *vtboostpb.Recipe) error
}

func (ts *Server) WatchRecipeChanges(ctx context.Context, watcher RecipeWatcher) error {
	conn, err := ts.srv.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return err
	}

	for ctx.Err() == nil {
		current, w, err := conn.Watch(ctx, boosttopo.PathActiveRecipe)
		if err != nil {
			if topo.IsErrType(err, topo.NoNode) {
				time.Sleep(100 * time.Millisecond)
			} else {
				ts.log.Warn("failed to Watch active recipe; retrying", zap.Error(err))
				time.Sleep(1 * time.Second)
			}
			continue
		}

		if err := ts.onRecipeChange(ctx, current, watcher); err != nil {
			ts.log.Error("failed to process recipe change", zap.Error(err))
		}

		for rcp := range w {
			if err := ts.onRecipeChange(ctx, rcp, watcher); err != nil {
				ts.log.Error("failed to process recipe change", zap.Error(err))
			}
		}
	}
	return nil
}

func (ts *Server) onRecipeChange(ctx context.Context, rw *topo.WatchData, watcher RecipeWatcher) error {
	if rw.Err != nil {
		if topo.IsErrType(rw.Err, topo.Interrupted) {
			return nil
		}
		return rw.Err
	}
	var recipe vtboostpb.Recipe
	if err := proto.Unmarshal(rw.Contents, &recipe); err != nil {
		return err
	}
	return watcher.PutRecipe(ctx, &recipe)
}

func (ts *Server) WaitForSelfInClusterState(ctx context.Context) (*vtboostpb.ClusterState, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	conn, err := ts.srv.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return nil, err
	}

	for ctx.Err() == nil {
		currentClusters, nextClusters, err := conn.Watch(ctx, boosttopo.PathClusterState)
		if err != nil {
			if topo.IsErrType(err, topo.NoNode) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			ts.log.Warn("cannot find an active Boost cluster; retrying...", zap.Error(err))
			time.Sleep(1 * time.Second)
			continue
		}

		if state := ts.clusterReady(currentClusters); state != nil {
			return state, nil
		}

		for cluster := range nextClusters {
			if state := ts.clusterReady(cluster); state != nil {
				return state, nil
			}
		}
	}
	return nil, ctx.Err()
}

func (ts *Server) clusterReady(rw *topo.WatchData) *vtboostpb.ClusterState {
	if rw.Err != nil {
		ts.log.Error("failed to check for clusterReady", zap.Error(rw.Err))
		return nil
	}

	var states vtboostpb.ClusterStates
	if err := proto.Unmarshal(rw.Contents, &states); err != nil {
		ts.log.Error("failed to unmarshal in clusterReady", zap.Error(err))
		return nil
	}

	ts.log.Debug("checking clusters for self", zap.Int("cluster_count", len(states.Clusters)))
	for _, st := range states.Clusters {
		if st.Uuid == ts.clusterID {
			return st
		}
	}
	return nil
}
