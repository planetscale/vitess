package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/topo/client"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	"vitess.io/vitess/go/boost/topo/watcher"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

type FakeBoostInstance struct {
	T       testing.TB
	Cluster string
	UUID    string
	Wait    *errgroup.Group

	topo   *toposerver.Server
	mu     sync.Mutex
	recipe *vtboost.Recipe
	state  *vtboost.ControllerState
	leader bool
}

func (ins *FakeBoostInstance) PutRecipe(ctx context.Context, recipe *vtboost.Recipe) error {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	if !ins.leader {
		ins.T.Errorf("received PutRecipe without being the leader")
	}

	ins.T.Logf("[%s] received new recipe version (v=%d)", ins.UUID, recipe.Version)

	ins.recipe = recipe
	_, err := ins.topo.UpdateControllerState(ctx, func(state *vtboost.ControllerState) error {
		state.RecipeVersion = recipe.Version
		return nil
	})
	return err
}

func (ins *FakeBoostInstance) StartLeadershipCampaign(ctx context.Context, state *vtboost.ControllerState) error {
	ins.mu.Lock()
	ins.leader = true
	ins.mu.Unlock()

	ins.Wait.Go(func() error {
		return ins.topo.WatchRecipeChanges(ctx, ins)
	})

	ins.T.Logf("[%s] started leadership campaign", ins.UUID)
	<-ctx.Done()
	ins.T.Logf("[%s] finished leadership campaign", ins.UUID)

	ins.mu.Lock()
	ins.leader = false
	ins.mu.Unlock()
	return nil
}

func (ins *FakeBoostInstance) NewLeader(state *vtboost.ControllerState) {
	ins.T.Logf("[%s] NewLeader: %s (recipe version=%d)", ins.UUID, state.Leader, state.RecipeVersion)
}

func (ins *FakeBoostInstance) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	panic("should not call")
}

func (ins *FakeBoostInstance) Close() {}

func (ins *FakeBoostInstance) Start(ctx context.Context, log *zap.Logger, ts *topo.Server) {
	log = log.With(zap.String("uuid", ins.UUID), zap.String("cluster", ins.Cluster))
	ins.topo = toposerver.NewTopoServer(log, ts, ins, ins.Cluster)

	ins.Wait.Go(func() error {
		return ins.topo.WatchLeadership(ctx, ins, ins.UUID)
	})
}

type fakeControllerClient struct {
	ins *FakeBoostInstance
}

func (f *fakeControllerClient) GetRecipe(ctx context.Context, in *vtboost.GetRecipeRequest, _ ...grpc.CallOption) (*vtboost.GetRecipeResponse, error) {
	panic("should not be called")
}

func (f *fakeControllerClient) ReadyCheck(ctx context.Context, in *vtboost.ReadyRequest, _ ...grpc.CallOption) (*vtboost.ReadyResponse, error) {
	panic("should not be called")
}

func (f *fakeControllerClient) SetScience(ctx context.Context, in *vtboost.SetScienceRequest, _ ...grpc.CallOption) (*vtboost.SetScienceResponse, error) {
	panic("should not be called")
}

func (f *fakeControllerClient) Graphviz(ctx context.Context, in *vtboost.GraphvizRequest, _ ...grpc.CallOption) (*vtboost.GraphvizResponse, error) {
	panic("should not be called")
}

func (f *fakeControllerClient) GetMaterializations(ctx context.Context, in *vtboost.MaterializationsRequest, _ ...grpc.CallOption) (*vtboost.MaterializationsResponse, error) {
	return f.ins.GetMaterializations()
}

func (ins *FakeBoostInstance) DialControllerClient() (vtboost.ControllerServiceClient, *grpc.ClientConn, error) {
	return &fakeControllerClient{ins: ins}, nil, nil
}

func (ins *FakeBoostInstance) GetMaterializations() (*vtboost.MaterializationsResponse, error) {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	if !ins.leader {
		return nil, fmt.Errorf("asked for materializations on non-leader")
	}

	var resp vtboost.MaterializationsResponse
	if ins.recipe != nil {
		for _, q := range ins.recipe.Queries {
			resp.Materializations = append(resp.Materializations, &vtboost.Materialization{
				Query:         q,
				NormalizedSql: q.Sql,
				View:          &vtboost.Materialization_ViewDescriptor{},
			})
		}
	}
	ins.T.Logf("GetMaterializations() = %d", len(resp.Materializations))
	return &resp, nil
}

func TestRecipeApplications(t *testing.T) {
	defer boosttest.EnsureNoLeaks(t)

	var wg errgroup.Group

	log, err := zap.NewDevelopment()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	ts := memorytopo.NewServer()

	defer func() {
		cancel()
		if err := wg.Wait(); err != nil {
			t.Errorf("unclean shutdown: %v", err)
		}
		ts.Close()
	}()

	var instances = make(map[string]*FakeBoostInstance)
	for _, cluster := range []string{"cluster1", "cluster2"} {
		for i := 0; i < 2; i++ {
			instance := &FakeBoostInstance{UUID: uuid.NewString(), Cluster: cluster, T: t, Wait: &wg}
			instance.Start(ctx, log, ts)
			instances[instance.UUID] = instance
		}
	}

	watch := watcher.NewWatcher(ts)
	watch.Dial = func(address string) (vtboost.ControllerServiceClient, *grpc.ClientConn, error) {
		return instances[address].DialControllerClient()
	}
	err = watch.Start()
	require.NoError(t, err)
	defer watch.Stop()

	client := client.NewClient(ts)
	_, err = client.AddCluster(context.Background(), &vtboost.AddClusterRequest{
		Uuid: "cluster1",
	})
	require.NoError(t, err)

	_, err = client.AddCluster(context.Background(), &vtboost.AddClusterRequest{
		Uuid: "cluster2",
	})
	require.NoError(t, err)

	_, err = client.PutRecipe(context.Background(), &vtboost.PutRecipeRequest{
		Recipe: &vtboost.Recipe{
			Queries: []*vtboost.CachedQuery{
				{
					PublicId: "0001",
					Sql:      "SELECT 1",
				},
				{
					PublicId: "0002",
					Sql:      "SELECT 2",
				},
			},
		},
	})
	require.NoError(t, err)

	WaitUntil(t, func() bool {
		return watch.Version() != "" &&
			len(watch.DebugState()["cluster1"].Materializations) > 0 &&
			len(watch.DebugState()["cluster2"].Materializations) > 0
	})

	state := watch.DebugState()
	assert.Len(t, state["cluster1"].Materializations, 2)
	assert.Len(t, state["cluster2"].Materializations, 2)
}
