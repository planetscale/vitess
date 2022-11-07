package integration

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	boosttopo "vitess.io/vitess/go/boost/topo/internal/topo"
	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/topo/client"
	"vitess.io/vitess/go/boost/topo/watcher"
	"vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func WaitUntil(t testing.TB, until func() bool) {
	t.Helper()

	start := time.Now()
	tick := time.NewTicker(1 * time.Millisecond)
	defer tick.Stop()
	for now := range tick.C {
		if now.Sub(start) >= 5*time.Second {
			t.Fatalf("timed out after 5s")
		}
		if until() {
			return
		}
	}
}

func TestBasicWatcherBehavior(t *testing.T) {
	client, watch, ts := setup(t)

	addCluster(t, client, "cluster1")
	addCluster(t, client, "cluster2")

	WaitUntil(t, func() bool { return watch.Version() != "" })
	currentVersion := watch.Version()

	state := watch.DebugState()
	assert.Len(t, state, 2)
	assert.Contains(t, state, "cluster1")
	assert.Contains(t, state, "cluster2")

	_, err := client.MakePrimaryCluster(context.Background(), &vtboost.PrimaryClusterRequest{Uuid: "cluster2"})
	require.NoError(t, err)

	WaitUntil(t, func() bool { return watch.Version() != currentVersion })
	currentVersion = watch.Version()

	state = watch.DebugState()
	assert.Len(t, state, 2)
	assert.Equal(t, vtboost.ClusterState_WARMING, state["cluster1"].State)
	assert.Equal(t, vtboost.ClusterState_PRIMARY, state["cluster2"].State)

	clusters, err := boosttopo.Load[vtboost.ClusterStates](context.Background(), ts, boosttopo.PathClusterState)
	require.NoError(t, err)
	assert.Len(t, clusters.Clusters, 2)
	for _, cluster := range clusters.Clusters {
		if cluster.Uuid == "cluster2" {
			assert.Len(t, cluster.Vtgates, 1)
		} else {
			assert.Len(t, cluster.Vtgates, 0)
		}
	}

	_, err = client.MakePrimaryCluster(context.Background(), &vtboost.PrimaryClusterRequest{Uuid: "cluster1"})
	require.NoError(t, err)

	WaitUntil(t, func() bool { return watch.Version() != currentVersion })
	currentVersion = watch.Version()

	state = watch.DebugState()
	assert.Len(t, state, 2)
	assert.Equal(t, vtboost.ClusterState_PRIMARY, state["cluster1"].State)
	assert.Equal(t, vtboost.ClusterState_WARMING, state["cluster2"].State)
}

func TestClientAddClusterTwice(t *testing.T) {
	c, watch, _ := setup(t)

	const uuid = "cluster1"

	addCluster(t, c, uuid)

	WaitUntil(t, func() bool { return watch.Version() != "" })

	_, err := c.AddCluster(context.Background(), &vtboost.AddClusterRequest{
		Uuid: uuid,
	})

	var cerr *client.Error
	require.ErrorAs(t, err, &cerr)
	require.Equal(t, client.ErrClusterAlreadyExists, cerr.Code)
}

func TestClientMakePrimaryClusterTwice(t *testing.T) {
	c, watch, _ := setup(t)

	const uuid = "cluster1"

	addCluster(t, c, uuid)

	WaitUntil(t, func() bool { return watch.Version() != "" })

	_, err := c.MakePrimaryCluster(context.Background(), &vtboost.PrimaryClusterRequest{
		Uuid: uuid,
	})
	require.NoError(t, err)

	_, err = c.MakePrimaryCluster(context.Background(), &vtboost.PrimaryClusterRequest{
		Uuid: uuid,
	})

	var cerr *client.Error
	require.ErrorAs(t, err, &cerr)
	require.Equal(t, client.ErrClusterAlreadyPrimary, cerr.Code)
}

func TestClientDrainClusterTwice(t *testing.T) {
	c, watch, _ := setup(t)

	// Two clusters so one can go to warming after we drain.
	addCluster(t, c, "cluster1")
	addCluster(t, c, "cluster2")

	WaitUntil(t, func() bool { return watch.Version() != "" })

	_, err := c.MakePrimaryCluster(context.Background(), &vtboost.PrimaryClusterRequest{
		Uuid: "cluster1",
	})
	require.NoError(t, err)

	_, err = c.DrainCluster(context.Background(), &vtboost.DrainClusterRequest{
		Uuid:      "cluster1",
		DrainedAt: &vttime.Time{Seconds: 1},
	})
	require.NoError(t, err)

	_, err = c.DrainCluster(context.Background(), &vtboost.DrainClusterRequest{
		Uuid:      "cluster1",
		DrainedAt: &vttime.Time{Seconds: 2},
	})

	var cerr *client.Error
	require.ErrorAs(t, err, &cerr)
	require.Equal(t, client.ErrClusterAlreadyDraining, cerr.Code)
}

func TestClientRemoveClusterTwice(t *testing.T) {
	c, watch, _ := setup(t)

	// Two clusters so one becomes primary and we can remove the other.
	addCluster(t, c, "cluster1")
	addCluster(t, c, "cluster2")

	WaitUntil(t, func() bool { return watch.Version() != "" })

	_, err := c.MakePrimaryCluster(context.Background(), &vtboost.PrimaryClusterRequest{
		Uuid: "cluster1",
	})
	require.NoError(t, err)

	_, err = c.RemoveCluster(context.Background(), &vtboost.RemoveClusterRequest{
		Uuids: []string{"cluster2"},
	})
	require.NoError(t, err)

	_, err = c.RemoveCluster(context.Background(), &vtboost.RemoveClusterRequest{
		Uuids: []string{"cluster2"},
	})

	var cerr *client.Error
	require.ErrorAs(t, err, &cerr)
	require.Equal(t, client.ErrClusterNotFound, cerr.Code)
}

func TestClientRemoveClusters(t *testing.T) {
	c, watch, _ := setup(t)

	// Verify we can remove multiple clusters at once while leaving a final one
	// as our primary.
	addCluster(t, c, "cluster1")
	addCluster(t, c, "cluster2")
	addCluster(t, c, "cluster3")

	WaitUntil(t, func() bool { return watch.Version() != "" })

	_, err := c.RemoveCluster(context.Background(), &vtboost.RemoveClusterRequest{
		Uuids: []string{"cluster1", "cluster2"},
	})
	require.NoError(t, err)

	checkRemoved := func(t *testing.T, uuid string) {
		t.Helper()

		_, err = c.RemoveCluster(context.Background(), &vtboost.RemoveClusterRequest{
			Uuids: []string{uuid},
		})

		var cerr *client.Error
		require.ErrorAs(t, err, &cerr)
		require.Equal(t, client.ErrClusterNotFound, cerr.Code)
	}

	checkRemoved(t, "cluster1")
	checkRemoved(t, "cluster2")
	checkRemoved(t, "clusterNotFound")
}

func TestClientPurge(t *testing.T) {
	c, watch, ts := setup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err)

	// Create a bunch of boost state in the topo, including:
	//  - two clusters
	//  - a populated recipe
	//  - a simulated worker registration
	addCluster(t, c, "cluster1")
	addCluster(t, c, "cluster2")
	_, err = c.PutRecipe(ctx, &vtboost.PutRecipeRequest{
		Recipe: &vtboost.Recipe{
			Queries: []*vtboost.CachedQuery{{
				PublicId:       "abcdef",
				Name:           "customers count",
				Sql:            "SELECT COUNT(id) FROM customers",
				Keyspace:       "keyspace",
				MaxMemoryUsage: 1024,
			}},
		},
	})
	require.NoError(t, err)

	worker := path.Join(boosttopo.PathWorker("123456", 1), "abcdef")
	_, err = conn.Create(ctx, worker, []byte("blob"))
	require.NoError(t, err)

	WaitUntil(t, func() bool { return watch.Version() != "" })

	// Now purge all that state and verify that everything was removed.
	_, err = c.Purge(ctx, &vtboost.PurgeRequest{})
	require.NoError(t, err)

	{
		_, err := c.ListClusters(ctx, &vtboost.ListClustersRequest{})
		require.Error(t, err)

		var cerr *client.Error
		require.ErrorAs(t, err, &cerr)
		require.Equal(t, client.ErrNoClusterStates, cerr.Code)
	}

	{
		_, err := c.GetRecipe(ctx, &vtboost.GetRecipeRequest{})
		require.Error(t, err)

		var cerr *client.Error
		require.ErrorAs(t, err, &cerr)
		require.Equal(t, client.ErrNoActiveRecipe, cerr.Code)
	}

	{
		_, _, err := conn.Get(ctx, worker)
		require.Error(t, err)
		require.True(t, topo.IsErrType(err, topo.NoNode))
	}

	// Purging again is a noop.
	_, err = c.Purge(ctx, &vtboost.PurgeRequest{})
	require.NoError(t, err)

}

func setup(t *testing.T) (*client.Client, *watcher.Watcher, *topo.Server) {
	t.Helper()

	t.Cleanup(func() { boosttest.EnsureNoLeaks(t) })

	ts := memorytopo.NewServer()
	t.Cleanup(ts.Close)

	var (
		c = client.NewClient(ts)
		w = watcher.NewWatcher(ts)
	)

	w.Start()
	t.Cleanup(w.Stop)

	return c, w, ts
}

func addCluster(t *testing.T, client *client.Client, uuid string) {
	t.Helper()

	_, err := client.AddCluster(context.Background(), &vtboost.AddClusterRequest{
		Uuid: uuid,
	})
	require.NoError(t, err)

	_, err = client.GetCluster(context.Background(), &vtboost.GetClusterRequest{
		Uuid: uuid,
	})
	require.NoError(t, err)
}
