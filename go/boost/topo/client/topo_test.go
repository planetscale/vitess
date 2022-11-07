package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestClientNilRequest(t *testing.T) {
	ts := memorytopo.NewServer()
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := NewClient(ts)

	// Any of these calls can be invoked via vtctld or directly in
	// psdb-operator, so make sure that they cannot crash either the vtboost pod
	// or the operator itself.

	_, _ = c.AddCluster(ctx, nil)
	_, _ = c.RemoveCluster(ctx, nil)
	_, _ = c.MakePrimaryCluster(ctx, nil)
	_, _ = c.DrainCluster(ctx, nil)
	_, _ = c.GetCluster(ctx, nil)
	_, _ = c.ListClusters(ctx, nil)
	_, _ = c.PutRecipe(ctx, nil)
	_, _ = c.GetRecipe(ctx, nil)
	_, _ = c.DescribeRecipe(ctx, nil)
	_, _ = c.Purge(ctx, nil)
}

func TestClientRemoveCluster(t *testing.T) {
	tests := []struct {
		name   string
		uuid   string
		recipe *vtboostpb.PutRecipeRequest
		code   ErrorCode
	}{
		{
			name: "no recipe",
			uuid: "foo",
		},
		{
			name: "empty recipe",
			uuid: "foo",
			recipe: &vtboostpb.PutRecipeRequest{
				Recipe: &vtboostpb.Recipe{
					Queries: nil,
				},
			},
		},
		{
			name: "cluster not found",
			uuid: "bar",
			code: ErrClusterNotFound,
		},
		{
			name: "active recipe",
			uuid: "foo",
			recipe: &vtboostpb.PutRecipeRequest{
				Recipe: &vtboostpb.Recipe{
					Queries: []*vtboostpb.CachedQuery{{
						PublicId: "foo",
						Sql:      "SELECT COUNT(id) FROM customers",
					}},
				},
			},
			code: ErrNoPrimaryOrWarming,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := memorytopo.NewServer()
			t.Cleanup(ts.Close)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			c := NewClient(ts)

			_, err := c.AddCluster(ctx, &vtboostpb.AddClusterRequest{Uuid: "foo"})
			require.NoError(t, err)

			if tt.recipe != nil {
				_, err := c.PutRecipe(ctx, tt.recipe)
				require.NoError(t, err)
			}

			_, err = c.RemoveCluster(ctx, &vtboostpb.RemoveClusterRequest{
				Uuids: []string{tt.uuid},
			})
			if tt.code == 0 {
				require.NoError(t, err)
				return
			}

			var berr *Error
			require.ErrorAs(t, err, &berr)
			require.Equal(t, tt.code, berr.Code)
		})
	}
}

func Test_validateCluster(t *testing.T) {
	tests := []struct {
		name  string
		state *vtboostpb.ClusterStates
		err   error
	}{
		{
			name: "cluster already exists",
			state: &vtboostpb.ClusterStates{
				Clusters: []*vtboostpb.ClusterState{
					{
						Uuid:  "foo",
						State: vtboostpb.ClusterState_PRIMARY,
					},
					{
						Uuid:  "foo",
						State: vtboostpb.ClusterState_WARMING,
					},
				},
			},
			err: &Error{
				Code: ErrClusterAlreadyExists,
				Err:  errors.New(`duplicate UUID "foo" in cluster list`),
			},
		},
		{
			name: "drain no timestamp",
			state: &vtboostpb.ClusterStates{
				Clusters: []*vtboostpb.ClusterState{{
					Uuid:  "foo",
					State: vtboostpb.ClusterState_DRAINING,
				}},
			},
			err: &Error{
				Code: ErrDrainNoTimestamp,
				Err:  errors.New(`cluster "foo" is marked as draining without a timestamp`),
			},
		},
		{
			name: "multiple primaries",
			state: &vtboostpb.ClusterStates{
				Clusters: []*vtboostpb.ClusterState{
					{
						Uuid:  "foo",
						State: vtboostpb.ClusterState_PRIMARY,
					},
					{
						Uuid:  "bar",
						State: vtboostpb.ClusterState_PRIMARY,
					},
				},
			},
			err: &Error{
				Code: ErrMultiplePrimaries,
				Err:  errors.New("more than one cluster is marked as primary"),
			},
		},
		{
			name: "no primary or warming",
			state: &vtboostpb.ClusterStates{
				Clusters: []*vtboostpb.ClusterState{{
					Uuid:  "foo",
					State: vtboostpb.ClusterState_DISABLED,
				}},
			},
			err: &Error{
				Code: ErrNoPrimaryOrWarming,
				Err:  errors.New("no primary or warming clusters configured"),
			},
		},
		{
			name: "ok",
			state: &vtboostpb.ClusterStates{
				Clusters: []*vtboostpb.ClusterState{
					{
						Uuid:  "foo",
						State: vtboostpb.ClusterState_PRIMARY,
					},
					{
						Uuid:  "bar",
						State: vtboostpb.ClusterState_WARMING,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.err, validateCluster(tt.state))
		})
	}
}
