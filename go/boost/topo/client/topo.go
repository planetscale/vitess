package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/exp/slices"
	"google.golang.org/grpc/codes"

	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
	boosttopo "vitess.io/vitess/go/boost/topo/internal/topo"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/topo"
)

type Client struct {
	ts *topo.Server
}

func NewClient(ts *topo.Server) *Client {
	return &Client{ts: ts}
}

func validateCluster(state *vtboostpb.ClusterStates) error {
	var (
		primaries, warming []*vtboostpb.ClusterState
		seenUUIDs          = map[string]struct{}{}
	)

	for _, cluster := range state.GetClusters() {
		if _, exists := seenUUIDs[cluster.GetUuid()]; exists {
			return &Error{ErrClusterAlreadyExists, fmt.Errorf("duplicate UUID %q in cluster list", cluster.GetUuid())}
		}
		seenUUIDs[cluster.GetUuid()] = struct{}{}
		switch cluster.GetState() {
		case vtboostpb.ClusterState_PRIMARY:
			primaries = append(primaries, cluster)
		case vtboostpb.ClusterState_WARMING:
			warming = append(warming, cluster)
		case vtboostpb.ClusterState_DRAINING:
			if cluster.GetDrainedAt() == nil {
				return &Error{ErrDrainNoTimestamp, fmt.Errorf("cluster %q is marked as draining without a timestamp", cluster.GetUuid())}
			}
		}
	}

	if len(primaries) > 1 {
		return &Error{ErrMultiplePrimaries, errors.New("more than one cluster is marked as primary")}
	}
	if len(primaries)+len(warming) == 0 {
		return &Error{ErrNoPrimaryOrWarming, errors.New("no primary or warming clusters configured")}
	}
	return nil
}

func (c *Client) AddCluster(ctx context.Context, req *vtboostpb.AddClusterRequest) (*vtboostpb.ClusterChangeResponse, error) {
	addCluster := func(state *vtboostpb.ClusterStates) error {
		createdAt := req.GetCreatedAt()
		if createdAt == nil {
			createdAt = protoutil.TimeToProto(time.Now())
		}
		state.Clusters = append(state.Clusters, &vtboostpb.ClusterState{
			Uuid:                req.GetUuid(),
			State:               vtboostpb.ClusterState_WARMING,
			CreatedAt:           createdAt,
			ExpectedWorkerCount: req.GetExpectedWorkerCount(),
		})
		return validateCluster(state)
	}

	if _, err := boosttopo.Update(ctx, c.ts, boosttopo.PathClusterState, addCluster); err != nil {
		return nil, err
	}
	return &vtboostpb.ClusterChangeResponse{}, nil
}

func (c *Client) RemoveCluster(ctx context.Context, req *vtboostpb.RemoveClusterRequest) (*vtboostpb.ClusterChangeResponse, error) {
	// Build a set of clusters to remove in one operation.
	remove := make(map[string]bool, len(req.GetUuids()))
	for _, uuid := range req.GetUuids() {
		remove[uuid] = false
	}

	removeCluster := func(state *vtboostpb.ClusterStates) error {
		var keep []*vtboostpb.ClusterState
		for _, cluster := range state.GetClusters() {
			if _, ok := remove[cluster.GetUuid()]; ok {
				// Removed what we were asked and continue.
				remove[cluster.GetUuid()] = true
				continue
			}

			keep = append(keep, cluster)
		}

		// If any of the clusters from the removal set were not seen, report an
		// error.
		for uuid, removed := range remove {
			if !removed {
				return &Error{ErrClusterNotFound, fmt.Errorf("no cluster found to remove with UUID %q", uuid)}
			}
		}

		state.Clusters = keep
		if len(keep) > 0 {
			// Check the clusters we're keeping.
			return validateCluster(state)
		}

		// Refuse to remove the final cluster if a non-empty recipe is active so
		// vtgates don't immediately error.
		found, err := c.haveRecipe(ctx)
		if err != nil {
			return fmt.Errorf("could not check for active recipe on cluster removal: %v", err)
		}
		if found {
			return &Error{
				ErrNoPrimaryOrWarming,
				errors.New("no primary or warming clusters configured, but a recipe is still active"),
			}
		}

		return nil
	}

	if _, err := boosttopo.Update(ctx, c.ts, boosttopo.PathClusterState, removeCluster); err != nil {
		return nil, err
	}
	return &vtboostpb.ClusterChangeResponse{}, nil
}

// haveRecipe checks if an active recipe with more than zero queries exists in
// the boost topology. It returns true if a recipe exists in topo and has at
// least one query configured.
func (c *Client) haveRecipe(ctx context.Context) (bool, error) {
	recipe, err := boosttopo.Load[vtboostpb.Recipe](ctx, c.ts, boosttopo.PathActiveRecipe)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return false, nil
		}

		return false, err
	}

	return len(recipe.GetQueries()) > 0, nil

}

func (c *Client) MakePrimaryCluster(ctx context.Context, req *vtboostpb.PrimaryClusterRequest) (*vtboostpb.PrimaryClusterResponse, error) {
	makePrimaryCluster := func(state *vtboostpb.ClusterStates) error {
		found := false
		for _, cluster := range state.GetClusters() {
			current := cluster.GetUuid() == req.GetUuid()
			switch {
			case current && cluster.GetState() == vtboostpb.ClusterState_PRIMARY:
				// Cluster is already primary.
				return &Error{
					ErrClusterAlreadyPrimary,
					fmt.Errorf("cluster with UUID %q is already primary", req.GetUuid()),
				}
			case current:
				// Cluster was not primary, but now will be.
				cluster.State = vtboostpb.ClusterState_PRIMARY
				found = true
			case cluster.State == vtboostpb.ClusterState_PRIMARY:
				// Move the other primary to warming
				cluster.State = vtboostpb.ClusterState_WARMING
			}
		}
		if !found {
			return &Error{ErrClusterNotFound, fmt.Errorf("no cluster found to make primary with UUID %q", req.GetUuid())}
		}
		return validateCluster(state)
	}

	if _, err := boosttopo.Update(ctx, c.ts, boosttopo.PathClusterState, makePrimaryCluster); err != nil {
		return nil, err
	}
	return &vtboostpb.PrimaryClusterResponse{}, nil
}

func (c *Client) DrainCluster(ctx context.Context, req *vtboostpb.DrainClusterRequest) (*vtboostpb.DrainClusterResponse, error) {
	drainCluster := func(state *vtboostpb.ClusterStates) error {
		found := false
		for _, cluster := range state.GetClusters() {
			if cluster.GetUuid() != req.GetUuid() {
				continue
			}

			if cluster.GetState() == vtboostpb.ClusterState_DRAINING {
				// The caller of this API is psdb-operator and we want to permit
				// multiple calls to drain the same cluster over different
				// reconciliation loops. We can simplify the operator side of
				// things if trying to send another BoostDrainCluster RPC just
				// results in an "already exists" error.
				return &Error{
					ErrClusterAlreadyDraining,
					fmt.Errorf("cluster with UUID %q is already draining, deadline: %s",
						req.GetUuid(), logutil.ProtoToTime(cluster.DrainedAt),
					),
				}
			}

			cluster.State = vtboostpb.ClusterState_DRAINING
			cluster.DrainedAt = req.GetDrainedAt()
			found = true
			break
		}
		if !found {
			return &Error{ErrClusterNotFound, fmt.Errorf("no cluster found to drain with UUID %q", req.GetUuid())}
		}
		return validateCluster(state)
	}

	if _, err := boosttopo.Update(ctx, c.ts, boosttopo.PathClusterState, drainCluster); err != nil {
		return nil, err
	}
	return &vtboostpb.DrainClusterResponse{}, nil
}

func (c *Client) GetCluster(ctx context.Context, req *vtboostpb.GetClusterRequest) (*vtboostpb.GetClusterResponse, error) {
	if req.GetUuid() == "" {
		return nil, &Error{ErrClusterNotFound, errors.New("empty request UUID")}
	}

	clusters, err := boosttopo.Load[vtboostpb.ClusterStates](ctx, c.ts, boosttopo.PathClusterState)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return nil, &Error{ErrNoClusterStates, err}
		}

		return nil, err
	}

	for _, cluster := range clusters.GetClusters() {
		if cluster.GetUuid() != req.GetUuid() {
			continue
		}

		controller, err := boosttopo.Load[vtboostpb.ControllerState](ctx, c.ts, boosttopo.PathControllerState(cluster.GetUuid()))
		if err != nil {
			log.Warningf("boosttopo.Load: failed to lookup Controller state: %v", err)
			// add the Cluster anyway, but with a nil controller
		}

		return &vtboostpb.GetClusterResponse{
			Cluster:    cluster,
			Controller: controller,
		}, nil
	}

	return nil, &Error{ErrClusterNotFound, errors.New("no cluster matched requested UUID")}
}

func (c *Client) ListClusters(ctx context.Context, _ *vtboostpb.ListClustersRequest) (*vtboostpb.ListClustersResponse, error) {
	var resp vtboostpb.ListClustersResponse

	clusters, err := boosttopo.Load[vtboostpb.ClusterStates](ctx, c.ts, boosttopo.PathClusterState)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return nil, &Error{ErrNoClusterStates, err}
		}

		return nil, err
	}

	for _, cluster := range clusters.GetClusters() {
		state := &vtboostpb.ListClustersResponse_State{
			Cluster:    cluster,
			Controller: nil,
		}

		state.Controller, err = boosttopo.Load[vtboostpb.ControllerState](ctx, c.ts, boosttopo.PathControllerState(cluster.GetUuid()))
		if err != nil {
			log.Warningf("boosttopo.Load: failed to lookup Controller state: %v", err)
			// add the Cluster to the list anyway, but with a nil controller
		}

		resp.Clusters = append(resp.Clusters, state)
	}

	return &resp, nil
}

func (c *Client) PutRecipe(ctx context.Context, req *vtboostpb.PutRecipeRequest) (*vtboostpb.PutRecipeResponse, error) {
	// as a sanity check, try parsing the recipe and converting it back into Proto before we attempt
	// to update it into the topology
	recipe, err := boostplan.NewRecipeFromProto(req.GetRecipe().GetQueries())
	if err != nil {
		return nil, err
	}

	recipepb, err := boosttopo.Update(ctx, c.ts, boosttopo.PathActiveRecipe, func(recipepb *vtboostpb.Recipe) error {
		recipepb.Queries = recipe.ToProto()
		recipepb.Version++
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &vtboostpb.PutRecipeResponse{RecipeVersion: recipepb.Version}, nil
}

func (c *Client) GetRecipe(ctx context.Context, _ *vtboostpb.GetRecipeRequest) (*vtboostpb.GetRecipeResponse, error) {
	recipe, err := boosttopo.Load[vtboostpb.Recipe](ctx, c.ts, boosttopo.PathActiveRecipe)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return nil, &Error{ErrNoActiveRecipe, err}
		}
		return nil, err
	}
	return &vtboostpb.GetRecipeResponse{Recipe: recipe}, nil
}

func (c *Client) DescribeRecipe(ctx context.Context, req *vtboostpb.DescribeRecipeRequest) (*vtboostpb.DescribeRecipeResponse, error) {
	controller, err := boosttopo.Load[vtboostpb.ControllerState](ctx, c.ts, boosttopo.PathControllerState(req.GetUuid()))
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return nil, &Error{ErrClusterNotFound, err}
		}

		return nil, err
	}

	leader, grpcConn, err := boostrpc.NewControllerClient(controller.GetLeader())
	if err != nil {
		return nil, err
	}
	defer grpcConn.Close()

	var res vtboostpb.DescribeRecipeResponse
	switch req := req.GetRequest().(type) {
	case *vtboostpb.DescribeRecipeRequest_Graphviz:
		resp, err := leader.Graphviz(ctx, req.Graphviz)
		if err != nil {
			return nil, err
		}

		res.Response = &vtboostpb.DescribeRecipeResponse_Graphviz{
			Graphviz: resp,
		}
	default:
		return nil, &Error{ErrDescribe, err}
	}

	return &res, nil
}

func (c *Client) Purge(ctx context.Context, _ *vtboostpb.PurgeRequest) (*vtboostpb.PurgeResponse, error) {
	if err := boosttopo.Purge(ctx, c.ts); err != nil {
		return nil, err
	}

	return &vtboostpb.PurgeResponse{}, nil
}

func (c *Client) SetScience(ctx context.Context, req *vtboostpb.SetScienceRequest) (*vtboostpb.SetScienceResponse, error) {
	clusters, err := boosttopo.Load[vtboostpb.ClusterStates](ctx, c.ts, boosttopo.PathClusterState)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return nil, &Error{ErrNoClusterStates, err}
		}

		return nil, err
	}

	reqClusters := req.GetClusters()
	for _, cluster := range clusters.GetClusters() {
		// checking if the current cluster is the one we want to SetScience on
		if len(reqClusters) > 0 && !slices.Contains(reqClusters, cluster.GetUuid()) {
			continue
		}

		_, err = boosttopo.Update(ctx, c.ts, boosttopo.PathControllerState(cluster.GetUuid()), func(controllerState *vtboostpb.ControllerState) error {
			controllerState.Science = req.GetScience()
			return nil
		})
		if err != nil {
			log.Warningf("boosttopo.Update: failed to update Controller state: %v", err)
		}
	}

	return &vtboostpb.SetScienceResponse{}, nil
}

// An Error is a structured error used to generate RPC response codes when boost
// topo information cannot be get or set via RPC request.
type Error struct {
	Code ErrorCode
	Err  error
}

// An ErrorCode indicates the type of error stored in an Error.
type ErrorCode int

// Possible ErrorCode values.
const (
	ErrUnspecified ErrorCode = iota
	ErrClusterAlreadyDraining
	ErrClusterAlreadyExists
	ErrClusterAlreadyPrimary
	ErrClusterNotFound
	ErrDrainNoTimestamp
	ErrMultiplePrimaries
	ErrNoActiveRecipe
	ErrNoClusterStates
	ErrNoPrimaryOrWarming
	ErrDescribe
)

// GRPCCode returns the gRPC error code for an ErrorCode.
func (c ErrorCode) GRPCCode() codes.Code {
	// Keep alphabetized and in sync with ErrorCode.
	switch c {
	case ErrClusterAlreadyDraining:
		return codes.AlreadyExists
	case ErrClusterAlreadyExists:
		return codes.AlreadyExists
	case ErrClusterAlreadyPrimary:
		return codes.AlreadyExists
	case ErrClusterNotFound:
		return codes.NotFound
	case ErrDrainNoTimestamp:
		return codes.InvalidArgument
	case ErrMultiplePrimaries:
		return codes.InvalidArgument
	case ErrNoActiveRecipe:
		return codes.NotFound
	case ErrNoClusterStates:
		return codes.NotFound
	case ErrNoPrimaryOrWarming:
		return codes.InvalidArgument
	case ErrDescribe:
		return codes.InvalidArgument
	default:
		return codes.Internal
	}
}

// String returns a string description of an ErrorCode.
func (c ErrorCode) String() string {
	// Keep alphabetized and in sync with ErrorCode.
	switch c {
	case ErrClusterAlreadyDraining:
		return "cluster is already draining"
	case ErrClusterAlreadyExists:
		return "cluster already exists"
	case ErrClusterAlreadyPrimary:
		return "cluster is already primary"
	case ErrClusterNotFound:
		return "cluster not found"
	case ErrDrainNoTimestamp:
		return "drain no timestamp"
	case ErrMultiplePrimaries:
		return "multiple primaries"
	case ErrNoActiveRecipe:
		return "no active recipe"
	case ErrNoClusterStates:
		return "no cluster states"
	case ErrNoPrimaryOrWarming:
		return "no primary or warming"
	case ErrDescribe:
		return "invalid describe format"
	default:
		return "unknown"
	}
}

// Error implements error.
func (e *Error) Error() string {
	return fmt.Sprintf("boost client error %q: %v", e.Code, e.Err)
}

// Cause implements the Cause interface
func (e *Error) Cause() error {
	return e.Err
}
