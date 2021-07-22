/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reparentutil

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/event"

	"vitess.io/vitess/go/vt/concurrency"

	"vitess.io/vitess/go/mysql"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"

	"vitess.io/vitess/go/vt/topotools/events"

	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/vt/orchestrator/logic"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

type (
	// ReparentFunctions is an interface which has all the functions implementation required for re-parenting
	ReparentFunctions interface {
		LockShard(context.Context) (context.Context, func(*error), error)
		GetTopoServer() *topo.Server
		GetKeyspace() string
		GetShard() string
		CheckIfFixed() bool
		PreRecoveryProcesses(context.Context) error
		StopReplicationAndBuildStatusMaps(context.Context, tmclient.TabletManagerClient, *events.Reparent, logutil.Logger) error
		GetPrimaryRecoveryType() logic.MasterRecoveryType
		AddError(string) error
		FindPrimaryCandidates(context.Context, logutil.Logger, tmclient.TabletManagerClient) error
		CheckIfNeedToOverridePrimary() error
		StartReplication(context.Context, *events.Reparent, logutil.Logger, tmclient.TabletManagerClient) error
		GetNewPrimary() *topodatapb.Tablet
	}

	// VtctlReparentFunctions is the Vtctl implementation for ReparentFunctions
	VtctlReparentFunctions struct {
		NewPrimaryAlias              *topodatapb.TabletAlias
		IgnoreReplicas               sets.String
		WaitReplicasTimeout          time.Duration
		keyspace                     string
		shard                        string
		ts                           *topo.Server
		lockAction                   string
		tabletMap                    map[string]*topo.TabletInfo
		statusMap                    map[string]*replicationdatapb.StopReplicationStatus
		primaryStatusMap             map[string]*replicationdatapb.PrimaryStatus
		validCandidates              map[string]mysql.Position
		winningPosition              mysql.Position
		winningPrimaryTabletAliasStr string
	}
)

var (
	_ ReparentFunctions = (*logic.VtOrcReparentFunctions)(nil)
	_ ReparentFunctions = (*VtctlReparentFunctions)(nil)
)

// LockShard implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) LockShard(ctx context.Context) (context.Context, func(*error), error) {
	vtctlReparent.lockAction = vtctlReparent.getLockAction(vtctlReparent.NewPrimaryAlias)

	return vtctlReparent.ts.LockShard(ctx, vtctlReparent.keyspace, vtctlReparent.shard, vtctlReparent.lockAction)
}

// GetTopoServer implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) GetTopoServer() *topo.Server {
	return vtctlReparent.ts
}

// GetKeyspace implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) GetKeyspace() string {
	return vtctlReparent.keyspace
}

// GetShard implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) GetShard() string {
	return vtctlReparent.shard
}

// CheckIfFixed implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) CheckIfFixed() bool {
	return false
}

// PreRecoveryProcesses implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) PreRecoveryProcesses(ctx context.Context) error {
	var err error
	vtctlReparent.tabletMap, err = vtctlReparent.ts.GetTabletMapForShard(ctx, vtctlReparent.keyspace, vtctlReparent.shard)
	if err != nil {
		return vterrors.Wrapf(err, "failed to get tablet map for %v/%v: %v", vtctlReparent.keyspace, vtctlReparent.shard, err)
	}
	return nil
}

// StopReplicationAndBuildStatusMaps implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) StopReplicationAndBuildStatusMaps(ctx context.Context, tmc tmclient.TabletManagerClient, ev *events.Reparent, logger logutil.Logger) error {
	var err error
	vtctlReparent.statusMap, vtctlReparent.primaryStatusMap, err = StopReplicationAndBuildStatusMaps(ctx, tmc, ev, vtctlReparent.tabletMap, vtctlReparent.WaitReplicasTimeout, vtctlReparent.IgnoreReplicas, logger)
	if err != nil {
		return vterrors.Wrapf(err, "failed to stop replication and build status maps: %v", err)
	}
	return nil
}

// GetPrimaryRecoveryType implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) GetPrimaryRecoveryType() logic.MasterRecoveryType {
	return logic.MasterRecoveryGTID
}

// AddError implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) AddError(errorMsg string) error {
	return vterrors.New(vtrpc.Code_INTERNAL, errorMsg)
}

// FindPrimaryCandidates implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) FindPrimaryCandidates(ctx context.Context, logger logutil.Logger, tmc tmclient.TabletManagerClient) error {
	validCandidates, err := FindValidEmergencyReparentCandidates(vtctlReparent.statusMap, vtctlReparent.primaryStatusMap)
	if err != nil {
		return err
	} else if len(validCandidates) == 0 {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}

	// Wait for all candidates to apply relay logs
	if err := vtctlReparent.waitForAllRelayLogsToApply(ctx, logger, tmc, validCandidates); err != nil {
		return err
	}

	// Elect the candidate with the most up-to-date position.

	for alias, position := range validCandidates {
		if vtctlReparent.winningPosition.IsZero() || position.AtLeast(vtctlReparent.winningPosition) {
			vtctlReparent.winningPosition = position
			vtctlReparent.winningPrimaryTabletAliasStr = alias
		}
	}

	vtctlReparent.validCandidates = validCandidates

	return nil
}

// CheckIfNeedToOverridePrimary implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) CheckIfNeedToOverridePrimary() error {
	// If we were requested to elect a particular primary, verify it's a valid
	// candidate (non-zero position, no errant GTIDs) and is at least as
	// advanced as the winning position.
	if vtctlReparent.NewPrimaryAlias != nil {
		vtctlReparent.winningPrimaryTabletAliasStr = topoproto.TabletAliasString(vtctlReparent.NewPrimaryAlias)
		pos, ok := vtctlReparent.validCandidates[vtctlReparent.winningPrimaryTabletAliasStr]
		switch {
		case !ok:
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "master elect %v has errant GTIDs", vtctlReparent.winningPrimaryTabletAliasStr)
		case !pos.AtLeast(vtctlReparent.winningPosition):
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "master elect %v at position %v is not fully caught up. Winning position: %v", vtctlReparent.winningPrimaryTabletAliasStr, pos, vtctlReparent.winningPosition)
		}
	}
	return nil
}

// StartReplication implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) StartReplication(ctx context.Context, ev *events.Reparent, logger logutil.Logger, tmc tmclient.TabletManagerClient) error {
	// Do the promotion.
	return vtctlReparent.promoteNewPrimary(ctx, ev, logger, tmc)
}

// GetNewPrimary implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) GetNewPrimary() *topodatapb.Tablet {
	return vtctlReparent.tabletMap[vtctlReparent.winningPrimaryTabletAliasStr].Tablet
}

func (vtctlReparent *VtctlReparentFunctions) getLockAction(newPrimaryAlias *topodatapb.TabletAlias) string {
	action := "EmergencyReparentShard"

	if newPrimaryAlias != nil {
		action += fmt.Sprintf("(%v)", topoproto.TabletAliasString(newPrimaryAlias))
	}

	return action
}

func (vtctlReparent *VtctlReparentFunctions) waitForAllRelayLogsToApply(ctx context.Context, logger logutil.Logger, tmc tmclient.TabletManagerClient, validCandidates map[string]mysql.Position) error {
	errCh := make(chan error)
	defer close(errCh)

	groupCtx, groupCancel := context.WithTimeout(ctx, vtctlReparent.WaitReplicasTimeout)
	defer groupCancel()

	waiterCount := 0

	for candidate := range validCandidates {
		// When we called StopReplicationAndBuildStatusMaps, we got back two
		// maps: (1) the StopReplicationStatus of any replicas that actually
		// stopped replication; and (2) the MasterStatus of anything that
		// returned ErrNotReplica, which is a tablet that is either the current
		// primary or is stuck thinking it is a MASTER but is not in actuality.
		//
		// If we have a tablet in the validCandidates map that does not appear
		// in the statusMap, then we have either (a) the current primary, which
		// is not replicating, so it is not applying relay logs; or (b) a tablet
		// that is stuck thinking it is MASTER but is not in actuality. In that
		// second case - (b) - we will most likely find that the stuck MASTER
		// does not have a winning position, and fail the ERS. If, on the other
		// hand, it does have a winning position, we are trusting the operator
		// to know what they are doing by emergency-reparenting onto that
		// tablet. In either case, it does not make sense to wait for relay logs
		// to apply on a tablet that was never applying relay logs in the first
		// place, so we skip it, and log that we did.
		status, ok := vtctlReparent.statusMap[candidate]
		if !ok {
			logger.Infof("EmergencyReparent candidate %v not in replica status map; this means it was not running replication (because it was formerly MASTER), so skipping WaitForRelayLogsToApply step for this candidate", candidate)
			continue
		}

		go func(alias string, status *replicationdatapb.StopReplicationStatus) {
			var err error
			defer func() { errCh <- err }()
			err = WaitForRelayLogsToApply(groupCtx, tmc, vtctlReparent.tabletMap[alias], status)
		}(candidate, status)

		waiterCount++
	}

	errgroup := concurrency.ErrorGroup{
		NumGoroutines:        waiterCount,
		NumRequiredSuccesses: waiterCount,
		NumAllowedErrors:     0,
	}
	rec := errgroup.Wait(groupCancel, errCh)

	if len(rec.Errors) != 0 {
		return vterrors.Wrapf(rec.Error(), "could not apply all relay logs within the provided WaitReplicasTimeout (%s): %v", vtctlReparent.WaitReplicasTimeout, rec.Error())
	}

	return nil
}

func (vtctlReparent *VtctlReparentFunctions) promoteNewPrimary(ctx context.Context, ev *events.Reparent, logger logutil.Logger, tmc tmclient.TabletManagerClient) error {
	logger.Infof("promoting tablet %v to master", vtctlReparent.winningPrimaryTabletAliasStr)
	event.DispatchUpdate(ev, "promoting replica")

	newPrimaryTabletInfo, ok := vtctlReparent.tabletMap[vtctlReparent.winningPrimaryTabletAliasStr]
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "attempted to promote master-elect %v that was not in the tablet map; this an impossible situation", vtctlReparent.winningPrimaryTabletAliasStr)
	}

	rp, err := tmc.PromoteReplica(ctx, newPrimaryTabletInfo.Tablet)
	if err != nil {
		return vterrors.Wrapf(err, "master-elect tablet %v failed to be upgraded to master: %v", vtctlReparent.winningPrimaryTabletAliasStr, err)
	}

	if err := topo.CheckShardLocked(ctx, vtctlReparent.keyspace, vtctlReparent.shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	replCtx, replCancel := context.WithTimeout(ctx, vtctlReparent.WaitReplicasTimeout)
	defer replCancel()

	event.DispatchUpdate(ev, "reparenting all tablets")

	// Create a context and cancel function to watch for the first successful
	// SetMaster call on a replica. We use a background context so that this
	// context is only ever Done when its cancel is called by the background
	// goroutine we're about to spin up.
	//
	// Similarly, create a context and cancel for the replica waiter goroutine
	// to signal when all replica goroutines have finished. In the case where at
	// least one replica succeeds, replSuccessCtx will be canceled first, while
	// allReplicasDoneCtx is guaranteed to be canceled within
	// opts.WaitReplicasTimeout plus some jitter.
	replSuccessCtx, replSuccessCancel := context.WithCancel(context.Background())
	allReplicasDoneCtx, allReplicasDoneCancel := context.WithCancel(context.Background())

	now := time.Now().UnixNano()
	replWg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}

	handlePrimary := func(alias string, ti *topo.TabletInfo) error {
		logger.Infof("populating reparent journal on new master %v", alias)
		return tmc.PopulateReparentJournal(replCtx, ti.Tablet, now, vtctlReparent.lockAction, newPrimaryTabletInfo.Alias, rp)
	}

	handleReplica := func(alias string, ti *topo.TabletInfo) {
		defer replWg.Done()
		logger.Infof("setting new master on replica %v", alias)

		forceStart := false
		if status, ok := vtctlReparent.statusMap[alias]; ok {
			fs, err := ReplicaWasRunning(status)
			if err != nil {
				err = vterrors.Wrapf(err, "tablet %v could not determine StopReplicationStatus: %v", alias, err)
				rec.RecordError(err)

				return
			}

			forceStart = fs
		}

		err := tmc.SetMaster(replCtx, ti.Tablet, newPrimaryTabletInfo.Alias, now, "", forceStart)
		if err != nil {
			err = vterrors.Wrapf(err, "tablet %v SetMaster failed: %v", alias, err)
			rec.RecordError(err)

			return
		}

		// Signal that at least one goroutine succeeded to SetMaster.
		replSuccessCancel()
	}

	numReplicas := 0

	for alias, ti := range vtctlReparent.tabletMap {
		switch {
		case alias == vtctlReparent.winningPrimaryTabletAliasStr:
			continue
		case !vtctlReparent.IgnoreReplicas.Has(alias):
			replWg.Add(1)
			numReplicas++
			go handleReplica(alias, ti)
		}
	}

	// Spin up a background goroutine to wait until all replica goroutines
	// finished. Polling this way allows us to have promoteNewPrimary return
	// success as soon as (a) the primary successfully populates its reparent
	// journal and (b) at least one replica successfully begins replicating.
	//
	// If we were to follow the more common pattern of blocking on replWg.Wait()
	// in the main body of promoteNewPrimary, we would be bound to the
	// time of slowest replica, instead of the time of the fastest successful
	// replica, and we want ERS to be fast.
	go func() {
		replWg.Wait()
		allReplicasDoneCancel()
	}()

	primaryErr := handlePrimary(vtctlReparent.winningPrimaryTabletAliasStr, newPrimaryTabletInfo)
	if primaryErr != nil {
		logger.Warningf("master failed to PopulateReparentJournal")
		replCancel()

		return vterrors.Wrapf(primaryErr, "failed to PopulateReparentJournal on master: %v", primaryErr)
	}

	select {
	case <-replSuccessCtx.Done():
		// At least one replica was able to SetMaster successfully
		return nil
	case <-allReplicasDoneCtx.Done():
		// There are certain timing issues between replSuccessCtx.Done firing
		// and allReplicasDoneCtx.Done firing, so we check again if truly all
		// replicas failed (where `numReplicas` goroutines recorded an error) or
		// one or more actually managed to succeed.
		errCount := len(rec.Errors)

		switch {
		case errCount > numReplicas:
			// Technically, rec.Errors should never be greater than numReplicas,
			// but it's better to err on the side of caution here, but also
			// we're going to be explicit that this is doubly unexpected.
			return vterrors.Wrapf(rec.Error(), "received more errors (= %d) than replicas (= %d), which should be impossible: %v", errCount, numReplicas, rec.Error())
		case errCount == numReplicas:
			return vterrors.Wrapf(rec.Error(), "%d replica(s) failed: %v", numReplicas, rec.Error())
		default:
			return nil
		}
	}
}

// ChooseNewPrimary finds a tablet that should become a primary after reparent.
// The criteria for the new primary-elect are (preferably) to be in the same
// cell as the current primary, and to be different from avoidPrimaryAlias. The
// tablet with the most advanced replication position is chosen to minimize the
// amount of time spent catching up with the current primary.
//
// Note that the search for the most advanced replication position will race
// with transactions being executed on the current primary, so when all tablets
// are at roughly the same position, then the choice of new primary-elect will
// be somewhat unpredictable.
func ChooseNewPrimary(
	ctx context.Context,
	tmc tmclient.TabletManagerClient,
	shardInfo *topo.ShardInfo,
	tabletMap map[string]*topo.TabletInfo,
	avoidPrimaryAlias *topodatapb.TabletAlias,
	waitReplicasTimeout time.Duration,
	// (TODO:@ajm188) it's a little gross we need to pass this, maybe embed in the context?
	logger logutil.Logger,
) (*topodatapb.TabletAlias, error) {
	if avoidPrimaryAlias == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "tablet to avoid for reparent is not provided, cannot choose new primary")
	}

	var primaryCell string
	if shardInfo.MasterAlias != nil {
		primaryCell = shardInfo.MasterAlias.Cell
	}

	var (
		searcher = topotools.NewMaxReplicationPositionSearcher(tmc, logger, waitReplicasTimeout)
		wg       sync.WaitGroup
	)

	for _, tablet := range tabletMap {
		switch {
		case primaryCell != "" && tablet.Alias.Cell != primaryCell:
			continue
		case topoproto.TabletAliasEqual(tablet.Alias, avoidPrimaryAlias):
			continue
		case tablet.Tablet.Type != topodatapb.TabletType_REPLICA:
			continue
		}

		wg.Add(1)

		go func(tablet *topodatapb.Tablet) {
			defer wg.Done()
			searcher.ProcessTablet(ctx, tablet)
		}(tablet.Tablet)
	}

	wg.Wait()

	if maxPosTablet := searcher.MaxPositionTablet(); maxPosTablet != nil {
		return maxPosTablet.Alias, nil
	}

	return nil, nil
}

// FindCurrentPrimary returns the current primary tablet of a shard, if any. The
// current primary is whichever tablet of type MASTER (if any) has the most
// recent MasterTermStartTime, which is the same rule that vtgate uses to route
// master traffic.
//
// The return value is nil if the current primary cannot be definitively
// determined. This can happen either if no tablet claims to be type MASTER, or
// if multiple tablets claim to be type MASTER and happen to have the same
// MasterTermStartTime timestamp (a tie).
//
// The tabletMap must be a complete map (not a partial result) for the shard.
func FindCurrentPrimary(tabletMap map[string]*topo.TabletInfo, logger logutil.Logger) *topo.TabletInfo {
	var (
		currentPrimary       *topo.TabletInfo
		currentTermStartTime time.Time
	)

	for _, tablet := range tabletMap {
		if tablet.Type != topodatapb.TabletType_MASTER {
			continue
		}

		if currentPrimary == nil {
			currentPrimary = tablet
			currentTermStartTime = tablet.GetMasterTermStartTime()
			continue
		}

		otherPrimaryTermStartTime := tablet.GetMasterTermStartTime()
		if otherPrimaryTermStartTime.After(currentTermStartTime) {
			currentPrimary = tablet
			currentTermStartTime = otherPrimaryTermStartTime
		} else if otherPrimaryTermStartTime.Equal(currentTermStartTime) {
			// A tie should not happen unless the upgrade order was violated
			// (e.g. some vttablets have not been upgraded) or if we get really
			// unlucky.
			//
			// Either way, we need to be safe and not assume we know who the
			// true primary is.
			logger.Warningf(
				"Multiple primaries (%v and %v) are tied for MasterTermStartTime; can't determine the true primary.",
				topoproto.TabletAliasString(currentPrimary.Alias),
				topoproto.TabletAliasString(tablet.Alias),
			)

			return nil
		}
	}

	return currentPrimary
}
