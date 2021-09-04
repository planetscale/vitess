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
	"time"

	"vitess.io/vitess/go/event"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
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
		GetWaitReplicasTimeout() time.Duration
		GetWaitForRelayLogsTimeout() time.Duration
		HandleRelayLogFailure(err error) error
		GetIgnoreReplicas() sets.String
		CheckPrimaryRecoveryType() error
		RestrictValidCandidates(map[string]mysql.Position, map[string]*topo.TabletInfo) (map[string]mysql.Position, error)
		FindPrimaryCandidates(context.Context, logutil.Logger, tmclient.TabletManagerClient, map[string]mysql.Position, map[string]*topo.TabletInfo) (*topodatapb.Tablet, map[string]*topo.TabletInfo, error)
		PromotedReplicaIsIdeal(*topodatapb.Tablet, *topodatapb.Tablet, map[string]*topo.TabletInfo, map[string]mysql.Position) bool
		PostTabletChangeHook(*topodatapb.Tablet)
		GetBetterCandidate(*topodatapb.Tablet, *topodatapb.Tablet, []*topodatapb.Tablet, map[string]*topo.TabletInfo) *topodatapb.Tablet
		CheckIfNeedToOverridePromotion(newPrimary *topodatapb.Tablet) error
		StartReplication(context.Context, *events.Reparent, logutil.Logger, tmclient.TabletManagerClient) error
		GetNewPrimary() *topodatapb.Tablet

		// TODO: remove this
		SetMaps(map[string]*topo.TabletInfo, map[string]*replicationdatapb.StopReplicationStatus, map[string]*replicationdatapb.PrimaryStatus)
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
	_ ReparentFunctions = (*VtctlReparentFunctions)(nil)
)

// NewVtctlReparentFunctions creates a new VtctlReparentFunctions which is used in ERS ans PRS
func NewVtctlReparentFunctions(newPrimaryAlias *topodatapb.TabletAlias, ignoreReplicas sets.String, waitReplicasTimeout time.Duration, keyspace string, shard string, ts *topo.Server) *VtctlReparentFunctions {
	return &VtctlReparentFunctions{
		NewPrimaryAlias:     newPrimaryAlias,
		IgnoreReplicas:      ignoreReplicas,
		WaitReplicasTimeout: waitReplicasTimeout,
		keyspace:            keyspace,
		shard:               shard,
		ts:                  ts,
	}
}

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
	return nil
}

func (vtctlReparentFunctions *VtctlReparentFunctions) GetWaitReplicasTimeout() time.Duration {
	return vtctlReparentFunctions.WaitReplicasTimeout
}

func (vtctlReparentFunctions *VtctlReparentFunctions) GetWaitForRelayLogsTimeout() time.Duration {
	return vtctlReparentFunctions.WaitReplicasTimeout
}

func (vtctlReparentFunctions *VtctlReparentFunctions) HandleRelayLogFailure(err error) error {
	return err
}

func (vtctlReparentFunctions *VtctlReparentFunctions) GetIgnoreReplicas() sets.String {
	return vtctlReparentFunctions.IgnoreReplicas
}

// CheckPrimaryRecoveryType implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) CheckPrimaryRecoveryType() error {
	return nil
}

// RestrictValidCandidates implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) RestrictValidCandidates(validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (map[string]mysql.Position, error) {
	restrictedValidCandidates := make(map[string]mysql.Position)

	for candidate, position := range validCandidates {
		candidateInfo, ok := tabletMap[candidate]
		if !ok {
			return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", candidate)
		}

		if candidateInfo.Type != topodatapb.TabletType_PRIMARY && candidateInfo.Type != topodatapb.TabletType_REPLICA {
			continue
		}

		restrictedValidCandidates[candidate] = position
	}

	return restrictedValidCandidates, nil
}

// FindPrimaryCandidates implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) FindPrimaryCandidates(ctx context.Context, logger logutil.Logger, tmc tmclient.TabletManagerClient, validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (*topodatapb.Tablet, map[string]*topo.TabletInfo, error) {
	// Elect the candidate with the most up-to-date position.
	for alias, position := range validCandidates {
		if vtctlReparent.winningPosition.IsZero() || position.AtLeast(vtctlReparent.winningPosition) {
			vtctlReparent.winningPosition = position
			vtctlReparent.winningPrimaryTabletAliasStr = alias
		}
	}

	vtctlReparent.validCandidates = validCandidates

	// If we were requested to elect a particular primary, verify it's a valid
	// candidate (non-zero position, no errant GTIDs) and is at least as
	// advanced as the winning position.
	if vtctlReparent.NewPrimaryAlias != nil {
		vtctlReparent.winningPrimaryTabletAliasStr = topoproto.TabletAliasString(vtctlReparent.NewPrimaryAlias)
		pos, ok := vtctlReparent.validCandidates[vtctlReparent.winningPrimaryTabletAliasStr]
		switch {
		case !ok:
			return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "master elect %v has errant GTIDs", vtctlReparent.winningPrimaryTabletAliasStr)
		case !pos.AtLeast(vtctlReparent.winningPosition):
			return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "master elect %v at position %v is not fully caught up. Winning position: %v", vtctlReparent.winningPrimaryTabletAliasStr, pos, vtctlReparent.winningPosition)
		}
	}

	// TODO:= handle not found error
	newPrimaryAlias := tabletMap[vtctlReparent.winningPrimaryTabletAliasStr]
	return newPrimaryAlias.Tablet, tabletMap, nil
}

// 	PostReplicationChangeHook implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) PostTabletChangeHook(*topodatapb.Tablet) {
	return
}

// 	PromotedReplicaIsIdeal implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) PromotedReplicaIsIdeal(newPrimary, prevPrimary *topodatapb.Tablet, tabletMap map[string]*topo.TabletInfo, validCandidates map[string]mysql.Position) bool {
	if vtctlReparent.NewPrimaryAlias != nil {
		//explicit request to promote a specific tablet
		return true
	}
	if prevPrimary != nil {
		if (newPrimary.Type == topodatapb.TabletType_PRIMARY || newPrimary.Type == topodatapb.TabletType_REPLICA) && newPrimary.Alias.Cell == prevPrimary.Alias.Cell {
			return true
		}
		return false
	}

	if newPrimary.Type == topodatapb.TabletType_PRIMARY || newPrimary.Type == topodatapb.TabletType_REPLICA {
		return true
	}
	return false
}

// 	GetBetterCandidate implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) GetBetterCandidate(newPrimary, prevPrimary *topodatapb.Tablet, validCandidates []*topodatapb.Tablet, tabletMap map[string]*topo.TabletInfo) *topodatapb.Tablet {

	if prevPrimary != nil {
		// find one which is of the correct type and matches the cell of the previous primary
		for _, candidate := range validCandidates {
			if (candidate.Type == topodatapb.TabletType_PRIMARY || candidate.Type == topodatapb.TabletType_REPLICA) && prevPrimary.Alias.Cell == candidate.Alias.Cell {
				return candidate
			}
		}
	}
	for _, candidate := range validCandidates {
		if candidate.Type == topodatapb.TabletType_PRIMARY || candidate.Type == topodatapb.TabletType_REPLICA {
			return candidate
		}
	}
	return newPrimary
}

// CheckIfNeedToOverridePrimary implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) CheckIfNeedToOverridePromotion(newPrimary *topodatapb.Tablet) error {
	return nil
}

// StartReplication implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) StartReplication(ctx context.Context, ev *events.Reparent, logger logutil.Logger, tmc tmclient.TabletManagerClient) error {
	// Do the promotion.
	//return vtctlReparent.promoteNewPrimary(ctx, ev, logger, tmc)
	return nil
}

// GetNewPrimary implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) GetNewPrimary() *topodatapb.Tablet {
	return vtctlReparent.tabletMap[vtctlReparent.winningPrimaryTabletAliasStr].Tablet
}

func (vtctlReparent *VtctlReparentFunctions) SetMaps(tabletMap map[string]*topo.TabletInfo, statusMap map[string]*replicationdatapb.StopReplicationStatus, primaryStatusMap map[string]*replicationdatapb.PrimaryStatus) {
	vtctlReparent.tabletMap = tabletMap
	vtctlReparent.statusMap = statusMap
	vtctlReparent.primaryStatusMap = primaryStatusMap
}

func (vtctlReparent *VtctlReparentFunctions) getLockAction(newPrimaryAlias *topodatapb.TabletAlias) string {
	action := "EmergencyReparentShard"

	if newPrimaryAlias != nil {
		action += fmt.Sprintf("(%v)", topoproto.TabletAliasString(newPrimaryAlias))
	}

	return action
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

	_, err = reparentReplicasAndPopulateJournal(ctx, ev, logger, tmc, newPrimaryTabletInfo.Tablet, vtctlReparent.lockAction, rp, vtctlReparent.tabletMap, vtctlReparent.statusMap, vtctlReparent, false)
	return err
}
