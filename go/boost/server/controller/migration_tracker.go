package controller

import (
	"errors"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
)

type migrationTracker struct {
	Migration

	topo             *toposerver.Server
	epoch            boostpb.Epoch
	newRecipeVersion int64
}

func parseErrors(multiError error) (systemErrors []string, queryErrors map[string]string) {
	queryErrors = make(map[string]string)
	for _, err := range multierr.Errors(multiError) {
		if scoped, ok := err.(boostplan.QueryError); ok {
			queryErrors[scoped.QueryPublicID()] = scoped.Error()
		} else {
			systemErrors = append(systemErrors, err.Error())
		}
	}
	return
}

func (mig *migrationTracker) trackActivation(activation *boostplan.ActivationResult, planErr error) {
	systemErrors, queryErrors := parseErrors(planErr)

	_, err := mig.topo.UpdateControllerState(mig.Migration, func(state *vtboostpb.ControllerState) error {
		return mig.updateRecipeStatus(state, func(status *vtboostpb.ControllerState_RecipeStatus) {
			status.Progress = vtboostpb.ControllerState_RecipeStatus_APPLYING
			status.SystemErrors = append(status.SystemErrors, systemErrors...)

			for publicID, err := range queryErrors {
				status.Queries = append(status.Queries, &vtboostpb.ControllerState_RecipeStatus_Query{
					QueryPublicId: publicID,
					Progress:      vtboostpb.ControllerState_RecipeStatus_FAILED,
					Error:         err,
				})
			}

			for _, existing := range activation.QueriesUnchanged {
				status.Queries = append(status.Queries, &vtboostpb.ControllerState_RecipeStatus_Query{
					QueryPublicId: existing.PublicId,
					Progress:      vtboostpb.ControllerState_RecipeStatus_READY,
				})
			}

			for _, added := range activation.QueriesAdded {
				if _, failed := queryErrors[added.PublicId]; failed {
					continue
				}
				status.Queries = append(status.Queries, &vtboostpb.ControllerState_RecipeStatus_Query{
					QueryPublicId: added.PublicId,
					Progress:      vtboostpb.ControllerState_RecipeStatus_APPLYING,
				})
			}

			for _, removed := range activation.QueriesRemoved {
				if _, failed := queryErrors[removed.PublicId]; failed {
					continue
				}
				status.Queries = append(status.Queries, &vtboostpb.ControllerState_RecipeStatus_Query{
					QueryPublicId: removed.PublicId,
					Progress:      vtboostpb.ControllerState_RecipeStatus_REMOVING,
				})
			}
		})
	})
	if err != nil {
		mig.Log().Error("UpdateControllerState failed; recipe may not be properly applied", zap.Error(err))
	}
}

func (mig *migrationTracker) trackCommit(commitErr error) {
	systemErrors, queryErrors := parseErrors(commitErr)
	success := len(systemErrors) == 0 && len(queryErrors) == 0

	_, err := mig.topo.UpdateControllerState(mig.Migration, func(state *vtboostpb.ControllerState) error {
		if success {
			// Upgrade recipe version if we've committed cleanly
			state.RecipeVersion = mig.newRecipeVersion
		}

		return mig.updateRecipeStatus(state, func(status *vtboostpb.ControllerState_RecipeStatus) {
			if success {
				status.Progress = vtboostpb.ControllerState_RecipeStatus_READY
			} else {
				status.Progress = vtboostpb.ControllerState_RecipeStatus_FAILED
			}

			status.SystemErrors = append(status.SystemErrors, systemErrors...)

			for _, query := range status.Queries {
				switch query.Progress {
				case vtboostpb.ControllerState_RecipeStatus_FAILED:
					// nothing to do; query was previously failed

				case vtboostpb.ControllerState_RecipeStatus_APPLYING, vtboostpb.ControllerState_RecipeStatus_REMOVING:
					if err, ok := queryErrors[query.QueryPublicId]; ok {
						query.Progress = vtboostpb.ControllerState_RecipeStatus_FAILED
						query.Error = err
					} else {
						query.Progress = vtboostpb.ControllerState_RecipeStatus_READY
					}
				}
			}
		})
	})
	if err != nil {
		mig.Log().Error("UpdateControllerState failed; recipe may not be properly applied", zap.Error(err))
	}
}

var errInvalidEpoch = errors.New("epoch race (somebody else already updated our controller state?)")

func (mig *migrationTracker) updateRecipeStatus(state *vtboostpb.ControllerState, update func(status *vtboostpb.ControllerState_RecipeStatus)) error {
	if boostpb.Epoch(state.Epoch) > mig.epoch {
		return errInvalidEpoch
	}
	if state.RecipeVersionStatus == nil {
		state.RecipeVersionStatus = make(map[int64]*vtboostpb.ControllerState_RecipeStatus)
	}
	version := mig.newRecipeVersion
	status, ok := state.RecipeVersionStatus[version]
	if !ok {
		status = &vtboostpb.ControllerState_RecipeStatus{Version: version}
		state.RecipeVersionStatus[version] = status
	}
	update(status)
	return nil
}

func (mig *migrationTracker) Activate(recipe *boostplan.VersionedRecipe, schema *boostplan.SchemaInformation) (*boostplan.ActivationResult, error) {
	mig.newRecipeVersion = recipe.Version()

	result, err := mig.Migration.Activate(recipe, schema)
	mig.trackActivation(result, err)
	return result, err
}

func (mig *migrationTracker) Commit() error {
	err := mig.Migration.Commit()
	mig.trackCommit(err)
	return err
}

func NewTrackedMigration(mig Migration, ctrl *Controller) Migration {
	return &migrationTracker{
		Migration: mig,
		topo:      ctrl.topo,
		epoch:     ctrl.epoch,
	}
}
