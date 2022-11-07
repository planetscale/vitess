package topo

import (
	"fmt"
)

const pathBase = "boost"

const PathActiveRecipe = "boost/active_recipe"

const PathClusterState = "boost/cluster_state"

func PathWorker(uuid string, epoch int64) string {
	return fmt.Sprintf("boost/%s/epochs/%d/workers", uuid, epoch)
}

func PathLeader(uuid string) string {
	return fmt.Sprintf("boost/%s/leader", uuid)
}

func PathControllerState(uuid string) string {
	return fmt.Sprintf("boost/%s/controller_state", uuid)
}
