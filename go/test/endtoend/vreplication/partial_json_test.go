package vreplication

import (
	"github.com/stretchr/testify/require"
	"testing"
	"vitess.io/vitess/go/test/utils"
)

func TestPartialJSON(t *testing.T) {
	defaultRdonly = 1
	defaultReplicas = 0
	cells := []string{"zone1"}

	vc = NewVitessCluster(t, &clusterOptions{cells: cells})
	defer vc.TearDown()

	keyspace := "product"
	shard := "0"

	require.NoError(t, utils.SetBinlogRowImageMode("noblob", vc.ClusterConfig.tmpDir))
	defer utils.SetBinlogRowImageMode("", vc.ClusterConfig.tmpDir)

	cell1 := vc.Cells["zone1"]
	vc.AddKeyspace(t, []*Cell{cell1}, keyspace, shard, initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, sourceKsOpts)

	verifyClusterHealth(t, vc)
	insertInitialData(t)

	// vtgateConn, cancel := getVTGateConn()
	// defer cancel()
}
