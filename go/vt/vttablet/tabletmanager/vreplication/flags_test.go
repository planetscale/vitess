package vreplication

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetCopyInsertConcurrency(t *testing.T) {
	gomaxprocs := runtime.GOMAXPROCS(0)

	defer func(experimentalFlags int64, parallelBulkInserts string, gomaxprocs int) {
		vreplicationExperimentalFlags = experimentalFlags
		vreplicationParallelBulkInserts = parallelBulkInserts
		runtime.GOMAXPROCS(gomaxprocs)
	}(vreplicationExperimentalFlags, vreplicationParallelBulkInserts, gomaxprocs)

	// Default, without experimental flag.
	require.Equal(t, false, isExperimentalParallelBulkInsertsEnabled(),
		"expected experimental parallel bulk inserts to be disabled by default")
	require.Equal(t, 1, getCopyInsertConcurrency(),
		"expected copy insert concurrency to default to 1 when experimental parallel bulk inserts is disabled")

	// Default, with experimental flag.
	vreplicationExperimentalFlags = vreplicationExperimentalFlags | vreplicationExperimentalParallelizeBulkInserts
	expected := gomaxprocs
	if expected > 4 {
		expected = 4
	}
	require.Equalf(t, expected, getCopyInsertConcurrency(),
		"expected copy insert concurrency %d expected when GOMAXPROCS=%d", expected, gomaxprocs)

	// Store GOMAXPROCS for subsequent tests.
	oldgomaxprocs := gomaxprocs

	// With 1 <= GOMAXPROCS <= 4
	for i := 1; i <= 4; i++ {
		if i == oldgomaxprocs {
			continue
		}

		runtime.GOMAXPROCS(i)
		require.Equalf(t, i, getCopyInsertConcurrency(),
			"expected copy insert concurrency %d when GOMAXPROCS=%d", i, gomaxprocs)
	}

	// With GOMAXPROCS > 4
	for i := 5; i <= 8; i++ {
		if i == oldgomaxprocs {
			continue
		}

		runtime.GOMAXPROCS(i)
		require.Equalf(t, 4, getCopyInsertConcurrency(),
			"expected copy insert concurrency %d when GOMAXPROCS=%d", 4, gomaxprocs)
	}

	// Reset GOMAXPROCS.
	runtime.GOMAXPROCS(gomaxprocs)

	// With invalid args.
	invalidArgs := []string{"-1", "0", "foo"}
	for _, v := range invalidArgs {
		vreplicationParallelBulkInserts = v
		require.Equalf(t, 1, getCopyInsertConcurrency(),
			"expected copy insert concurrency to fall back to %d when set to invalid value=%s", 1, v)
	}
}

func TestIsExperimentalParallelBulkInsertsEnabled(t *testing.T) {
	defer func(experimentalFlags int64) {
		vreplicationExperimentalFlags = experimentalFlags
	}(vreplicationExperimentalFlags)

	// Default, false
	require.False(t, isExperimentalParallelBulkInsertsEnabled(),
		"expected parallel bulk inserts disabled by default")

	vreplicationExperimentalFlags |= vreplicationExperimentalParallelizeBulkInserts
	require.True(t, isExperimentalParallelBulkInsertsEnabled(),
		"expected parallel bulk inserts to be enabled")
}
