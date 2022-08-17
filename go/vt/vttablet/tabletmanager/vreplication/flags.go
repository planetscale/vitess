package vreplication

import (
	"flag"
	"runtime"
	"strconv"
	"time"

	"vitess.io/vitess/go/vt/log"
)

var (
	// idleTimeout is set to slightly above 1s, compared to heartbeatTime
	// set by VStreamer at slightly below 1s. This minimizes conflicts
	// between the two timeouts.
	idleTimeout = 1100 * time.Millisecond

	dbLockRetryDelay = 1 * time.Second

	relayLogMaxSize  = flag.Int("relay_log_max_size", 250000, "Maximum buffer size (in bytes) for VReplication target buffering. If single rows are larger than this, a single row is buffered at a time.")
	relayLogMaxItems = flag.Int("relay_log_max_items", 5000, "Maximum number of rows for VReplication target buffering.")

	copyPhaseDuration   = flag.Duration("vreplication_copy_phase_duration", 1*time.Hour, "Duration for each copy phase loop (before running the next catchup: default 1h)")
	replicaLagTolerance = flag.Duration("vreplication_replica_lag_tolerance", 1*time.Minute, "Replica lag threshold duration: once lag is below this we switch from copy phase to the replication (streaming) phase")

	// vreplicationHeartbeatUpdateInterval determines how often the time_updated column is updated if there are no real events on the source and the source
	// vstream is only sending heartbeats for this long. Keep this low if you expect high QPS and are monitoring this column to alert about potential
	// outages. Keep this high if
	// 		you have too many streams the extra write qps or cpu load due to these updates are unacceptable
	//		you have too many streams and/or a large source field (lot of participating tables) which generates unacceptable increase in your binlog size
	vreplicationHeartbeatUpdateInterval = flag.Int("vreplication_heartbeat_update_interval", 1, "Frequency (in seconds, default 1, max 60) at which the time_updated column of a vreplication stream when idling")
	// vreplicationMinimumHeartbeatUpdateInterval overrides vreplicationHeartbeatUpdateInterval if the latter is higher than this
	// to ensure that it satisfies liveness criteria implicitly expected by internal processes like Online DDL
	vreplicationMinimumHeartbeatUpdateInterval = 60

	vreplicationExperimentalFlags                        = flag.Int64("vreplication_experimental_flags", 0x01, "(Bitmask) of experimental features in vreplication to enable")
	vreplicationParallelBulkInserts                      = flag.String("vreplication_parallel_bulk_inserts", "auto", "Number of parallel insertion workers to use during copy phase. Set to a number >= 1 or to \"auto\". \"auto\" uses GOMAXPROCS or 4, whichever is smaller.")
	vreplicationExperimentalFlagOptimizeInserts    int64 = 1
	vreplicationExperimentalParallelizeBulkInserts int64 = 0x02
	vreplicationStoreCompressedGTID                      = flag.Bool("vreplication_store_compressed_gtid", false, "Store compressed gtids in the pos column of _vt.vreplication")
)

func getCopyInsertConcurrency() int {
	if !isExperimentalParallelBulkInsertsEnabled() {
		return 1
	}
	copyInsertConcurrency := string(*vreplicationParallelBulkInserts)
	if copyInsertConcurrency == "auto" {
		gomaxprocs := runtime.GOMAXPROCS(0)
		if gomaxprocs <= 4 {
			return gomaxprocs
		}
		return 4
	}
	i64, err := strconv.ParseInt(copyInsertConcurrency, 10, 32)
	if err != nil {
		log.Warningf("Invalid value %s for --vreplication_parallel_bulk_inserts: %s. Defaulting to 1.", copyInsertConcurrency, err.Error())
		return 1
	}
	if i64 <= 0 {
		log.Warningf("Invalid value %d for --vreplication_parallel_bulk_inserts. Defaulting to 1.", copyInsertConcurrency)
		return 1
	}
	return int(i64)
}

func isExperimentalParallelBulkInsertsEnabled() bool {
	return *vreplicationExperimentalFlags /**/ & /**/ vreplicationExperimentalParallelizeBulkInserts != 0
}
