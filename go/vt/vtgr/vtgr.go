package vtgr

import (
	"flag"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vtgr/config"

	"vitess.io/vitess/go/vt/vtgr/db"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgr/controller"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	refreshInterval      = flag.Duration("refresh_interval", 10*time.Second, "refresh interval to load tablets")
	scanInterval         = flag.Duration("scan_interval", 3*time.Second, "scan interval to diagnose and repair")
	scanAndRepairTimeout = flag.Duration("scan_repair_timeout", 3*time.Second, "time to wait for a Diagnose and repair operation")
	vtgrConfigFile       = flag.String("vtgr_config", "", "config file for vtgr")

	localDbPort = flag.Int("db_port", 0, "local mysql port, set this to enable local fast check")
)

// VTGR manages VTGR
type VTGR struct {
	// Shards are all the shards that a VTGR is monitored
	// caller can choose to iterate the shards to scan and repair for more granule control (e.g., stats report)
	// instead of calling ScanAndRepair() directly
	Shards []*controller.GRShard
	topo   controller.GRTopo
	tmc    tmclient.TabletManagerClient
	ctx    context.Context
}

func newVTGR(ctx context.Context, ts controller.GRTopo, tmc tmclient.TabletManagerClient) *VTGR {
	return &VTGR{
		topo: ts,
		tmc:  tmc,
		ctx:  ctx,
	}
}

// OpenTabletDiscovery opens connection with topo server
// and triggers the first round of controller based on parameter
func OpenTabletDiscovery(ctx context.Context, cellsToWatch, clustersToWatch []string) *VTGR {
	if *vtgrConfigFile == "" {
		log.Fatal("vtgr_config is required")
	}
	config, err := config.ReadVTGRConfig(*vtgrConfigFile)
	if err != nil {
		log.Fatalf("Cannot load vtgr config file: %v", err)
	}
	vtgr := newVTGR(
		ctx,
		topo.Open(),
		tmclient.NewTabletManagerClient(),
	)
	var shards []*controller.GRShard
	ctx, cancel := context.WithTimeout(vtgr.ctx, *topo.RemoteOperationTimeout)
	defer cancel()
	for _, ks := range clustersToWatch {
		if strings.Contains(ks, "/") {
			// This is a keyspace/shard specification
			input := strings.Split(ks, "/")
			shards = append(shards, controller.NewGRShard(input[0], input[1], cellsToWatch, vtgr.tmc, vtgr.topo, db.NewVTGRSqlAgent(), config, *localDbPort))
		} else {
			// Assume this is a keyspace and find all shards in keyspace
			shardNames, err := vtgr.topo.GetShardNames(ctx, ks)
			if err != nil {
				// Log the errr and continue
				log.Errorf("Error fetching shards for keyspace %v: %v", ks, err)
				continue
			}
			if len(shardNames) == 0 {
				log.Errorf("Topo has no shards for ks: %v", ks)
				continue
			}
			for _, s := range shardNames {
				shards = append(shards, controller.NewGRShard(ks, s, cellsToWatch, vtgr.tmc, vtgr.topo, db.NewVTGRSqlAgent(), config, *localDbPort))
			}
		}
	}
	vtgr.Shards = shards
	log.Infof("Monitoring shards size %v", len(vtgr.Shards))
	// Force refresh all tablet here to populate data for vtgr
	var wg sync.WaitGroup
	for _, shard := range vtgr.Shards {
		wg.Add(1)
		go func(shard *controller.GRShard) {
			defer wg.Done()
			shard.UpdateTabletsInShardWithLock(ctx)
		}(shard)
	}
	wg.Wait()
	log.Info("Ready to start VTGR")
	return vtgr
}

// RefreshCluster get the latest tablets from topo server
func (vtgr *VTGR) RefreshCluster() {
	for _, shard := range vtgr.Shards {
		go func(shard *controller.GRShard) {
			ticker := time.Tick(*refreshInterval)
			for range ticker {
				ctx, cancel := context.WithTimeout(vtgr.ctx, *refreshInterval)
				shard.UpdateTabletsInShardWithLock(ctx)
				cancel()
			}
		}(shard)
	}
}

// ScanAndRepair starts the scanAndFix routine
func (vtgr *VTGR) ScanAndRepair() {
	for _, shard := range vtgr.Shards {
		go func(shard *controller.GRShard) {
			ticker := time.Tick(*scanInterval)
			for range ticker {
				func() {
					ctx, cancel := context.WithTimeout(vtgr.ctx, *scanAndRepairTimeout)
					defer cancel()
					log.Infof("Start scan and repair %v/%v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
					shard.ScanAndRepairShard(ctx)
					log.Infof("Finished scan and repair %v/%v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
				}()
			}
		}(shard)
	}
}

// Diagnose exposes the endpoint to diagnose a particular shard
func (vtgr *VTGR) Diagnose(ctx context.Context, shard *controller.GRShard) (controller.DiagnoseType, error) {
	return shard.Diagnose(ctx)
}

// Repair exposes the endpoint to repair a particular shard
func (vtgr *VTGR) Repair(ctx context.Context, shard *controller.GRShard, diagnose controller.DiagnoseType) (controller.RepairResultCode, error) {
	return shard.Repair(ctx, diagnose)
}

// GetCurrentShardStatuses is used when we want to know what VTGR observes
// it contains information about a list of instances and primary tablet
func (vtgr *VTGR) GetCurrentShardStatuses() []controller.ShardStatus {
	var result []controller.ShardStatus
	for _, shard := range vtgr.Shards {
		status := shard.GetCurrentShardStatuses()
		result = append(result, status)
	}
	return result
}
