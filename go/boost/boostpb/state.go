package boostpb

import (
	"time"
)

func DefaultConfig() *Config {
	return &Config{
		Shards:         0,
		PartialEnabled: true,
		FrontierStrategy: &FrontierStrategy{
			Type:  FrontierStrategyType_NONE,
			Match: "",
		},
		DomainConfig: &DomainConfig{
			ConcurrentReplays:  512,
			ReplayBatchTimeout: 100_000 * time.Nanosecond,
		},
		HeartbeatEvery:   1 * time.Second,
		HealthcheckEvery: 10 * time.Second,
		Quorum:           1,
		// TODO: enable Finkelstein by default
		Reuse:      ReuseType_NO_REUSE,
		EvictEvery: 5 * time.Second,
	}
}
