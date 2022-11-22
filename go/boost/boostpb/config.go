package boostpb

import (
	"fmt"
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
			UpqueryMode:        UpqueryMode_SELECT_GTID,
		},
		HeartbeatEvery:   1 * time.Second,
		HealthcheckEvery: 10 * time.Second,
		Quorum:           1,
		// TODO: enable Finkelstein by default
		Reuse:             ReuseType_NO_REUSE,
		EvictEvery:        5 * time.Second,
		WorkerReadTimeout: 40 * time.Second,
	}
}

// Set implements flag.Value for UpqueryMode
func (f *UpqueryMode) Set(strVal string) error {
	v, ok := UpqueryMode_value[strVal]
	if !ok {
		return fmt.Errorf("invalid UpqueryMode: %q", strVal)
	}
	*f = UpqueryMode(v)
	return nil
}
