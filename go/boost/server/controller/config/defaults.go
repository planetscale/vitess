package config

import (
	"fmt"
	"time"
)

func DefaultMaterializationConfig() *Materialization {
	return &Materialization{
		PartialEnabled: true,
		UpqueryMode:    UpqueryGenerationMode_FULL_MIDFLOW_UPQUERIES,
	}
}

func DefaultConfig() *Config {
	return &Config{
		Shards:          0,
		Materialization: DefaultMaterializationConfig(),
		Domain: &Domain{
			ConcurrentReplays:  512,
			ReplayBatchTimeout: 100_000 * time.Nanosecond,
			UpqueryMode:        UpqueryMode_SELECT_GTID,
		},
		HeartbeatEvery:   1 * time.Second,
		HealthcheckEvery: 10 * time.Second,
		Quorum:           1,
		// TODO: enable Finkelstein by default
		Reuse:               ReuseType_NO_REUSE,
		EvictEvery:          5 * time.Second,
		WorkerReadTimeout:   40 * time.Second,
		VstreamStartTimeout: 1 * time.Minute,
		VstreamStartRetries: 10,
	}
}

// Set implements pflag.Value for UpqueryMode
func (um *UpqueryMode) Set(strVal string) error {
	v, ok := UpqueryMode_value[strVal]
	if !ok {
		return fmt.Errorf("invalid UpqueryMode: %q", strVal)
	}
	*um = UpqueryMode(v)
	return nil
}

// Type implements pflag.Type for UpqueryMode
func (um *UpqueryMode) Type() string {
	return um.String()
}