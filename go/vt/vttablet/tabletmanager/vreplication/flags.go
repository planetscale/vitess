/*
Copyright 2022 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"strconv"
	"time"

	"vitess.io/vitess/go/vt/log"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

var (
	retryDelay          time.Duration
	maxTimeToRetryError time.Duration // Default behavior is to keep retrying, for backward compatibility

	tabletTypesStr = "in_order:REPLICA,PRIMARY" // Default value

	relayLogMaxSize  int
	relayLogMaxItems int

	replicaLagTolerance time.Duration

	vreplicationHeartbeatUpdateInterval int

	vreplicationStoreCompressedGTID   bool
	vreplicationParallelInsertWorkers int
)

func registerVReplicationFlags(fs *pflag.FlagSet) {
	if vttablet.VReplicationConfigFlags.Register(fs, &VreplicationRetryDelayConfig{}) != nil {
		log.Warningf("Error registering vreplication_retry_delay")
	}
	if vttablet.VReplicationConfigFlags.Register(fs, &VreplicationMaxTimeToRetryOnErrorConfig{}) != nil {
		log.Warningf("Error registering vreplication_max_time_to_retry_on_error")
	}
	if vttablet.VReplicationConfigFlags.Register(fs, &RelayLogMaxSizeConfig{}) != nil {
		log.Warningf("Error registering relay_log_max_size")
	}
	if vttablet.VReplicationConfigFlags.Register(fs, &RelayLogMaxItemsConfig{}) != nil {
		log.Warningf("Error registering relay_log_max_items")
	}
	if vttablet.VReplicationConfigFlags.Register(fs, &ReplicaLagToleranceConfig{}) != nil {
		log.Warningf("Error registering vreplication_replica_lag_tolerance")
	}

	// vreplicationHeartbeatUpdateInterval determines how often the time_updated column is updated if there are no real events on the source and the source
	// vstream is only sending heartbeats for this long. Keep this low if you expect high QPS and are monitoring this column to alert about potential
	// outages. Keep this high if
	// 		you have too many streams the extra write qps or cpu load due to these updates are unacceptable
	//		you have too many streams and/or a large source field (lot of participating tables) which generates unacceptable increase in your binlog size
	if vttablet.VReplicationConfigFlags.Register(fs, &VreplicationHeartbeatUpdateIntervalConfig{}) != nil {
		log.Warningf("Error registering vreplication_heartbeat_update_interval")
	}

	if vttablet.VReplicationConfigFlags.Register(fs, &VreplicationStoreCompressedGTIDConfig{}) != nil {
		log.Warningf("Error registering vreplication_store_compressed_gtid")
	}
	if vttablet.VReplicationConfigFlags.Register(fs, &VreplicationParallelInsertWorkersConfig{}) != nil {
		log.Warningf("Error registering vreplication-parallel-insert-workers")
	}
}

type VreplicationRetryDelayConfig struct {
	vttablet.ConfigFlag
	vreplicationRetryDelay time.Duration
}

func (cf *VreplicationRetryDelayConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("vreplication_retry_delay")
	cf.vreplicationRetryDelay = 5 * time.Second
	fs.DurationVar(&retryDelay, cf.FlagName(), cf.vreplicationRetryDelay,
		"delay before retrying a failed workflow event in the replication phase")
}

func (cf *VreplicationRetryDelayConfig) Merge(v string) error {
	value, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("invalid value for vreplication_retry_delay")
	}
	cf.vreplicationRetryDelay = value
	return nil
}

func (cf *VreplicationRetryDelayConfig) Value() any {
	return cf.vreplicationRetryDelay
}

type VreplicationMaxTimeToRetryOnErrorConfig struct {
	vttablet.ConfigFlag
	vreplicationMaxTimeToRetryOnError time.Duration
}

func (cf *VreplicationMaxTimeToRetryOnErrorConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("vreplication_max_time_to_retry_on_error")
	cf.vreplicationMaxTimeToRetryOnError = 5 * time.Second
	fs.DurationVar(&maxTimeToRetryError, cf.FlagName(), cf.vreplicationMaxTimeToRetryOnError,
		"stop automatically retrying when we've had consecutive failures with the same error for this long after the first occurrence")
}

func (cf *VreplicationMaxTimeToRetryOnErrorConfig) Merge(v string) error {
	value, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("invalid value for vreplication_max_time_to_retry_on_error")
	}
	cf.vreplicationMaxTimeToRetryOnError = value
	return nil
}

func (cf *VreplicationMaxTimeToRetryOnErrorConfig) Value() any {
	return cf.vreplicationMaxTimeToRetryOnError
}

type RelayLogMaxSizeConfig struct {
	vttablet.ConfigFlag
	relayLogMaxSize int
}

func (cf *RelayLogMaxSizeConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("relay_log_max_size")
	cf.relayLogMaxSize = 25000
	fs.IntVar(&relayLogMaxSize, cf.FlagName(), cf.relayLogMaxSize, "Maximum buffer size (in bytes) for VReplication target buffering. If single rows are larger than this, a single row is buffered at a time.")
}

func (cf *RelayLogMaxSizeConfig) Merge(v string) error {
	value, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("invalid value for relay_log_max_size")
	}
	cf.relayLogMaxSize = value
	return nil
}

func (cf *RelayLogMaxSizeConfig) Value() any {
	return cf.relayLogMaxSize
}

type RelayLogMaxItemsConfig struct {
	vttablet.ConfigFlag
	relayLogMaxItems int
}

func (cf *RelayLogMaxItemsConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("relay_log_max_items")
	cf.relayLogMaxItems = 5000
	fs.IntVar(&relayLogMaxItems, cf.FlagName(), cf.relayLogMaxItems, "Maximum number of rows for VReplication target buffering.")
}

func (cf *RelayLogMaxItemsConfig) Merge(v string) error {
	value, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("invalid value for relay_log_max_items")
	}
	cf.relayLogMaxItems = value
	return nil
}

func (cf *RelayLogMaxItemsConfig) Value() any {
	return cf.relayLogMaxItems
}

type ReplicaLagToleranceConfig struct {
	vttablet.ConfigFlag
	replicaLagTolerance time.Duration
}

func (cf *ReplicaLagToleranceConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("vreplication_replica_lag_tolerance")
	cf.replicaLagTolerance = 1 * time.Minute
	fs.DurationVar(&replicaLagTolerance, cf.FlagName(), cf.replicaLagTolerance, "Replica lag threshold duration: once lag is below this we switch from copy phase to the replication (streaming) phase")
}

func (cf *ReplicaLagToleranceConfig) Merge(v string) error {
	value, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("invalid value for vreplication_replica_lag_tolerance")
	}
	cf.replicaLagTolerance = value
	return nil
}

func (cf *ReplicaLagToleranceConfig) Value() any {
	return cf.replicaLagTolerance
}

type VreplicationHeartbeatUpdateIntervalConfig struct {
	vttablet.ConfigFlag
	vreplicationHeartbeatUpdateInterval int
}

func (cf *VreplicationHeartbeatUpdateIntervalConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("vreplication_heartbeat_update_interval")
	cf.vreplicationHeartbeatUpdateInterval = 1
	fs.IntVar(&vreplicationHeartbeatUpdateInterval, cf.FlagName(), cf.vreplicationHeartbeatUpdateInterval, "Frequency (in seconds, default 1, max 60) at which the time_updated column of a vreplication stream when idling")
}

func (cf *VreplicationHeartbeatUpdateIntervalConfig) Merge(v string) error {
	value, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("invalid value for vreplication_heartbeat_update_interval")
	}
	cf.vreplicationHeartbeatUpdateInterval = value
	return nil
}

func (cf *VreplicationHeartbeatUpdateIntervalConfig) Value() any {
	return cf.vreplicationHeartbeatUpdateInterval
}

type VreplicationStoreCompressedGTIDConfig struct {
	vttablet.ConfigFlag
	vreplicationStoreCompressedGTID bool
}

func (cf *VreplicationStoreCompressedGTIDConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("vreplication_store_compressed_gtid")
	cf.vreplicationStoreCompressedGTID = false
	fs.BoolVar(&vreplicationStoreCompressedGTID, cf.FlagName(), cf.vreplicationStoreCompressedGTID, "Store compressed gtids in the pos column of the sidecar database's vreplication table")
}

func (cf *VreplicationStoreCompressedGTIDConfig) Merge(v string) error {
	value, err := strconv.ParseBool(v)
	if err != nil {
		return fmt.Errorf("invalid value for vreplication_store_compressed_gtid")
	}
	cf.vreplicationStoreCompressedGTID = value
	return nil
}

func (cf *VreplicationStoreCompressedGTIDConfig) Value() any {
	return cf.vreplicationStoreCompressedGTID
}

type VreplicationParallelInsertWorkersConfig struct {
	vttablet.ConfigFlag
	vreplicationParallelInsertWorkers int
}

func (cf *VreplicationParallelInsertWorkersConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("vreplication-parallel-insert-workers")
	cf.vreplicationParallelInsertWorkers = 1
	fs.IntVar(&vreplicationParallelInsertWorkers, cf.FlagName(), cf.vreplicationParallelInsertWorkers, "Number of parallel insertion workers to use during copy phase. Set <= 1 to disable parallelism, or > 1 to enable concurrent insertion during copy phase.")
}

func (cf *VreplicationParallelInsertWorkersConfig) Merge(v string) error {
	value, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("invalid value for vreplication-parallel-insert-workers")
	}
	cf.vreplicationParallelInsertWorkers = value
	return nil
}

func (cf *VreplicationParallelInsertWorkersConfig) Value() any {
	return cf.vreplicationParallelInsertWorkers
}

func init() {
	servenv.OnParseFor("vtcombo", registerVReplicationFlags)
	servenv.OnParseFor("vttablet", registerVReplicationFlags)
}

var (
	_ vttablet.IConfigFlag = &VreplicationRetryDelayConfig{}
	_ vttablet.IConfigFlag = &VreplicationMaxTimeToRetryOnErrorConfig{}
	_ vttablet.IConfigFlag = &RelayLogMaxSizeConfig{}
	_ vttablet.IConfigFlag = &RelayLogMaxItemsConfig{}
	_ vttablet.IConfigFlag = &ReplicaLagToleranceConfig{}
	_ vttablet.IConfigFlag = &VreplicationHeartbeatUpdateIntervalConfig{}
	_ vttablet.IConfigFlag = &VreplicationStoreCompressedGTIDConfig{}
	_ vttablet.IConfigFlag = &VreplicationParallelInsertWorkersConfig{}
)
