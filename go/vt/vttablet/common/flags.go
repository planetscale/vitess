/*
Copyright 2023 The Vitess Authors.

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

package vttablet

import (
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/servenv"
)

const (
	// VReplicationExperimentalFlags is a bitmask of experimental features in vreplication.
	VReplicationExperimentalFlagOptimizeInserts           = int64(1)
	VReplicationExperimentalFlagAllowNoBlobBinlogRowImage = int64(2)
	VReplicationExperimentalFlagVPlayerBatching           = int64(4)
)

var (
	// Default flags.
	VReplicationExperimentalFlags int64
	VReplicationNetReadTimeout    int
	VReplicationNetWriteTimeout   int
	CopyPhaseDuration             time.Duration
)

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
	servenv.OnParseFor("vtcombo", registerFlags)
}

func registerFlags(fs *pflag.FlagSet) {
	if VReplicationConfigFlags.Register(fs, &VReplicationExperimentalFlagsConfig{}) != nil {
		log.Warningf("Error registering vreplication_experimental_flags")
	}
	if VReplicationConfigFlags.Register(fs, &VReplicationNetReadTimeoutConfig{}) != nil {
		log.Warningf("Error registering vreplication_net_read_timeout")
	}
	if VReplicationConfigFlags.Register(fs, &VReplicationNetWriteTimeoutConfig{}) != nil {
		log.Warningf("Error registering vreplication_net_write_timeout")
	}
	if VReplicationConfigFlags.Register(fs, &CopyPhaseDurationConfig{}) != nil {
		log.Warningf("Error registering vreplication_copy_phase_duration")
	}
}

type VReplicationExperimentalFlagsConfig struct {
	ConfigFlag
	vreplicationExperimentalFlags int64
}

func (cf *VReplicationExperimentalFlagsConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("vreplication_experimental_flags")
	cf.vreplicationExperimentalFlags = VReplicationExperimentalFlagOptimizeInserts | VReplicationExperimentalFlagAllowNoBlobBinlogRowImage
	fs.Int64Var(&VReplicationExperimentalFlags, cf.FlagName(), cf.vreplicationExperimentalFlags,
		"(Bitmask) of experimental features in vreplication to enable")
}

func (cf *VReplicationExperimentalFlagsConfig) Merge(v string) error {
	value, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid value for vreplication_experimental_flags")
	}
	cf.vreplicationExperimentalFlags = value
	return nil
}

func (cf *VReplicationExperimentalFlagsConfig) Value() any {
	return cf.vreplicationExperimentalFlags
}

type VReplicationNetReadTimeoutConfig struct {
	ConfigFlag
	vreplicationNetReadTimeout int
}

func (cf *VReplicationNetReadTimeoutConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("vreplication_net_read_timeout")
	cf.vreplicationNetReadTimeout = 300
	fs.IntVar(&VReplicationNetReadTimeout, cf.FlagName(), cf.vreplicationNetReadTimeout,
		"Session value of net_read_timeout for vreplication, in seconds")
}

func (cf *VReplicationNetReadTimeoutConfig) Merge(v string) error {
	value, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("invalid value for vreplication_net_read_timeout")
	}
	cf.vreplicationNetReadTimeout = value
	return nil
}

func (cf *VReplicationNetReadTimeoutConfig) Value() any {
	return cf.vreplicationNetReadTimeout
}

type VReplicationNetWriteTimeoutConfig struct {
	ConfigFlag
	vreplicationNetWriteTimeout int
}

func (cf *VReplicationNetWriteTimeoutConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("vreplication_net_write_timeout")
	cf.vreplicationNetWriteTimeout = 600
	fs.IntVar(&VReplicationNetWriteTimeout, cf.FlagName(), cf.vreplicationNetWriteTimeout,
		"Session value of net_write_timeout for vreplication, in seconds")
}

func (cf *VReplicationNetWriteTimeoutConfig) Merge(v string) error {
	value, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("invalid value for vreplication_net_write_timeout")
	}
	cf.vreplicationNetWriteTimeout = value
	return nil
}

func (cf *VReplicationNetWriteTimeoutConfig) Value() any {
	return cf.vreplicationNetWriteTimeout
}

type CopyPhaseDurationConfig struct {
	ConfigFlag
	copyPhaseDuration time.Duration
}

func (cf *CopyPhaseDurationConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.SetFlagName("vreplication_copy_phase_duration")
	cf.copyPhaseDuration = 1 * time.Hour
	fs.DurationVar(&CopyPhaseDuration, cf.FlagName(), cf.copyPhaseDuration,
		"Duration for each copy phase loop (before running the next catchup: default 1h)")
}

func (cf *CopyPhaseDurationConfig) Merge(v string) error {
	value, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("invalid value for vreplication_copy_phase_duration")
	}
	cf.copyPhaseDuration = value
	return nil
}

func (cf *CopyPhaseDurationConfig) Value() any {
	return cf.copyPhaseDuration
}

var (
	_ IConfigFlag = (*VReplicationExperimentalFlagsConfig)(nil)
	_ IConfigFlag = (*VReplicationNetReadTimeoutConfig)(nil)
	_ IConfigFlag = (*VReplicationNetWriteTimeoutConfig)(nil)
	_ IConfigFlag = (*CopyPhaseDurationConfig)(nil)
)
