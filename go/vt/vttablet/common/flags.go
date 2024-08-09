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
	VReplicationNetReadTimeout    = 300
	VReplicationNetWriteTimeout   = 600
	CopyPhaseDuration             = 1 * time.Hour
)

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
	servenv.OnParseFor("vtcombo", registerFlags)
}

func registerFlags(fs *pflag.FlagSet) {
	if VReplicationConfigFlags.Register(fs, &VReplicationExperimentalFlagsConfig{}) != nil {
		log.Warningf("Error registering vreplication_experimental_flags")
	}
	fs.IntVar(&VReplicationNetReadTimeout, "vreplication_net_read_timeout", VReplicationNetReadTimeout, "Session value of net_read_timeout for vreplication, in seconds")
	fs.IntVar(&VReplicationNetWriteTimeout, "vreplication_net_write_timeout", VReplicationNetWriteTimeout, "Session value of net_write_timeout for vreplication, in seconds")
	fs.DurationVar(&CopyPhaseDuration, "vreplication_copy_phase_duration", CopyPhaseDuration, "Duration for each copy phase loop (before running the next catchup: default 1h)")
}

type VReplicationExperimentalFlagsConfig struct {
	ConfigFlag
	vreplicationExperimentalFlags int64
}

func (cf *VReplicationExperimentalFlagsConfig) New(flagName string, fs *pflag.FlagSet) {
	cf.flagName = "vreplication_experimental_flags"
	cf.vreplicationExperimentalFlags = VReplicationExperimentalFlagOptimizeInserts | VReplicationExperimentalFlagAllowNoBlobBinlogRowImage
	fs.Int64Var(&VReplicationExperimentalFlags, cf.flagName, cf.vreplicationExperimentalFlags,
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

var (
	_ IConfigFlag = (*VReplicationExperimentalFlagsConfig)(nil)
)
