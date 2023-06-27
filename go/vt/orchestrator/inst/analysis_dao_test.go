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

package inst

import (
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/orchestrator/test"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// The initialSQL is a set of insert commands copied from a dump of an actual running VTOrc instances. The relevant insert commands are here.
	// This is a dump taken from a test running 4 tablets, zone1-101 is the primary, zone1-100 is a replica, zone1-112 is a rdonly and zone2-200 is a cross-cell replica.
	initialSQL = []string{
		`INSERT INTO database_instance VALUES('zone1-0000000112','localhost',10945,'2023-06-27 12:51:41','2023-06-27 12:51:41',36247180,'8.0.32','ROW',1,1,'vt-0000000112-bin.000001',14054,'localhost',10909,1,1,'vt-0000000100-bin.000001',13723,'vt-0000000100-bin.000001',13723,0,0,0,'[]','localhost:10909',1,'','',1,0,'vt-0000000112-relay-bin.000002',13955,0,1,0,'zone1','',50,0,0,0,1,'3a5ffc30-14e9-11ee-9f42-dea9b28274c0:1-47','3a5ffb18-14e9-11ee-bd17-41313679be4c','ks:0','2023-06-27 12:51:41','',0,1,0,0,'','Homebrew','8.0','FULL',5533084,0,0,'ON',1,'3a5ffc30-14e9-11ee-9f42-dea9b28274c0','','3a5ffc30-14e9-11ee-9f42-dea9b28274c0,3a5ffb18-14e9-11ee-bd17-41313679be4c',1,1,'',1000000000000000000,1,0,0,0,1,'',1,'','','','',0);`,
		`INSERT INTO database_instance VALUES('zone1-0000000100','localhost',10909,'2023-06-27 12:51:41','2023-06-27 12:51:41',1188890788,'8.0.32','ROW',1,1,'vt-0000000100-bin.000001',13723,'',0,0,0,'',0,'',0,NULL,NULL,1,'[{"Hostname":"","Port":10909}]','localhost:10909',0,'','',0,0,'',0,0,0,0,'zone1','',50,0,0,0,1,'3a5ffc30-14e9-11ee-9f42-dea9b28274c0:1-47','3a5ffc30-14e9-11ee-9f42-dea9b28274c0','ks:0','2023-06-27 12:51:41','',0,0,0,0,'','Homebrew','8.0','FULL',5611625,0,0,'ON',1,'','','3a5ffc30-14e9-11ee-9f42-dea9b28274c0',-1,-1,'',1000000000000000000,1,0,0,0,1,'',1,'','','','',0);`,
		`INSERT INTO database_instance VALUES('zone1-0000000101','localhost',10912,'2023-06-27 12:51:41','2023-06-27 12:51:41',1142751186,'8.0.32','ROW',1,1,'vt-0000000101-bin.000001',14054,'localhost',10909,1,1,'vt-0000000100-bin.000001',13723,'vt-0000000100-bin.000001',13723,0,0,0,'[]','localhost:10909',1,'','',1,0,'vt-0000000101-relay-bin.000002',13955,0,1,0,'zone1','',51,0,0,0,1,'3a5ffc30-14e9-11ee-9f42-dea9b28274c0:1-47','3a5ffb72-14e9-11ee-bc7a-41313575aaf8','ks:0','2023-06-27 12:51:41','',0,1,0,0,'','Homebrew','8.0','FULL',5989583,0,0,'ON',1,'3a5ffc30-14e9-11ee-9f42-dea9b28274c0','','3a5ffc30-14e9-11ee-9f42-dea9b28274c0,3a5ffb72-14e9-11ee-bc7a-41313575aaf8',1,1,'',1000000000000000000,1,0,0,0,1,'',1,'','','','',0);`,
		`INSERT INTO database_instance VALUES('zone2-0000000200','localhost',10951,'2023-06-27 12:51:40','2023-06-27 12:51:40',613518267,'8.0.32','ROW',1,1,'vt-0000000200-bin.000001',14054,'localhost',10909,1,1,'vt-0000000100-bin.000001',13723,'vt-0000000100-bin.000001',13723,0,0,0,'[]','localhost:10909',1,'','',1,0,'vt-0000000200-relay-bin.000002',13955,0,1,0,'zone2','',50,0,0,0,1,'3a5ffc30-14e9-11ee-9f42-dea9b28274c0:1-47','3a5ff6c2-14e9-11ee-8eb7-19918c0c98d3','ks:0','2023-06-27 12:51:40','',0,1,0,0,'','Homebrew','8.0','FULL',1863833,0,0,'ON',1,'3a5ffc30-14e9-11ee-9f42-dea9b28274c0','','3a5ffc30-14e9-11ee-9f42-dea9b28274c0,3a5ff6c2-14e9-11ee-8eb7-19918c0c98d3',1,1,'',1000000000000000000,1,0,0,0,1,'',1,'','','','',0);`,
		`INSERT INTO vitess_tablet VALUES('zone1-0000000100','localhost',10909,'ks','0','zone1',1,'2023-06-27 12:51:16.999937+00:00',X'616c6961733a7b63656c6c3a227a6f6e653122207569643a3130307d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a31303930387d20706f72745f6d61703a7b6b65793a227674222076616c75653a31303930377d206b657973706163653a226b73222073686172643a22302220747970653a5052494d415259206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a3130393039207072696d6172795f7465726d5f73746172745f74696d653a7b7365636f6e64733a31363837383730323736206e616e6f7365636f6e64733a3939393933373030307d2064625f7365727665725f76657273696f6e3a22382e302e3332222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
		`INSERT INTO vitess_tablet VALUES('zone1-0000000101','localhost',10912,'ks','0','zone1',2,'0001-01-01 00:00:00+00:00',X'616c6961733a7b63656c6c3a227a6f6e653122207569643a3130317d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a31303931317d20706f72745f6d61703a7b6b65793a227674222076616c75653a31303931307d206b657973706163653a226b73222073686172643a22302220747970653a5245504c494341206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a31303931322064625f7365727665725f76657273696f6e3a22382e302e3332222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
		`INSERT INTO vitess_tablet VALUES('zone1-0000000112','localhost',10945,'ks','0','zone1',3,'0001-01-01 00:00:00+00:00',X'616c6961733a7b63656c6c3a227a6f6e653122207569643a3131327d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a31303934347d20706f72745f6d61703a7b6b65793a227674222076616c75653a31303934337d206b657973706163653a226b73222073686172643a22302220747970653a52444f4e4c59206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a31303934352064625f7365727665725f76657273696f6e3a22382e302e3332222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
		`INSERT INTO vitess_tablet VALUES('zone2-0000000200','localhost',10951,'ks','0','zone2',2,'0001-01-01 00:00:00+00:00',X'616c6961733a7b63656c6c3a227a6f6e653222207569643a3230307d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a31303935307d20706f72745f6d61703a7b6b65793a227674222076616c75653a31303934397d206b657973706163653a226b73222073686172643a22302220747970653a5245504c494341206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a31303935312064625f7365727665725f76657273696f6e3a22382e302e3332222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
		`INSERT INTO vitess_keyspace VALUES('ks',0,'none');`,
	}
)

func TestGetReplicationAnalysis(t *testing.T) {
	tests := []struct {
		name       string
		info       []*test.InfoForRecoveryAnalysis
		codeWanted AnalysisCode
		wantErr    string
	}{
		{
			name: "ClusterHasNoPrimary",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy: "none",
				LastCheckValid:   1,
			}},
			codeWanted: ClusterHasNoPrimary,
		}, {
			name: "DeadPrimary",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy:              "none",
				LastCheckValid:                0,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 0,
				IsPrimary:                     1,
			}},
			codeWanted: DeadPrimary,
		}, {
			name: "DeadPrimaryWithoutReplicas",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy: "none",
				LastCheckValid:   0,
				CountReplicas:    0,
				IsPrimary:        1,
			}},
			codeWanted: DeadPrimaryWithoutReplicas,
		}, {
			name: "DeadPrimaryAndReplicas",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy: "none",
				LastCheckValid:   0,
				CountReplicas:    3,
				IsPrimary:        1,
			}},
			codeWanted: DeadPrimaryAndReplicas,
		}, {
			name: "DeadPrimaryAndSomeReplicas",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy:              "none",
				LastCheckValid:                0,
				CountReplicas:                 4,
				CountValidReplicas:            2,
				CountValidReplicatingReplicas: 0,
				IsPrimary:                     1,
			}},
			codeWanted: DeadPrimaryAndSomeReplicas,
		}, {
			name: "PrimaryHasPrimary",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy:   "none",
				LastCheckValid:     1,
				CountReplicas:      4,
				CountValidReplicas: 4,
				IsPrimary:          0,
			}},
			codeWanted: PrimaryHasPrimary,
		}, {
			name: "PrimaryIsReadOnly",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy:   "none",
				LastCheckValid:     1,
				CountReplicas:      4,
				CountValidReplicas: 4,
				IsPrimary:          1,
				ReadOnly:           1,
			}},
			codeWanted: PrimaryIsReadOnly,
		}, {
			name: "PrimarySemiSyncMustNotBeSet",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy:       "none",
				LastCheckValid:         1,
				CountReplicas:          4,
				CountValidReplicas:     4,
				IsPrimary:              1,
				SemiSyncPrimaryEnabled: 1,
			}},
			codeWanted: PrimarySemiSyncMustNotBeSet,
		}, {
			name: "PrimarySemiSyncMustBeSet",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy:       "semi_sync",
				LastCheckValid:         1,
				CountReplicas:          4,
				CountValidReplicas:     4,
				IsPrimary:              1,
				SemiSyncPrimaryEnabled: 0,
			}},
			codeWanted: PrimarySemiSyncMustBeSet,
		}, {
			name: "NotConnectedToPrimary",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6708,
				},
				DurabilityPolicy:              "none",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
			}, {
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				LastCheckValid: 1,
				ReadOnly:       1,
				IsPrimary:      1,
			}},
			codeWanted: NotConnectedToPrimary,
		}, {
			name: "ReplicaIsWritable",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6708,
				},
				DurabilityPolicy:              "none",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
			}, {TabletInfo: &topodatapb.Tablet{
				Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
				Hostname:      "localhost",
				Keyspace:      "ks",
				Shard:         "0",
				Type:          topodatapb.TabletType_REPLICA,
				MysqlHostname: "localhost",
				MysqlPort:     6709,
			},
				DurabilityPolicy: "none",
				LastCheckValid:   1,
				ReadOnly:         0,
			}},
			codeWanted: ReplicaIsWritable,
		}, {
			name: "ConnectedToWrongPrimary",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6708,
				},
				DurabilityPolicy:              "none",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
			}, {
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy: "none",
				LastCheckValid:   1,
				ReadOnly:         1,
			}},
			codeWanted: ConnectedToWrongPrimary,
		}, {
			name: "ReplicationStopped",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6708,
				},
				DurabilityPolicy:              "none",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
			}, {
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy: "none",
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
				},
				LastCheckValid:     1,
				ReadOnly:           1,
				ReplicationStopped: 1,
			}},
			codeWanted: ReplicationStopped,
		},
		{
			name: "ReplicaSemiSyncMustBeSet",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6708,
				},
				DurabilityPolicy:              "semi_sync",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
				SemiSyncPrimaryEnabled:        1,
			}, {
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
				},
				DurabilityPolicy:       "semi_sync",
				LastCheckValid:         1,
				ReadOnly:               1,
				SemiSyncReplicaEnabled: 0,
			}},
			codeWanted: ReplicaSemiSyncMustBeSet,
		}, {
			name: "ReplicaSemiSyncMustNotBeSet",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6708,
				},
				DurabilityPolicy:              "none",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
			}, {
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
				},
				DurabilityPolicy:       "none",
				LastCheckValid:         1,
				ReadOnly:               1,
				SemiSyncReplicaEnabled: 1,
			}},
			codeWanted: ReplicaSemiSyncMustNotBeSet,
		}, {
			name: "SnapshotKeyspace",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				// Snapshot Keyspace
				KeyspaceType:     1,
				DurabilityPolicy: "none",
				LastCheckValid:   1,
			}},
			codeWanted: NoProblem,
		}, {
			name: "EmptyDurabilityPolicy",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				LastCheckValid: 1,
			}},
			// We will ignore these keyspaces too until the durability policy is set in the topo server
			codeWanted: NoProblem,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rowMaps []sqlutils.RowMap
			for _, analysis := range tt.info {
				analysis.SetValuesFromTabletInfo()
				rowMaps = append(rowMaps, analysis.ConvertToRowMap())
			}
			db.Db = test.NewTestDB([][]sqlutils.RowMap{rowMaps})

			got, err := GetReplicationAnalysis("", &ReplicationAnalysisHints{})
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.codeWanted == NoProblem {
				require.Len(t, got, 0)
				return
			}
			require.Len(t, got, 1)
			require.Equal(t, tt.codeWanted, got[0].Analysis)
		})
	}
}

// TestAuditInstanceAnalysisInChangelog tests the functionality of the auditInstanceAnalysisInChangelog function
// and verifies that we write the correct number of times to the database.
func TestAuditInstanceAnalysisInChangelog(t *testing.T) {
	tests := []struct {
		name            string
		cacheExpiration time.Duration
	}{
		{
			name:            "Long expiration",
			cacheExpiration: 2 * time.Minute,
		}, {
			name:            "Very short expiration",
			cacheExpiration: 100 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the cache for the test to use.
			oldRecentInstantAnalysisCache := recentInstantAnalysis
			oldAnalysisChangeWriteCounter := analysisChangeWriteCounter

			recentInstantAnalysis = cache.New(tt.cacheExpiration, 100*time.Millisecond)
			analysisChangeWriteCounter = metrics.NewCounter()

			defer func() {
				// Set the old values back.
				recentInstantAnalysis = oldRecentInstantAnalysisCache
				analysisChangeWriteCounter = oldAnalysisChangeWriteCounter
				// Each test should clear the database. The easiest way to do that is to run all the initialization commands again.
				db.ClearOrchestratorDatabase()
			}()

			updates := []struct {
				tabletAlias             string
				analysisCode            AnalysisCode
				writeCounterExpectation int
				wantErr                 string
			}{
				{
					// Store a new analysis for the zone1-100 tablet.
					tabletAlias:             "zone1-100",
					analysisCode:            ReplicationStopped,
					writeCounterExpectation: 1,
				}, {
					// Write the same analysis, no new write should happen.
					tabletAlias:             "zone1-100",
					analysisCode:            ReplicationStopped,
					writeCounterExpectation: 1,
				}, {
					// Change the analysis. This should trigger an update.
					tabletAlias:             "zone1-100",
					analysisCode:            ReplicaSemiSyncMustBeSet,
					writeCounterExpectation: 2,
				},
			}

			for _, upd := range updates {
				// We sleep 200 milliseconds to make sure that the cache has had time to update.
				// It should be able to delete entries if the expiration is less than 200 milliseconds.
				time.Sleep(200 * time.Millisecond)
				err := auditInstanceAnalysisInChangelog(upd.tabletAlias, upd.analysisCode)
				if upd.wantErr != "" {
					require.EqualError(t, err, upd.wantErr)
					continue
				}
				require.NoError(t, err)
				require.EqualValues(t, upd.writeCounterExpectation, analysisChangeWriteCounter.Count())
			}
		})
	}
}
