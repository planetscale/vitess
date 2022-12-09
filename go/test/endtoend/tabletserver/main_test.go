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

package tabletserver

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/grpctabletconn"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

var (
	clusterInstance                  *cluster.LocalProcessCluster
	queryService                     queryservice.QueryService
	primaryTablet                    cluster.Vttablet
	primaryTarget                    *querypb.Target
	hostname                         = "localhost"
	keyspaceName                     = "ks"
	cell                             = "zone1"
	tabletHealthcheckRefreshInterval = 5 * time.Second
	tabletUnhealthyThreshold         = tabletHealthcheckRefreshInterval * 2
	sqlSchema                        = `
	create table t1(
		id bigint,
		value varchar(16),
		primary key(id)
	) Engine=InnoDB DEFAULT CHARSET=utf8;
	CREATE VIEW v1 AS SELECT id, value FROM t1;
`

	vSchema = `
	{
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
      "t1": {
        "column_vindexes": [
          {
            "column": "id",
            "name": "hash"
          }
        ]
      }
    }
  }`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Set extra tablet args for lock timeout
		clusterInstance.VtTabletExtraArgs = []string{
			"--lock_tables_timeout", "5s",
			"--watch_replication_stream",
			"--heartbeat_enable",
			"--health_check_interval", tabletHealthcheckRefreshInterval.String(),
			"--unhealthy_threshold", tabletUnhealthyThreshold.String(),
			"--shutdown_grace_period", "2",
		}
		// We do not need semiSync for this test case.
		clusterInstance.EnableSemiSync = false

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}

		if err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false); err != nil {
			return 1
		}

		// Collect table paths and ports
		primaryTablet = *clusterInstance.Keyspaces[0].Shards[0].Vttablets[0]

		// create grpc client
		queryService, err = grpctabletconn.DialTablet(getTablet(primaryTablet.GrpcPort), true)
		if err != nil {
			return 1
		}

		primaryTarget = &querypb.Target{
			Keyspace:   keyspaceName,
			Shard:      primaryTablet.VttabletProcess.Shard,
			TabletType: topodatapb.TabletType_PRIMARY,
			Cell:       cell,
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func getTablet(tabletGrpcPort int) *topodatapb.Tablet {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	return &topodatapb.Tablet{Hostname: hostname, PortMap: portMap}
}

func waitForCheckMySQLRunning(t *testing.T, valWanted float64) {
	t.Helper()
	timeout := time.After(30 * time.Second)
	for {
		_, err := queryService.Execute(context.Background(), primaryTarget, "select id, value from t1 where id = 1", nil, 0, 0, &querypb.ExecuteOptions{})
		status := primaryTablet.VttabletProcess.GetVars()
		val, exists := status["CheckMySQLRunning"]
		log.Errorf("%v, %v, %T, %v", exists, val, val, err)
		if exists && val.(float64) == valWanted {
			log.Errorf("returning from waitfor checkmysql")
			return
		}

		select {
		case <-timeout:
			t.Fatalf("CheckMySQL didn't run in the time provided")
		default:
			time.Sleep(1 * time.Second)
		}
	}
}
