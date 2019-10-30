/*
Copyright 2019 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	vtParams              mysql.ConnParams
	masterTabletParams    mysql.ConnParams
	replicaTabletParams   mysql.ConnParams
	replicaTabletGrpcPort int
	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	sqlSchema             = `
  create table t1(
	id bigint,
	value varchar(16),
	primary key(id)
) Engine=InnoDB;
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
	flag.Parse()

	exitCode := func() int {
		clusterInstance = &cluster.LocalProcessCluster{Cell: cell, Hostname: hostname}
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// List of users authorized to execute vschema ddl operations
		clusterInstance.VtGateExtraArgs = []string{"-vschema_ddl_authorized_users=%"}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		if err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, true); err != nil {
			return 1
		}

		// Start vtgate
		if err = clusterInstance.StartVtgate(); err != nil {
			return 1
		}
		// vtParams = mysql.ConnParams{
		// 	Host: clusterInstance.Hostname,
		// 	Port: clusterInstance.VtgateMySQLPort,
		// }

		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		var masterTabletPath string
		var replicaTabletPath string
		for _, tablet := range tablets {
			path := fmt.Sprintf(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tablet.TabletUID)))
			if tablet.Type == "master" {
				masterTabletPath = path
			} else {
				replicaTabletPath = path
				// replicaTabletAddr = fmt.Sprintf("http://localhost:%d", tablet.GrpcPort)
				replicaTabletGrpcPort = tablet.GrpcPort
			}
		}

		masterTabletParams = mysql.ConnParams{
			Uname:      "vt_dba",
			DbName:     "vt_" + keyspaceName,
			UnixSocket: masterTabletPath + "/mysql.sock",
		}
		replicaTabletParams = mysql.ConnParams{
			Uname:      "vt_dba",
			DbName:     "vt_" + keyspaceName,
			UnixSocket: replicaTabletPath + "/mysql.sock",
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}

func tmcLockTables(ctx context.Context, tabletGrpcPort int) error {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	vtablet := &tabletpb.Tablet{Hostname: hostname, PortMap: portMap}
	client := tmc.NewClient()
	return client.LockTables(ctx, vtablet)
}

func tmcUnlockTables(ctx context.Context, tabletGrpcPort int) error {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	vtablet := &tabletpb.Tablet{Hostname: hostname, PortMap: portMap}
	client := tmc.NewClient()
	return client.UnlockTables(ctx, vtablet)
}
