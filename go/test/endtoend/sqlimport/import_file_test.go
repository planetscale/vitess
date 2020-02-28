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

package sqlimport

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	hostname              = "localhost"
	keyspaceName          = "test_keyspace"
	testingID             = 1
	tableName             = "vt_prepare_stmt_test"
	cell                  = "zone1"
	mysqlAuthServerStatic = "mysql_auth_server_static.json"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)

		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1, err
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1, err
		}

		output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("SqlImport", "-file=filepath")
		fmt.Println(output, err)
		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}
