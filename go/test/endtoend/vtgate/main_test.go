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

package vtgate

import (
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	ClusterInstance *cluster.LocalProcessCluster
	KeyspaceName    = "ks"
	Cell            = "zone1"
	Hostname        = "localhost"
	SchemaSql       = `
create table user( 
	id bigint,
	name varchar(64),
	primary key(id)
) ENGINE=InnoDB;
`

	VSchema = `
{
	"sharded": true,
	"vindexes": {
		"hash_index": {
			"type": "hash"
		}
	},
	"tables": {
		"user": {
			"column_vindexes": [
				{
				"column": "id",
				"name": "hash_index"
				}
			]
		}
	}
}
`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ClusterInstance = &cluster.LocalProcessCluster{Cell: Cell, Hostname: Hostname}
		defer ClusterInstance.Teardown()

		// Start topo server
		err := ClusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSql,
			VSchema:   VSchema,
		}

		shardNames := []string{"-80", "80-"}
		if err = ClusterInstance.StartKeyspace(*keyspace, shardNames, 1, true); err != nil {
			return 1
		}

		// Start vtgate
		if err = ClusterInstance.StartVtgate(); err != nil {
			return 1
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}
