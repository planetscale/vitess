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

// This program generates a fully escaped command line for
// issuing a VReplicationExec command.
// Change the contents of the data structures in the main
// program to match your requirements and issue 'go run vreplgen.go'

package main

import (
	"bytes"
	"fmt"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func main() {
	vtctl := "$kvtctl"
	dbName := "vt_customer"
	streams := []struct {
		tabletid string
		bls      *binlogdatapb.BinlogSource
	}{{
		tabletid: "zone1-1581380800",
		bls: &binlogdatapb.BinlogSource{
			Keyspace: "product",
			Shard:    "0",
			Filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "product",
					Filter: "select * from product",
				}},
			},
			OnDdl: binlogdatapb.OnDDLAction_IGNORE,
		},
	}, {
		tabletid: "zone1-822947600",
		bls: &binlogdatapb.BinlogSource{
			Keyspace: "product",
			Shard:    "0",
			Filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "product",
					Filter: "select * from product",
				}},
			},
			OnDdl: binlogdatapb.OnDDLAction_IGNORE,
		},
	}}

	for _, stream := range streams {
		val := sqltypes.NewVarBinary(fmt.Sprintf("%v", stream.bls))
		var sqlEscaped bytes.Buffer
		val.EncodeSQL(&sqlEscaped)
		query := fmt.Sprintf("insert into _vt.vreplication "+
			"(db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values"+
			"('%s', %s, '', 9999, 9999, 'master', 0, 0, 'Running')", dbName, sqlEscaped.String())

		fmt.Printf("%s VReplicationExec %s '%s'\n", vtctl, stream.tabletid, strings.Replace(query, "'", "'\"'\"'", -1))
	}
}
