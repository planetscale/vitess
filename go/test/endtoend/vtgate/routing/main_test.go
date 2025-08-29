/*
Copyright 2020 The Vitess Authors.

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

package unsharded

import (
	"context"
	_ "embed"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	cell            = "zone1"
	hostname        = "localhost"
	sks             = "source"
	tks             = "target"

	//go:embed schema.sql
	SchemaSQL string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		sourceKs := cluster.Keyspace{Name: sks, SchemaSQL: SchemaSQL}
		if err := clusterInstance.StartUnshardedKeyspace(sourceKs, 1, false); err != nil {
			log.Fatal(err.Error())
			return 1
		}

		targetKs := cluster.Keyspace{Name: tks}
		if err := clusterInstance.StartUnshardedKeyspace(targetKs, 1, false); err != nil {
			log.Fatal(err.Error())
			return 1
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			log.Fatal(err.Error())
			return 1
		}

		// Also check we can create procedures through the vtgate.
		vtParams = mysql.ConnParams{
			Host: "localhost",
			Port: clusterInstance.VtgateMySQLPort,
		}
		conn, err := mysql.Connect(context.Background(), &vtParams)
		if err != nil {
			log.Fatal(err.Error())
			return 1
		}
		defer conn.Close()

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestQueriesWithRoutingRules(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		time.Sleep(2 * time.Second)
	}()
	startQueries(t, ctx)

	workflow := "TestQueriesWithRoutingRules"
	mtw := cluster.NewMoveTables(t, clusterInstance, workflow, tks, sks, "t1", nil)
	out, err := mtw.Create()
	t.Logf("movetables created: %s", out)
	require.NoError(t, err)

	mtw.WaitForVreplCatchup(5 * time.Second)
	t.Logf("movetables catchup phase completed")

	time.Sleep(2 * time.Second)
	out, err = mtw.SwitchReads()
	t.Logf("movetables switch reads: %v", out)
	require.NoError(t, err)
}

func startQueries(t *testing.T, ctx context.Context) {
	users := 1000
	conns := make([]*mysql.Conn, 0, users)
	for range users {
		conn, err := mysql.Connect(ctx, &vtParams)
		require.NoError(t, err)
		conns = append(conns, conn)
	}

	query := "select * from t1"
	for _, conn := range conns {
		go func(conn *mysql.Conn) {
			defer conn.Close()
			if _, err := conn.ExecuteFetch("use @replica", 1000, true); err != nil {
				t.Logf("error in use @primary: (%d, %v)", conn.ID(), err)
			}
			for {
				if ctx.Err() != nil {
					return
				}
				// if _, err := conn.ExecuteFetch("use @primary", 1000, true); err != nil {
				// 	t.Logf("error in use @primary: (%d, %v)", conn.ID(), err)
				// }
				// if _, err := conn.ExecuteFetch(query, 1000, true); err != nil {
				// 	t.Logf("error in primary query: (%d, %v)", conn.ID(), err)
				// }
				if _, err := conn.ExecuteFetch(query, 1000, true); err != nil {
					t.Logf("error in replica query: (%d, %v)", conn.ID(), err)
				}
				// time.Sleep(10 * time.Millisecond)
			}
		}(conn)
	}
}
