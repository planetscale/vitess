/*
Copyright 2021 The Vitess Authors.

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

package vttest

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vttest"

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	cluster        *vttest.LocalCluster
	vtParams       mysql.ConnParams
	mysqlParams    mysql.ConnParams
	grpcAddress    string
	tabletHostName = flag.String("tablet_hostname", "", "the tablet hostname")
	cfg            vttest.Config
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {

		cfg.Topology = &vttestpb.VTTestTopology{
			Keyspaces: getHundredKeyspaces(),
		}

		cfg.TabletHostName = *tabletHostName

		cluster = &vttest.LocalCluster{
			Config: cfg,
		}
		if err := cluster.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			cluster.TearDown()
			return 1
		}
		defer cluster.TearDown()
		vtParams = mysql.ConnParams{
			Host: "localhost",
			Port: cluster.Env.PortForProtocol("vtcombo_mysql_port", ""),
		}
		mysqlParams = cluster.MySQLConnParams()
		//
		//ctx := context.Background()
		//conn, err := mysql.Connect(ctx, &mysqlParams)
		//if err != nil {
		//	return 1
		//}
		//defer conn.Close()
		//_, err = conn.ExecuteFetch("set global max_connections = 100000", 1000, true)
		//if err != nil {
		//	return 1
		//}

		grpcAddress = fmt.Sprintf("localhost:%d", cluster.Env.PortForProtocol("vtcombo", "grpc"))
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestSetup(t *testing.T) {
	err := insertStartValue(t, cfg.Topology.Keyspaces)
	require.NoError(t, err)
}

func insertStartValue(t *testing.T, keyspaces []*vttestpb.Keyspace) error {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		return err
	}
	defer conn.Close()

	// lets insert a single starting value for each keyspace
	for i, keyspace := range keyspaces {
		_, err = conn.ExecuteFetch(fmt.Sprintf("create table %s.t1_last_insert_id(id1 int)", keyspace.Name), 1000, true)
		assert.NoError(t, err)
		if err != nil {
			continue
		}
		_, err = conn.ExecuteFetch(fmt.Sprintf("insert into %s.t1_last_insert_id(id1) values(%d)", keyspace.Name, i), 1000, true)
		assert.NoError(t, err)
	}
	return nil
}

func getHundredKeyspaces() []*vttestpb.Keyspace {
	var keyspaces []*vttestpb.Keyspace
	for i := 1; i <= 75; i++ {
		keyspaces = append(keyspaces, &vttestpb.Keyspace{
			Name: "ks" + strconv.Itoa(i),
			Shards: []*vttestpb.Shard{{
				Name: "-",
			}},
		})
	}
	return keyspaces
}
