package booste2e

import (
	"context"
	"flag"
	"fmt"
	"html/template"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"

	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
	"vitess.io/vitess/go/boost/topo/client"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
)

var flagGtidMode = flag.String("gtid-mode", "SELECT_GTID", "GTID tracking mode for upqueries (options are 'SELECT_GTID', 'TRACK_GTID')")

const DefaultKeyspace = "source"
const DefaultSchemaChangeUser = "root"

type Test struct {
	testing.TB

	Vitess      *cluster.LocalProcessCluster
	Topo        *topo.Server
	VtParams    *mysql.ConnParams
	MySQLParams *mysql.ConnParams

	Keyspace         string
	Cell             string
	SchemaSQL        string
	VSchema          string
	SchemaChangeUser string
	Shards           []string

	BoostTestCluster *boosttest.Cluster
	BoostProcesses   []*BoostProcess
	BoostTopo        *client.Client

	ClusterUUID string
	Recipe      *vtboost.Recipe

	vtgateconn *mysql.Conn
}

func WithRecipe(name string) Option {
	return func(test *Test) {
		recipe := testrecipe.Load(test, name)
		test.Recipe = recipe.ToProto()
		test.SchemaSQL = recipe.ToStringDDL(test.Keyspace)
	}
}

func WithDDL(ddl string) Option {
	return func(test *Test) {
		test.SchemaSQL = ddl
	}
}

func WithCachedQueries(queriesSql ...string) Option {
	return func(test *Test) {
		if test.Recipe == nil {
			test.Recipe = &vtboost.Recipe{Version: 1}
		}
		for _, sql := range queriesSql {
			test.Recipe.Queries = append(test.Recipe.Queries, &vtboost.CachedQuery{
				Name:     fmt.Sprintf("anonymous_query_%d", len(test.Recipe.Queries)),
				Sql:      sql,
				Keyspace: test.Keyspace,
			})
		}
	}
}

func WithBoostInstance(args ...string) Option {
	return func(test *Test) {
		test.BoostProcesses = append(test.BoostProcesses, &BoostProcess{ExtraArgs: args})
	}
}

type Option func(test *Test)

func Setup(t testing.TB, options ...Option) *Test {
	if testing.Short() {
		t.Skipf("skipping End-To-End test when running in short mode")
	}

	t.Cleanup(func() {
		boosttest.EnsureNoLeaks(t)
	})

	test := &Test{
		TB:               t,
		Keyspace:         DefaultKeyspace,
		Cell:             boosttest.DefaultLocalCell,
		SchemaChangeUser: DefaultSchemaChangeUser,
		Shards:           []string{"-"},
	}

	for _, config := range options {
		config(test)
	}

	if test.SchemaSQL == "" {
		t.Fatalf("missing: test.SchemaSQL")
	}

	test.setupCluster()
	test.setupBoost()
	return test
}

func (test *Test) setupBoost() {
	var err error

	test.Topo, err = topo.OpenServer(*test.Vitess.TopoFlavorString(), test.Vitess.VtctlProcess.TopoGlobalAddress, test.Vitess.VtctlProcess.TopoGlobalRoot)
	if err != nil {
		test.Fatalf("failed to topo.OpenServer(): %v", err)
	}

	test.BoostTopo = client.NewClient(test.Topo)
	test.ClusterUUID = uuid.NewString()

	if test.BoostProcesses != nil {
		for _, boost := range test.BoostProcesses {
			boost := boost

			err = boost.Setup(test.Vitess, test.ClusterUUID)
			if err != nil {
				test.Fatalf("failed to Setup BoostProcess: %v", err)
			}

			test.Cleanup(func() {
				if err := boost.Teardown(); err != nil {
					test.Fatalf("failed to Teardown BoostProcess: %v", err)
				}
				// close test.Topo here because boosttest won't do it
				test.Topo.Close()
			})
		}

		_, err = test.BoostTopo.AddCluster(context.Background(), &vtboost.AddClusterRequest{
			Uuid:                test.ClusterUUID,
			ExpectedWorkerCount: 1,
		})
		if err != nil {
			test.Fatal(err)
		}

		_, err = test.BoostTopo.MakePrimaryCluster(context.Background(), &vtboost.PrimaryClusterRequest{
			Uuid: test.ClusterUUID,
		})
		if err != nil {
			test.Fatal(err)
		}

	} else {
		test.BoostTestCluster = boosttest.New(test,
			boosttest.WithTopoServer(test.Topo),
			boosttest.WithTabletManager(tmclient.NewTabletManagerClient()),
			boosttest.WithShards(0),
			boosttest.WithUpqueryMode(*flagGtidMode),
			boosttest.WithVitessExecutor(),
			boosttest.WithSchemaChangeUser("root"),
			boosttest.WithLocalCell(test.Cell),
			boosttest.WithClusterUUID(test.ClusterUUID),
		)
	}

	if test.Recipe != nil && len(test.Recipe.Queries) > 0 {
		_, err = test.BoostTopo.PutRecipe(context.Background(), &vtboost.PutRecipeRequest{Recipe: test.Recipe})
		if err != nil {
			test.Fatalf("failed to PutRecipe(): %v", err)
		}
		for _, q := range test.Recipe.Queries {
			test.Logf("query loaded: %+v", q)
		}
		time.Sleep(5 * time.Second) // TODO: wait shorter
	}
}

func (test *Test) setupCluster() {
	test.Vitess = cluster.NewCluster(test.Cell, "localhost")
	test.Cleanup(func() {
		test.Vitess.Teardown()
	})

	test.configurePlanetScaleACLs()

	// Start topo server
	err := test.Vitess.StartTopo()
	if err != nil {
		test.Fatalf("failed to StartTopo(): %v", err)
	}

	// Start keyspace
	keyspace := &cluster.Keyspace{
		Name:      test.Keyspace,
		SchemaSQL: test.SchemaSQL,
		VSchema:   test.VSchema,
	}
	test.Vitess.VtGateExtraArgs = []string{"--enable-boost", "--schema_change_signal"}
	test.Vitess.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal", "--queryserver-config-schema-change-signal-interval", "0.1"}
	test.Vitess.VtctldExtraArgs = []string{"--enable-boost"}
	err = test.Vitess.StartKeyspace(*keyspace, test.Shards, 0, false)
	if err != nil {
		test.Fatalf("failed to StartKeyspace(): %v", err)
	}

	test.Vitess.VtGateExtraArgs = append(test.Vitess.VtGateExtraArgs, "--enable_system_settings=true")

	// Start vtgate
	err = test.Vitess.StartVtgate()
	if err != nil {
		test.Fatalf("failed to StartVtgate(): %v", err)
	}

	test.VtParams = &mysql.ConnParams{
		Host: test.Vitess.Hostname,
		Port: test.Vitess.VtgateMySQLPort,
	}

	// create mysql instance and connection parameters
	conn, closer, err := utils.NewMySQL(test.Vitess, test.Keyspace, test.SchemaSQL)
	if err != nil {
		test.Fatalf("failed to create NewMySQL(): %v", err)
	}

	test.Cleanup(closer)
	test.MySQLParams = &conn
}

func (test *Test) Conn() *mysql.Conn {
	conn, err := mysql.Connect(context.Background(), test.VtParams)
	if err != nil {
		test.Fatalf("failed to mysql.Connect(): %v", err)
	}
	test.Cleanup(func() {
		conn.Close()
	})
	return conn
}

func (test *Test) ExecuteFetch(queryfmt string, args ...any) *sqltypes.Result {
	test.Helper()

	if test.vtgateconn == nil {
		test.vtgateconn = test.Conn()
	}

	query := fmt.Sprintf(queryfmt, args...)
	res, err := test.vtgateconn.ExecuteFetch(query, -1, false)
	if err != nil {
		test.Fatalf("failed to ExecuteFetch(%q): %v", query, err)
	}
	return res
}

func (test *Test) ToggleBoost(enable bool) {
	_ = test.ExecuteFetch("SET @@boost_cached_queries = %v", enable)
}

// userData1 is added here next to root since we use the MySQL protocol
// to insert data in the Boost end to end tests and the no-op MySQL
// auth plugin statically returns "userData1" as the username. In real
// PSDB production this value is not present but that doesn't matter
// for the test here.
var vtStaticAclTemplate = template.Must(template.New("vitess_acl").Parse(`
	"table_groups":
	[
	    {
	        "name": "planetscale user groups",
	        "table_names_or_prefixes":
	        [
	            "%"
	        ],
	        "readers":
	        [
	            "planetscale-reader",
	            "planetscale-writer",
	            "planetscale-admin",
	            "{{.Username}}",
				"userData1"
	        ],
	        "writers":
	        [
	            "planetscale-writer",
	            "planetscale-writer-only",
	            "planetscale-admin",
	            "{{.Username}}",
				"userData1"
	        ],
	        "admins":
	        [
	            "planetscale-admin",
	            "{{.Username}}",
				"userData1"
	        ]
	    }
	]
}`))

func (test *Test) configurePlanetScaleACLs() {
	aclPath := path.Join(test.Vitess.CurrentVTDATAROOT, "static-acl.json")
	aclFile, err := os.Create(aclPath)
	if err != nil {
		test.Fatalf("failed to create static ACL file: %v", err)
	}
	defer aclFile.Close()

	err = vtStaticAclTemplate.Execute(aclFile, map[string]any{
		"Username": test.SchemaChangeUser,
	})
	if err != nil {
		test.Fatal(err)
	}

	test.Vitess.VtGateExtraArgs = append(test.Vitess.VtGateExtraArgs, "--schema_change_signal_user", test.SchemaChangeUser, "--grpc_use_effective_callerid")
	test.Vitess.VtTabletExtraArgs = append(test.Vitess.VtTabletExtraArgs, "--table-acl-config ", aclPath, "--enforce-tableacl-config", "--queryserver-config-strict-table-acl")
}
