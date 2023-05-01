package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/boost/server"
	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats/prometheusbackend"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var schemaChangeUser string
var enableSchemaChangeSignal = true

var (
	hostname     string
	drpcPort     int
	clusterUUID  string
	cell         string
	cellsToWatch string
	memoryLimit  int
	cfg          = config.DefaultConfig()

	healthCheckRetryDelay = 2 * time.Millisecond
	healthCheckTimeout    = time.Minute
)

func registerFlags(fs *pflag.FlagSet) {
	// When deploying in Kubernetes, it's not enough to use the local hostname
	// to identify this Boost instance; we want to inject the pod's IP address
	// here which is not readily available from inside the container to ensure
	// the Boost node is uniquely identified across the cluster.
	//
	// Note that none of these flags have default values because they are
	// expected to be set by psdb-operator. Any multi-word flags should use
	// hyphen separators as is preferred by modern Vitess flag conventions.
	fs.StringVar(&hostname, "boost-hostname", "", "hostname for identifying this Boost instance")
	fs.IntVar(&drpcPort, "drpc-port", 0, "default listen address for DRPC")
	fs.StringVar(&clusterUUID, "boost-cluster-uuid", "", "UUID of the boost cluster, this value is used to find the cluster in the topology.")
	fs.StringVar(&cell, "cell", "", "cell on which the Boost cluster is deployed")
	fs.StringVar(&cellsToWatch, "cells_to_watch", "", "comma-separated list of cells for watching tablets")
	fs.IntVar(&memoryLimit, "memory-limit", 0, "the memory limit in bytes this process is allowed to allocate: used to detect memory pressure")

	fs.DurationVar(&healthCheckRetryDelay, "healthcheck_retry_delay", healthCheckRetryDelay, "health check retry delay")
	fs.DurationVar(&healthCheckTimeout, "healthcheck_timeout", healthCheckTimeout, "the health check timeout period")

	// gtid-mode configures how GTID tracking is performed by upqueries; setting this to 'TRACK_GTID' requires a forked
	// version of MySQL for now; the default value is 'SELECT_GTID', which works with any MySQL version even though we've
	// seen it can produce inconsistencies under load
	fs.Var(&cfg.Domain.UpqueryMode, "gtid-mode", "GTID tracking mode for upqueries (options are 'SELECT_GTID', 'TRACK_GTID')")

	// The default worker timeout value is set based on the timeout value for upqueries. This timeout
	// is configured at the vttablet level with --queryserver-config-olap-transaction-timeout and defaults
	// to 30 seconds. We add some additional buffer time here to then process the (potentially huge) results as well.
	fs.DurationVar(&cfg.WorkerReadTimeout, "worker-read-timeout", 40*time.Second, "the timeout for blocking reads on a worker")

	fs.BoolVar(&enableSchemaChangeSignal, "schema_change_signal", enableSchemaChangeSignal, "Enable the schema tracker; requires queryserver-config-schema-change-signal to be enabled on the underlying vttablets for this to work")
	fs.StringVar(&schemaChangeUser, "schema_change_signal_user", schemaChangeUser, "User to be used to send down query to vttablet to retrieve schema changes")

	acl.RegisterFlags(fs)
}

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterFlags()
	servenv.RegisterGRPCServerFlags()
	servenv.RegisterGRPCServerAuthFlags()
	servenv.RegisterServiceMapFlag()
	servenv.OnParseFor("vtboost", registerFlags)

	// Enable prometheus reporting for all our metrics
	servenv.OnRun(func() {
		prometheusbackend.Init("vtboost")
	})
}

func main() {
	servenv.ParseFlags("vtboost")
	servenv.Init()
	defer servenv.Close()

	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	ts := topo.Open()
	defer ts.Close()

	// Kubernetes expects vtboost to show liveness quickly via its drpc listener
	// or else the pod will repeatedly terminate, so open it before configuring
	// the external gateway.
	listener, err := net.Listen("tcp", netutil.JoinHostPort(hostname, int32(drpcPort)))
	if err != nil {
		log.Fatal("failed to start listener", zap.Error(err))
	}
	log.Info("drpc listener bound", zap.String("addr", listener.Addr().String()))

	if memoryLimit > 0 {
		log.Info("vtboost started with memory limit hint", zap.Int("limit", memoryLimit))

		// TODO(mdlayher): look into cgroups memory pressure monitoring. This
		// library seems promising: https://github.com/containerd/cgroups.
	}

	boost := server.NewBoostInstance(log, ts, tmclient.NewTabletManagerClient(), cfg, clusterUUID)

	if !enableSchemaChangeSignal {
		log.Fatal("Schema tracking must be enabled.")
	}

	ctx, cancel := context.WithCancel(context.Background())

	err = boost.ConfigureVitessExecutor(ctx, log, ts, cell, cellsToWatch, schemaChangeUser, healthCheckRetryDelay, healthCheckTimeout)
	if err != nil {
		log.Fatal("failed to configure external gateway", zap.Error(err))
	}

	servenv.HTTPHandleFunc("/debug/ready/controller", func(rw http.ResponseWriter, r *http.Request) {
		if !boost.Controller.IsReady() {
			http.Error(rw, "Controller not ready", 503)
		}
	})
	servenv.HTTPHandleFunc("/debug/ready/worker", func(rw http.ResponseWriter, r *http.Request) {
		if !boost.Worker.IsReady() {
			http.Error(rw, "Worker not ready", 503)
		}
	})

	servenv.OnRun(func() {
		go func() {
			if err := boost.Serve(ctx, listener); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("boost.Serve unclean exit", zap.Error(err))
			}
		}()
	})

	servenv.OnClose(func() {
		cancel()
		listener.Close()
	})

	servenv.RunDefault()
}
