package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"net/http"

	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/boostpb"

	"vitess.io/vitess/go/boost/server"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats/prometheusbackend"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	"vitess.io/vitess/go/vt/vtgate"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

func init() {
	servenv.RegisterDefaultFlags()

	// Enable prometheus reporting for all our metrics
	servenv.OnRun(func() {
		prometheusbackend.Init("vtboost")
	})
}

func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	var (
		hostname    string
		drpcPort    int
		clusterUUID string
		cell        string
		gtidMode    string
		memoryLimit int
	)

	// When deploying in Kubernetes, it's not enough to use the local hostname
	// to identify this Boost instance; we want to inject the pod's IP address
	// here which is not readily available from inside the container to ensure
	// the Boost node is uniquely identified across the cluster.
	//
	// Note that none of these flags have default values because they are
	// expected to be set by psdb-operator. Any multi-word flags should use
	// hyphen separators as is preferred by modern Vitess flag conventions.
	flag.StringVar(&hostname, "boost-hostname", "", "hostname for identifying this Boost instance")
	flag.IntVar(&drpcPort, "drpc-port", 0, "default listen address for DRPC")
	flag.StringVar(&clusterUUID, "boost-cluster-uuid", "", "UUID of the boost cluster, this value is used to find the cluster in the topology.")
	flag.StringVar(&cell, "cell", "", "cell on which the Boost cluster is deployed")
	flag.StringVar(&gtidMode, "gtid-mode", "SELECT_GTID", "GTID tracking mode for upqueries (options are 'SELECT_GTID', 'TRACK_GTID')")
	flag.IntVar(&memoryLimit, "memory-limit", 0, "the memory limit in bytes this process is allowed to allocate: used to detect memory pressure")

	servenv.ParseFlags("vtboost")
	servenv.Init()
	defer servenv.Close()

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

	config := boostpb.DefaultConfig()
	switch gtidMode {
	case "SELECT_GTID":
		config.DomainConfig.UpqueryMode = boostpb.UpqueryMode_SELECT_GTID
	case "TRACK_GTID":
		config.DomainConfig.UpqueryMode = boostpb.UpqueryMode_TRACK_GTID
	default:
		log.Fatal("invalid GTID mode selected", zap.String("gtid_mode", gtidMode))
	}
	boost := server.NewBoostInstance(log, ts, tmclient.NewTabletManagerClient(), config, clusterUUID)

	if !vtgate.EnableSchemaChangeSignal {
		log.Fatal("Schema tracking must be enabled.")
	}

	ctx, cancel := context.WithCancel(context.Background())

	err = boost.ConfigureVitessExecutor(ctx, log, ts, cell, vtgate.SchemaChangeUser)
	if err != nil {
		log.Fatal("failed to configure external gateway", zap.Error(err))
	}

	http.HandleFunc("/debug/ready/controller", func(rw http.ResponseWriter, r *http.Request) {
		if !boost.Controller.IsReady() {
			http.Error(rw, "Controller not ready", 503)
		}
	})
	http.HandleFunc("/debug/ready/worker", func(rw http.ResponseWriter, r *http.Request) {
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
