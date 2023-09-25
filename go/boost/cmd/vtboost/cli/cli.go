package cli

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/boost/server"
	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats/prometheusbackend"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	hostname    string
	drpcPort    int
	clusterUUID string
	cell        string
	memoryLimit int
	cfg         = config.DefaultConfig()

	ts  *topo.Server
	log *zap.Logger

	Main = &cobra.Command{
		Use:   "vtboost",
		Short: "",
		Long:  "",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) (err error) {
			if err = servenv.CobraPreRunE(cmd, args); err != nil {
				return err
			}

			servenv.Init()

			if log, err = zap.NewProduction(); err != nil {
				return err
			}

			ts = topo.Open()

			return nil
		},
		RunE: run,
		PostRun: func(cmd *cobra.Command, args []string) {
			ts.Close()
			servenv.Close()
		},
	}
)

func Log() *zap.Logger {
	if log == nil {
		panic("logger not initialized")
	}

	return log
}

func run(cmd *cobra.Command, args []string) error {
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

	ctx, cancel := context.WithCancel(context.Background())

	err = boost.ConfigureVitessExecutor(ctx, log, ts, cell, nil)
	if err != nil {
		log.Fatal("failed to configure external gateway", zap.Error(err))
	}

	servenv.HTTPHandleFunc("/debug/ready/controller", func(rw http.ResponseWriter, r *http.Request) {
		if !boost.Controller.IsReady() {
			http.Error(rw, "Controller not ready", http.StatusServiceUnavailable)
		}
	})
	servenv.HTTPHandleFunc("/debug/ready/worker", func(rw http.ResponseWriter, r *http.Request) {
		if !boost.Worker.IsReady() {
			http.Error(rw, "Worker not ready", http.StatusServiceUnavailable)
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
	return nil
}

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterFlags()
	servenv.RegisterGRPCServerFlags()
	servenv.RegisterGRPCServerAuthFlags()
	servenv.RegisterServiceMapFlag()

	servenv.MoveFlagsToCobraCommand(Main)

	// When deploying in Kubernetes, it's not enough to use the local hostname
	// to identify this Boost instance; we want to inject the pod's IP address
	// here which is not readily available from inside the container to ensure
	// the Boost node is uniquely identified across the cluster.
	//
	// Note that none of these flags have default values because they are
	// expected to be set by psdb-operator. Any multi-word flags should use
	// hyphen separators as is preferred by modern Vitess flag conventions.
	Main.Flags().StringVar(&hostname, "boost-hostname", hostname, "hostname for identifying this Boost instance")
	Main.Flags().IntVar(&drpcPort, "drpc-port", drpcPort, "default listen address for DRPC")
	Main.Flags().StringVar(&clusterUUID, "boost-cluster-uuid", clusterUUID, "UUID of the boost cluster, this value is used to find the cluster in the topology.")
	Main.Flags().StringVar(&cell, "cell", cell, "cell on which the Boost cluster is deployed")
	Main.Flags().IntVar(&memoryLimit, "memory-limit", memoryLimit, "the memory limit in bytes this process is allowed to allocate: used to detect memory pressure")

	// gtid-mode configures how GTID tracking is performed by upqueries; setting this to 'TRACK_GTID' requires a forked
	// version of MySQL for now; the default value is 'SELECT_GTID', which works with any MySQL version even though we've
	// seen it can produce inconsistencies under load
	Main.Flags().Var(&cfg.Domain.UpqueryMode, "gtid-mode", "GTID tracking mode for upqueries (options are 'SELECT_GTID', 'TRACK_GTID')")

	// The default worker timeout value is set based on the timeout value for upqueries. This timeout
	// is configured at the vttablet level with --queryserver-config-olap-transaction-timeout and defaults
	// to 30 seconds. We add some additional buffer time here to then process the (potentially huge) results as well.
	Main.Flags().DurationVar(&cfg.WorkerReadTimeout, "worker-read-timeout", cfg.WorkerReadTimeout, "the timeout for blocking reads on a worker")

	// The default vstream start timeout value defines how long we wait for a vstream to start before we
	// fail and retry the vstream startup process.
	Main.Flags().DurationVar(&cfg.VstreamStartTimeout, "vstream-start-timeout", cfg.VstreamStartTimeout, "the timeout for waiting for vstreams to start")

	// The number of timed we retry a vstream startup if it fails.
	Main.Flags().IntVar(&cfg.VstreamStartRetries, "vstream-start-retries", cfg.VstreamStartRetries, "number of retries for vstream startup if it fails")

	acl.RegisterFlags(Main.Flags())

	// Enable prometheus reporting for all our metrics
	servenv.OnRun(func() {
		prometheusbackend.Init("vtboost")
	})
}
