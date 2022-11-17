package booste2e

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"syscall"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

type BoostProcess struct {
	PortHTTP  int
	PortDRPC  int
	ExtraArgs []string

	proc *exec.Cmd
	exit chan error
}

func (boost *BoostProcess) Setup(cluster *cluster.LocalProcessCluster, clusterUUID string) error {
	if boost.PortHTTP == 0 {
		boost.PortHTTP = cluster.GetAndReservePort()
	}
	if boost.PortDRPC == 0 {
		boost.PortDRPC = cluster.GetAndReservePort()
	}

	args := []string{
		"--boost-cluster-uuid", clusterUUID,
		"--boost-hostname", "127.0.0.1",
		"--cell", cluster.Cell,
		"--port", strconv.Itoa(boost.PortHTTP),
		"--drpc-port", strconv.Itoa(boost.PortDRPC),
		"--logtostderr",
		"--topo_implementation", cluster.VtctlProcess.TopoImplementation,
		"--topo_global_server_address", cluster.VtctlProcess.TopoGlobalAddress,
		"--topo_global_root", cluster.VtctlProcess.TopoGlobalRoot,
	}
	args = append(args, boost.ExtraArgs...)

	boost.proc = exec.Command("vtboost", args...)
	boost.proc.Stdout, _ = os.Create(path.Join(cluster.TmpDirectory, "vtboost-stdout.txt"))
	boost.proc.Stderr, _ = os.Create(path.Join(cluster.TmpDirectory, "vtboost-stderr.txt"))
	boost.proc.Env = append(boost.proc.Env, os.Environ()...)

	if err := boost.proc.Start(); err != nil {
		return err
	}

	boost.exit = make(chan error)
	go func() {
		boost.exit <- boost.proc.Wait()
		close(boost.exit)
	}()

	return nil
}

func (boost *BoostProcess) Teardown() error {
	if err := boost.proc.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}

	select {
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out while waiting for vtboost to quit")
	case <-boost.exit:
		boost.proc = nil
		return nil
	}
}
