package startup

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/test/helpers/booste2e"
)

func TestVTBoostStartupAndCleanup(t *testing.T) {
	tt := booste2e.Setup(t, booste2e.WithoutBoost(), booste2e.WithRecipe("basic"))

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	var portHTTP = 16000 + rand.Intn(1000)
	var portDRPC = 15000 + rand.Intn(1000)

	cmd := exec.Command("vtboost",
		"--boost-cluster-uuid", "1eb20327-1c16-4957-8c7a-226ead6c9508",
		"--boost-hostname", "127.0.0.1",
		"--cell", "zone1",
		"--port", strconv.Itoa(portHTTP),
		"--drpc-port", strconv.Itoa(portDRPC),
		"--logtostderr",
		"--topo_implementation", tt.Vitess.VtctlProcess.TopoImplementation,
		"--topo_global_server_address", tt.Vitess.VtctlProcess.TopoGlobalAddress,
		"--topo_global_root", tt.Vitess.VtctlProcess.TopoGlobalRoot,
	)

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Start()
	require.NoError(t, err)

	for i := 0; ; i++ {
		if i == 10 {
			t.Errorf("cannot connect via HTTP to the vtboost server after 5 tries;\nstdout:\n%s\nstderr:%s\n", stdout.Bytes(), stderr.Bytes())
			break
		}

		time.Sleep(time.Duration(i) * time.Second)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/debug/ready/worker", portHTTP))
		if err != nil {
			t.Logf("liveness failed: %v", err)
			continue
		}

		if resp.StatusCode != 200 {
			t.Logf("liveness failed with status code %d", resp.StatusCode)
			continue
		}

		break
	}

	err = cmd.Process.Signal(syscall.SIGTERM)
	require.NoError(t, err)

	var done = make(chan error)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out while waiting for vtboost to quit")
	case err := <-done:
		require.NoError(t, err)
	}
}
