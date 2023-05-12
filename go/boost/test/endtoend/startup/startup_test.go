package startup

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"vitess.io/vitess/go/boost/test/helpers/booste2e"
)

func TestVTBoostStartupAndCleanup(t *testing.T) {
	tt := booste2e.Setup(t, booste2e.WithBoostInstance(), booste2e.WithRecipe("votes"))

	for i := 0; ; i++ {
		if i == 10 {
			t.Errorf("cannot connect via HTTP to the vtboost server after 5 tries")
			break
		}

		time.Sleep(time.Duration(i) * time.Second)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/debug/ready/worker", tt.BoostProcesses[0].PortHTTP))
		if err != nil {
			t.Logf("liveness failed: %v", err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Logf("liveness failed with status code %d", resp.StatusCode)
			continue
		}

		break
	}
}
