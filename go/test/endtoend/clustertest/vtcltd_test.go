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

package clustertest

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/smartystreets/assertions"
	vtctl "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
)

var (
	oneTableOutput = `
+---+
| a |
+---+
| 1 |
+---+
`
)

func TestVtctldProcess(t *testing.T) {
	url := fmt.Sprintf("http://%s:%d/api/keyspaces/", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)
	testURL(t, url, "keyspace url")

	healthCheckURL := fmt.Sprintf("http://%s:%d/debug/health/", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)
	testURL(t, healthCheckURL, "vtctld health check url")

	url = fmt.Sprintf("http://%s:%d/api/topodata/", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)

	testTopoDataAPI(url)
	testListAllTablets()
	testTabletStatus()
	testExecuteAsDba()
	testExecuteAsApp()
	testListAllTabletsUsingVtctlGrpc()
}

func testTopoDataAPI(url string) {
	resp, err := http.Get(url)
	assertions.ShouldBeNil(err)
	assertions.ShouldEqual(resp.StatusCode, 200)

	resultMap := make(map[string]interface{})
	respByte, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(respByte, &resultMap)
	assertions.ShouldBeNil(err)

	errorValue := reflect.ValueOf(resultMap["Error"])
	assertions.ShouldBeEmpty(errorValue.String())

	assertions.ShouldContainKey(resultMap, "Children")
	children := reflect.ValueOf(resultMap["Children"])
	assertions.ShouldContain(children, "global")
	assertions.ShouldContain(children, clusterInstance.Cell)
}

func testListAllTablets() {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ListAllTablets", clusterInstance.Cell)
	assertions.ShouldBeNil(err)

	tablets := getAllTablets()

	tabletsFromCMD := strings.Split(result, "\n")
	fmt.Println("len from cmd: " + fmt.Sprintf("%d", len(tabletsFromCMD)))
	fmt.Println("len from tab: " + fmt.Sprintf("%d", len(tabletsFromCMD)))

	assertions.ShouldEqual(len(tabletsFromCMD), len(tablets))

	for _, line := range tabletsFromCMD {
		assertions.ShouldContain(tablets, strings.Split(line, " ")[0])
	}
}

func testTabletStatus() {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d", clusterInstance.Hostname, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].HTTPPort))
	assertions.ShouldBeNil(err)
	respByte, err := ioutil.ReadAll(resp.Body)
	assertions.ShouldBeNil(err)
	result := string(respByte)
	assertions.ShouldBeTrue(strings.Contains(result, `Polling health information from. MySQLReplicationLag`))
	assertions.ShouldBeTrue(strings.Contains(result, `Alias: <a href="http://localhost:`))
	assertions.ShouldBeTrue(strings.Contains(result, `</html>`))
}

func testExecuteAsDba() {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDba", clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].Alias, `SELECT 1 AS a`)
	assertions.ShouldBeNil(err)
	assertions.ShouldEqual(result, oneTableOutput)
}

func testExecuteAsApp() {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ExecuteFetchAsApp", clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].Alias, `SELECT 1 AS a`)
	assertions.ShouldBeNil(err)
	assertions.ShouldEqual(result, oneTableOutput)
}

func testListAllTabletsUsingVtctlGrpc() {
	client, err := vtctl.NewClient(clusterInstance.VtctlclientProcess.Server)
	assertions.ShouldBeNil(err)

	ctx := context.Background()
	stream, err := client.ExecuteVtctlCommand(ctx, []string{"ListAllTablets", clusterInstance.Cell}, 1*time.Second)
	assertions.ShouldBeNil(err)

	tablets := getAllTablets()
	for range tablets {
		result, err := stream.Recv()
		assertions.ShouldNotBeNil(result)
		assertions.ShouldBeNil(err)
		assertions.ShouldContain(tablets, strings.Split(result.String(), " ")[0])
	}
	client.Close()
}

func getAllTablets() []string {
	tablets := make([]string, 0)
	for _, ks := range clusterInstance.Keyspaces {
		for _, shard := range ks.Shards {
			for _, tablet := range shard.Vttablets {
				tablets = append(tablets, tablet.Alias)
			}
		}
	}
	return tablets
}
