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

package endtoend

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
)

func TestVSchema(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	//Fetch VSchema and verify
	val, _ := cluster.GetVSchema("test")
	if got, want := isValidKeyspace(val), true; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//delete
	cluster.DeleteVSchema("test")
	_, err = cluster.GetVSchema("test")
	want := "node doesn't exist"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Deleted schema: %v, must contain %s", err, want)
	}

	//rebuild
	cluster.RebuildVSchema("test")
	time.Sleep(5 * time.Second)

	//verify again
	val, _ = cluster.GetVSchema("test")
	if got, want := isValidKeyspace(val), true; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

}

func isValidKeyspace(val string) bool {
	resultMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(val), &resultMap)
	if err != nil {
		panic(err)
	}
	key := resultMap["keyspaces"]
	if key != nil {
		return true
	} else {
		return false
	}
}
