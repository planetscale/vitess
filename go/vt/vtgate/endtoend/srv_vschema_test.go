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
	"reflect"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql"
)

func TestSrvVSchema(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	//hardcoded cell name from vtcombo cluster.
	cell := "test"

	//Fetch VSchema and verify if it has a Keyspace
	if vSchema, err := cluster.GetSrvVSchema(cell); err != nil {
		t.Errorf("No srv vschema present: %v", err)
	} else {
		if got, want := hasValidKeyspace(vSchema), true; got != want {
			t.Errorf("select:\n%v want\n%v", got, want)
		}
	}

	//Delete the current VSchema, it should return an error if we try to refer it.
	cluster.DeleteSrvVSchema(cell)
	_, err = cluster.GetSrvVSchema(cell)
	want := "node doesn't exist"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Deleted schema: %v, must contain %s", err, want)
	}

	//Rebuild the VSchema in the cluster
	cluster.RebuildSrvVSchema(cell)

	//Verify that Schema was build and has valid Keyspace
	if vSchema, err := cluster.GetSrvVSchema(cell); err != nil {
		t.Errorf("No srv vschema present: %v", err)
	} else {
		if got, want := hasValidKeyspace(vSchema), true; got != want {
			t.Errorf("select:\n%v want\n%v", got, want)
		}
	}

}

//Function to check if the provided vschema has valid keyspace.
func hasValidKeyspace(vSchema string) bool {
	resultMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(vSchema), &resultMap)
	if err != nil {
		panic(err)
	}

	//Length of VSchema should be 2 (1 for keyspaces and 1 for routing_rules)
	len := len(resultMap)
	if len != 2 {
		return false
	}

	keyspaces := resultMap["keyspaces"]
	keyspace := keyspaces.(map[string]interface{})
	// Keyspace name used for setting local cluster
	keyspaceName := "ks"
	validKeyspace := false

	v := reflect.ValueOf(keyspace)
	if v.Kind() == reflect.Map {
		for _, key := range v.MapKeys() {
			if keyspaceName == key.Interface() {
				validKeyspace = true
				break
			}
		}
	}

	return validKeyspace
}
