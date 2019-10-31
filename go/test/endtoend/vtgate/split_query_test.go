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

package vtgate

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql"
)

func TestSplitQuery(t *testing.T) {
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	defer conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	exec(t, conn, "insert into user(id, name) values(1,'john')")

	qr := exec(t, conn, "select id, name from user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("john")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	sql := `"select id, name from user"`
	// It currently hangs on this command.
	result, _ := clusterInstance.VtctlclientProcess.VtGateSplitQuery(KeyspaceName, sql, 2)
	fmt.Println(result)
}
