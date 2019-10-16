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
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	//vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestTransactionModes(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	gconn, err := vtgateconn.Dial(ctx, grpcAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer gconn.Close()
	fmt.Println("******")

	// cfg.TransactionMode = vtgatepb.TransactionMode_SINGLE
	//extra_args=['-transaction_mode', 'TWOPC'])
	// fmt.Println(cluster.Config.TransactionMode)

	// if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(3)]]`; got != want {
	// 	t.Errorf("select:\n%v want\n%v", got, want)
	// }

	//Insert trageted to multiple tables should fail with SINGLE trx mode
	cluster.Config.TransactionMode = "SINGLE"
	qr := exec(t, conn, "set transaction_mode='single'")

	fmt.Println(qr)
	exec(t, conn, "begin")
	exec(t, conn, "insert into twopc_user(user_id, name) values(1,'john')")
	exec(t, conn, "insert into twopc_lookup(name, id) values('paul',2)")
	exec(t, conn, "commit")
	_, err = conn.ExecuteFetch("commit", 1000, false)
	want := "multi-db transaction attempted"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("multi-db insert: %v, must contain %s", err, want)
	}

	//Insert trageted to multiple tables should PASS with TWOPC trx mode
	cluster.Config.TransactionMode = "TWOPC"
	qr = exec(t, conn, "set transaction_mode='twopc'")
	exec(t, conn, "begin")
	exec(t, conn, "insert into twopc_user(user_id, name) values(3,'mark')")
	exec(t, conn, "insert into twopc_lookup(name, id) values('doug',4)")
	exec(t, conn, "commit")

	//Verify the values are present
	qr = exec(t, conn, "select user_id from twopc_user where name='mark'")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(3)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select name from twopc_lookup where id=3")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("mark")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//DELETE from multiple tables using TWOPC trnx mode
	exec(t, conn, "begin")
	exec(t, conn, "delete from twopc_user where user_id = 3")
	exec(t, conn, "delete from twopc_lookup where id = 3")
	exec(t, conn, "commit")

	//VERIFY that values are deleted
	qr = exec(t, conn, "select user_id from twopc_user where user_id=3")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select name from twopc_lookup where id=3")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}
