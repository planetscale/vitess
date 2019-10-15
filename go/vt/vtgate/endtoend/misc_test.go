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
	"testing"

	"vitess.io/vitess/go/mysql"
	//vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestTruncateTable(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	//Truncate all the data in table
	exec(t, conn, "truncate table user")

	// Verify table is empty
	qr := exec(t, conn, "select * from user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Insert few records in the table
	exec(t, conn, "begin")
	exec(t, conn, "insert into user(id, name) values(1,'john'), (2, 'mark')")
	exec(t, conn, "commit")

	// Verify records are present in the table
	qr = exec(t, conn, "select * from user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("john")] [INT64(2) VARCHAR("mark")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Truncate all the data and verify table is empty
	exec(t, conn, "truncate user")
	qr = exec(t, conn, "select * from user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func TestScatterLimit(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	//Insert few records in the table
	exec(t, conn, "begin")
	exec(t, conn, "insert into user(id, name) values(1,'john'), (2, 'mark'), (3, 'paul'), (4, 'doug')")
	exec(t, conn, "commit")

	// Verify LIMIT works
	qr := exec(t, conn, "select id from user order by id limit 2")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1)] [INT64(2)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Verify LIMIT with OFFSET works
	qr = exec(t, conn, "select id from user order by id limit 1 offset 2")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(3)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Verify when limit is greater than values in the table
	qr = exec(t, conn, "select id from user order by id limit 100 offset 1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(2)] [INT64(3)] [INT64(4)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

}

func TestVindexFunc(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Test Vindex Function
	qr := exec(t, conn, "select id, keyspace_id from hash where id = 1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARBINARY("1") VARBINARY("\x16k@\xb4J\xbaK\xd6")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func TestAnalyzeTable(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Test Analyze Table
	qr := exec(t, conn, "analyze table t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("vt_ks_-80.t1") VARCHAR("analyze") VARCHAR("status") TEXT("OK")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Test Analyze Table 2
	qr = exec(t, conn, "analyze table t4_music_lookup")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("vt_ks_-80.t4_music_lookup") VARCHAR("analyze") VARCHAR("status") TEXT("OK")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func TestTransactionModes(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// cfg.TransactionMode = vtgatepb.TransactionMode_SINGLE
	//extra_args=['-transaction_mode', 'TWOPC'])
	// fmt.Println(cluster.Config.TransactionMode)

	//Insert trageted to multiple tables should fail with SINGLE trx mode
	cluster.Config.TransactionMode = "SINGLE"
	exec(t, conn, "begin")
	exec(t, conn, "insert into twopc_user(user_id, name) values(1,'john')")
	exec(t, conn, "insert into twopc_lookup(name, id) values('paul',2)")
	exec(t, conn, "commit")

	//Insert trageted to multiple tables should PASS with TWOPC trx mode
	cluster.Config.TransactionMode = "TWOPC"
	exec(t, conn, "begin")
	exec(t, conn, "insert into twopc_user(user_id, name) values(3,'mark')")
	exec(t, conn, "insert into twopc_lookup(name, id) values('doug',4)")
	exec(t, conn, "commit")

	//Verify the values are present
	qr := exec(t, conn, "select user_id from twopc_user where name='mark'")
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
