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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

func TestUsers(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn1, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn2, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	assert := assert.New(t)

	//Test insert
	for i := 1; i <= 4; i++ {
		each := strconv.Itoa(i)
		nameValue := "test " + each
		exec(t, conn, "insert into vt_user(id,name) values ("+each+",'"+nameValue+"')")
		qr := exec(t, conn, "select id,name from vt_user where id = "+strconv.Itoa(i))
		got := fmt.Sprintf("%v", qr.Rows)
		want := "[[INT64(" + each + ") VARCHAR(\"" + nameValue + "\")]]"
		assert.Equal(want, got)
	}

	//Test case sensitivity
	qr := exec(t, conn, "select Id, Name from vt_user where iD = 1")
	got := fmt.Sprintf("%v", qr.Rows)
	want := `[[INT64(1) VARCHAR("test 1")]]`
	assert.Equal(want, got)

	//Test directive timeout
	_, err = conn.ExecuteFetch("SELECT /*vt+ QUERY_TIMEOUT_MS=10 */ SLEEP(1)", 1000, false)
	want = "DeadlineExceeded"
	if assert.NotNil(err) {
		assert.Contains(err.Error(), want)
	}

	//Test directive timeout longer than the query time
	qr = exec(t, conn, "SELECT /*vt+ QUERY_TIMEOUT_MS=2000 */ SLEEP(1)")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(0)]]`
	assert.Equal(want, got)

	//Verify values in db
	exec(t, conn, "insert into vt_user(id,name) values(6,'test 6')")
	exec(t, conn1, "use `ks:-80`")
	qr = exec(t, conn1, "select id, name from vt_user")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(1) VARCHAR("test 1")] [INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")]]`
	assert.Equal(want, got)

	exec(t, conn2, "use `ks:80-`")
	qr = exec(t, conn2, "select id, name from vt_user")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(4) VARCHAR("test 4")] [INT64(6) VARCHAR("test 6")]]`
	assert.Equal(want, got)

	//Test IN clause
	qr = exec(t, conn, "select id, name from vt_user where id in (1, 4) order by id")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(1) VARCHAR("test 1")] [INT64(4) VARCHAR("test 4")]]`
	assert.Equal(want, got)

	//Test scatter
	qr = exec(t, conn, "select id, name from vt_user order by id")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(1) VARCHAR("test 1")] [INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")] [INT64(4) VARCHAR("test 4")] [INT64(6) VARCHAR("test 6")]]`
	assert.Equal(want, got)

	//Test updates
	exec(t, conn, "update vt_user set name = 'test one' where id = 1")
	exec(t, conn, "update vt_user set name = 'test four' where id = 4")
	qr = exec(t, conn1, "select id, name from vt_user")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(1) VARCHAR("test one")] [INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")]]`
	assert.Equal(want, got)

	qr = exec(t, conn2, "select id, name from vt_user")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(4) VARCHAR("test four")] [INT64(6) VARCHAR("test 6")]]`
	assert.Equal(want, got)

	//Test deletes
	exec(t, conn, "delete from vt_user where id = 1")
	exec(t, conn, "delete from vt_user where id = 4")
	qr = exec(t, conn1, "select id, name from vt_user")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")]]`
	assert.Equal(want, got)

	qr = exec(t, conn2, "select id, name from vt_user")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(6) VARCHAR("test 6")]]`
	assert.Equal(want, got)

	//Test scatter update
	exec(t, conn, "insert into vt_user (id, name) values (22,'name2'),(33,'name2')")
	qr = exec(t, conn, "select id, name from vt_user order by id")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")] [INT64(6) VARCHAR("test 6")] [INT64(22) VARCHAR("name2")] [INT64(33) VARCHAR("name2")]]`
	assert.Equal(want, got)

	exec(t, conn, "update vt_user set name='jose' where id > 20")
	qr = exec(t, conn, "select id, name from vt_user order by id")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")] [INT64(6) VARCHAR("test 6")] [INT64(22) VARCHAR("jose")] [INT64(33) VARCHAR("jose")]]`
	assert.Equal(want, got)

	//Test scatter delete
	exec(t, conn, "delete from vt_user where id > 20")
	qr = exec(t, conn, "select id, name from vt_user order by id")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")] [INT64(6) VARCHAR("test 6")]]`
	assert.Equal(want, got)

	//Test shard errors as warnings directive
	_, warnings, err := conn.ExecuteFetchWithWarningCount("SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS */ bad from vt_user", 1000, false)
	got = strconv.Itoa(int(warnings))
	want = strconv.Itoa(2)
	assert.Equal(want, got)

	//Test shard errors as warnings directive with timeout
	_, warnings, err = conn.ExecuteFetchWithWarningCount("SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS QUERY_TIMEOUT_MS=10 */ SLEEP(1)", 1000, false)
	got = strconv.Itoa(int(warnings))
	want = strconv.Itoa(1)
	assert.Equal(want, got)

	//deleting all records from vt_user table
	exec(t, conn, "delete from vt_user where id > 0")
}

func TestQueryStream(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	//Test insert
	for i := 1; i <= 4; i++ {
		each := strconv.Itoa(i)
		nameValue := "test " + each
		exec(t, conn, "insert into vt_user(id,name) values ("+each+",'"+nameValue+"')")
		qr := exec(t, conn, "select id,name from vt_user where id = "+strconv.Itoa(i))
		got := fmt.Sprintf("%v", qr.Rows)
		want := "[[INT64(" + each + ") VARCHAR(\"" + nameValue + "\")]]"
		assert.Equal(t, want, got)
	}

	//Test stream over scatter
	query := "select id, name from vt_user order by id"
	if err := conn.ExecuteStreamFetch(query); err != nil {
		t.Fatalf("ExecuteStreamFetch(%v) failed: %v", query, err)
	}
	qr := &sqltypes.Result{}
	qr.Fields, err = conn.Fields()
	if err != nil {
		t.Fatalf("Fields(%v) failed: %v", query, err)
	}
	if len(qr.Fields) == 0 {
		qr.Fields = nil
	}
	for {
		row, err := conn.FetchNext()
		if err != nil {
			t.Fatalf("FetchNext(%v) failed: %v", query, err)
		}
		if row == nil {
			// Done.
			break
		}
		qr.Rows = append(qr.Rows, row)
	}
	conn.CloseResult()
	got := fmt.Sprintf("%v", qr.Rows)
	want := `[[INT64(1) VARCHAR("test 1")] [INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")] [INT64(4) VARCHAR("test 4")]]`
	assert.Equal(t, want, got)

	//deleting all records from vt_user table
	exec(t, conn, "delete from vt_user where id > 0")
}
