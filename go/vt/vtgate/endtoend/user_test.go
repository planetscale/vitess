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
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql"
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

	//Test insert
	for i := 1; i <= 4; i++ {
		each := strconv.Itoa(i)
		nameQ := "test " + each
		exec(t, conn, "insert into vt_user(id,name) values ("+each+",'"+nameQ+"')")
		qr := exec(t, conn, "select id,name from vt_user where id = "+strconv.Itoa(i))
		if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64("+each+") VARCHAR(\""+nameQ+"\")]]"; got != want {
			t.Errorf("select:\n%v want\n%v", got, want)
		}
	}

	//Test select equal
	for i := 1; i <= 4; i++ {
		each := strconv.Itoa(i)
		nameQ := "test " + each
		qr := exec(t, conn, "select id,name from vt_user where id = "+each)
		if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64("+each+") VARCHAR(\""+nameQ+"\")]]"; got != want {
			t.Errorf("select:\n%v want\n%v", got, want)
		}
	}

	//Test case sensitivity
	qr := exec(t, conn, "select Id, Name from vt_user where iD = 1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("test 1")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test directive timeout
	_, err = conn.ExecuteFetch("SELECT /*vt+ QUERY_TIMEOUT_MS=10 */ SLEEP(1)", 1000, false)
	want := "DeadlineExceeded"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("second insert: %v, must contain %s", err, want)
	}

	//Test directive timeout longer than the query time
	qr = exec(t, conn, "SELECT /*vt+ QUERY_TIMEOUT_MS=2000 */ SLEEP(1)")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(0)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test shard errors as warnings directive
	//_, warningCount, err := conn.ExecuteFetchWithWarningCount("SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS QUERY_TIMEOUT_MS=10 */ SLEEP(1)", 1000, false)

	//Test insert with no auto-inc
	//move it to sequences

	//Verify values in db
	exec(t, conn, "insert into vt_user(id,name) values(6,'test 6')")
	exec(t, conn1, "use `ks:-80`")
	qr = exec(t, conn1, "select id, name from vt_user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("test 1")] [INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	exec(t, conn2, "use `ks:80-`")
	qr = exec(t, conn2, "select id, name from vt_user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(4) VARCHAR("test 4")] [INT64(6) VARCHAR("test 6")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test IN clause
	qr = exec(t, conn, "select id, name from vt_user where id in (1, 4) order by id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("test 1")] [INT64(4) VARCHAR("test 4")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test scatter
	qr = exec(t, conn, "select id, name from vt_user order by id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("test 1")] [INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")] [INT64(4) VARCHAR("test 4")] [INT64(6) VARCHAR("test 6")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test updates
	exec(t, conn, "update vt_user set name = 'test one' where id = 1")
	exec(t, conn, "update vt_user set name = 'test four' where id = 4")
	qr = exec(t, conn1, "select id, name from vt_user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("test one")] [INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn2, "select id, name from vt_user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(4) VARCHAR("test four")] [INT64(6) VARCHAR("test 6")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test deletes
	exec(t, conn, "delete from vt_user where id = 1")
	exec(t, conn, "delete from vt_user where id = 4")
	qr = exec(t, conn1, "select id, name from vt_user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn2, "select id, name from vt_user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(6) VARCHAR("test 6")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test scatter delete
	exec(t, conn, "insert into vt_user (id, name) values (22,'name2'),(33,'name2')")
	qr = exec(t, conn, "select id, name from vt_user order by id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")] [INT64(6) VARCHAR("test 6")] [INT64(22) VARCHAR("name2")] [INT64(33) VARCHAR("name2")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	exec(t, conn, "delete from vt_user where id > 20")
	qr = exec(t, conn, "select id, name from vt_user order by id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")] [INT64(6) VARCHAR("test 6")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test scatter update
	exec(t, conn, "insert into vt_user (id, name) values (22,'name2'),(33,'name2')")
	qr = exec(t, conn, "select id, name from vt_user order by id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")] [INT64(6) VARCHAR("test 6")] [INT64(22) VARCHAR("name2")] [INT64(33) VARCHAR("name2")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	exec(t, conn, "update vt_user set name='jose' where id > 20")
	qr = exec(t, conn, "select id, name from vt_user order by id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(2) VARCHAR("test 2")] [INT64(3) VARCHAR("test 3")] [INT64(6) VARCHAR("test 6")] [INT64(22) VARCHAR("jose")] [INT64(33) VARCHAR("jose")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test shard errors as warnings directive
	_, warnings, err := conn.ExecuteFetchWithWarningCount("SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS */ bad from vt_user", 1000, false)
	if got, want := warnings, uint16(2); got != want {
		t.Errorf("select:\n%d want\n%d", got, want)
	}

	//Test shard errors as warnings directive with timeout
	_, warnings, err = conn.ExecuteFetchWithWarningCount("SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS QUERY_TIMEOUT_MS=10 */ SLEEP(1)", 1000, false)
	if got, want := warnings, uint16(1); got != want {
		t.Errorf("select:\n%d want\n%d", got, want)
	}
}
