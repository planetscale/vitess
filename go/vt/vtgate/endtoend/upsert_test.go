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
)

func TestUpsert(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Create lookup entries for primary vindex:
	// No entry for 2. upsert_primary is not owned.
	// So, we need to pre-create entries that the
	// subsequent will insert will use to compute the
	// keyspace id.
	exec(t, conn, "begin")
	exec(t, conn, "insert into upsert_primary(pk, ksnum_id) values(1, 1), (3, 3), (4, 4), (5, 5), (6, 6)")
	exec(t, conn, "commit")

	// Create rows on the main table.
	exec(t, conn, "insert into upsert(pk, owned, user_id, col) values(1, 1, 1, 0), (3, 3, 3, 0), (4, 4, 4, 0), (5, 5, 5, 0), (6, 6, 6, 0)")

	// Now upsert: 1, 5 and 6 should succeed.
	exec(t, conn, "insert into upsert(pk, owned, user_id, col) values(1, 1, 1, 1), (2, 2, 2, 2), (3, 1, 1, 3), (4, 4, 1, 4), (5, 5, 5, 5), (6, 6, 6, 6) on duplicate key update col = values(col)")

	qr := exec(t, conn, "select pk, owned, user_id, col from upsert order by pk")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(1) INT64(1) INT64(1)] [INT64(3) INT64(3) INT64(3) INT64(0)] [INT64(4) INT64(4) INT64(4) INT64(0)] [INT64(5) INT64(5) INT64(5) INT64(5)] [INT64(6) INT64(6) INT64(6) INT64(6)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// insert ignore
	exec(t, conn, "insert into upsert_primary(pk, ksnum_id) values(7, 7)")
	// 1 will be sent but will not change existing row.
	// 2 will not be sent because there is no keyspace id for it.
	// 7 will be sent and will create a row.
	exec(t, conn, "insert ignore into upsert(pk, owned, user_id, col) values (1, 1, 1, 2), (2, 2, 2, 2), (7, 7, 7, 7)")

	qr = exec(t, conn, "select pk, owned, user_id, col from upsert order by pk")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(1) INT64(1) INT64(1)] [INT64(3) INT64(3) INT64(3) INT64(0)] [INT64(4) INT64(4) INT64(4) INT64(0)] [INT64(5) INT64(5) INT64(5) INT64(5)] [INT64(6) INT64(6) INT64(6) INT64(6)] [INT64(7) INT64(7) INT64(7) INT64(7)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

}
