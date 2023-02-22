/*
Copyright 2022 The Vitess Authors.

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

package vtexplain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestVtGateVtExplain(t *testing.T) {
	vtParams := clusterInstance.GetVTParams(shardedKs)
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	assertVExplainEquals := func(t *testing.T, conn *mysql.Conn, query, expected string) {
		t.Helper()

		qr := utils.Exec(t, conn, query)

		// strip the first column from each row as it is not deterministic in a VExplain query
		for i := range qr.Rows {
			qr.Rows[i] = qr.Rows[i][1:]
		}

		if err := sqltypes.RowsEqualsStr(expected, qr.Rows); err != nil {
			t.Error(err)
		}
	}

	utils.AssertContainsError(t, conn,
		`explain format=vtexplain insert into user (id,lookup,lookup_unique) values (4,'apa','foo'),(5,'apa','bar'),(6,'monkey','nobar')`,
		"vtexplain will actually run queries")

	expected := `[
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("insert into lookup(lookup, id, keyspace_id) values ('apa', 1, '\x16k@\xb4J\xbaK\xd6') on duplicate key update lookup = values(lookup), id = values(id), keyspace_id = values(keyspace_id)")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("insert into lookup(lookup, id, keyspace_id) values ('monkey', 3, 'N\xb1\x90ɢ\xfa\x16\x9c') on duplicate key update lookup = values(lookup), id = values(id), keyspace_id = values(keyspace_id)")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("insert into lookup_unique(lookup_unique, keyspace_id) values ('monkey', 'N\xb1\x90ɢ\xfa\x16\x9c')")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("insert into lookup_unique(lookup_unique, keyspace_id) values ('apa', '\x16k@\xb4J\xbaK\xd6')")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("insert into ` + "`user`" + `(id, lookup, lookup_unique) values (3, 'monkey', 'monkey')")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("insert into ` + "`user`" + `(id, lookup, lookup_unique) values (1, 'apa', 'apa')")]
	]`
	assertVExplainEquals(t, conn, `explain /*vt+ EXECUTE_DML_QUERIES */ format=vtexplain insert into user (id,lookup,lookup_unique) values (1,'apa','apa'),(3,'monkey','monkey')`, expected)

	expected = `[[INT32(0) VARCHAR("ks") VARCHAR("-40") VARCHAR("select lookup, keyspace_id from lookup where lookup in ('apa')")]` +
		` [INT32(1) VARCHAR("ks") VARCHAR("-40") VARCHAR("select id from ` + "`user`" + ` where lookup = 'apa'")]]`
	for _, mode := range []string{"oltp", "olap"} {
		t.Run(mode, func(t *testing.T) {
			utils.Exec(t, conn, "set workload = "+mode)
			utils.AssertMatches(t, conn, `explain format=vtexplain select id from user where lookup = "apa"`, expected)
		})
	}

	// transaction explicitly started to no commit in the end.
	utils.Exec(t, conn, "begin")
	expected = `[
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("insert into lookup(lookup, id, keyspace_id) values ('apa', 4, '\xd2\xfd\x88g\xd5\\r-\xfe'), ('apa', 5, 'p\xbb\x02<\x81\f\xa8z') on duplicate key update lookup = values(lookup), id = values(id), keyspace_id = values(keyspace_id)")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("insert into lookup(lookup, id, keyspace_id) values ('monkey', 6, '\xf0\x98H\\n\xc4ľq') on duplicate key update lookup = values(lookup), id = values(id), keyspace_id = values(keyspace_id)")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("insert into lookup_unique(lookup_unique, keyspace_id) values ('foo', '\xd2\xfd\x88g\xd5\\r-\xfe')")]
		[VARCHAR("ks") VARCHAR("80-c0") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("80-c0") VARCHAR("insert into lookup_unique(lookup_unique, keyspace_id) values ('bar', 'p\xbb\x02<\x81\f\xa8z')")]
		[VARCHAR("ks") VARCHAR("c0-") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("c0-") VARCHAR("insert into lookup_unique(lookup_unique, keyspace_id) values ('nobar', '\xf0\x98H\\n\xc4ľq')")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("80-c0") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("c0-") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("insert into ` + "`user`" + `(id, lookup, lookup_unique) values (5, 'apa', 'bar')")]
		[VARCHAR("ks") VARCHAR("c0-") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("c0-") VARCHAR("insert into ` + "`user`" + `(id, lookup, lookup_unique) values (4, 'apa', 'foo'), (6, 'monkey', 'nobar')")]
	]`
	assertVExplainEquals(t, conn, `explain /*vt+ EXECUTE_DML_QUERIES */ format=vtexplain insert into user (id,lookup,lookup_unique) values (4,'apa','foo'),(5,'apa','bar'),(6,'monkey','nobar')`, expected)

	utils.Exec(t, conn, "rollback")
}
