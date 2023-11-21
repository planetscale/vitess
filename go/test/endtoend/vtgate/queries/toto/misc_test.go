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

package toto

import (
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		tables := []string{"foods"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}

const (
	query2 = `select
    x2.id,
    x3.id
from
    foods x2
    left outer join foods x3 on (x2.original_food_id = x3.original_food_id)
    and (x3.version > x2.version)
where x2.deleted = true
  and x3.id is null
  and (x2.updated_at = '2022-09-15T10:57:43Z' and x2.id >= -9223372036854775808 or x2.updated_at > '2022-09-15T10:57:43Z')
  and x2.is_public = false
  and x2.food_type in (0, 1, 2)
order by
    x2.updated_at,
    x2.id
limit
    1000;
`




	query = `select
    x2.id,
    x2.user_id,
    x2.description,
    x2.food_type,
    x2.deleted,
    x2.created_at,
    x2.updated_at,
    x2.brand,
    x2.is_public,
    x2.country_id,
    x2.country_code,
    x2.brand_as_url,
    x2.version,
    x2.original_food_id,
    x2.brand_name_id,
    x2.edit_notes,
    x2.verified,
    x2.external_id,
    x2.external_version,
    x2.rank,
    x2.delta,
    x2.food_provider_id,
    x2.promoted_from_user_id,
    x2.promoted_from_food_id,
    x2.confidence_level
from
    foods x2
    left outer join foods x3 on (x2.original_food_id = x3.original_food_id)
    and (x3.version > x2.version)
where x2.deleted = true
  and x3.id is null
  and (x2.updated_at = '2022-09-15T10:57:43Z' and x2.id >= -9223372036854775808 or x2.updated_at > '2022-09-15T10:57:43Z')
  and x2.is_public = false
  and x2.food_type in (0, 1, 2)
order by
    x2.updated_at,
    x2.id
limit
    1
`
)

func TestToto(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()


	mcmp.Exec("insert into foods (original_food_id, updated_at, id, is_public, food_type, deleted, version) values (1, '2022-09-15 10:57:43', -1223372036854775808, false, 0, true, 1), (2, '2022-09-19 10:57:43', -2223372036854775808, false, 1, true, 2)")
	for i := 1; i < 10000; i++ {
		mcmp.Exec(fmt.Sprintf("insert into foods (original_food_id, updated_at, id, is_public, food_type, deleted, version) values (1, '2022-09-15 10:57:43', %d, false, 0, true, 1)", i))
	}

	utils.Exec(t, mcmp.VtConn, "SET workload = olap")
	utils.Exec(t, mcmp.VtConn, query)
}
