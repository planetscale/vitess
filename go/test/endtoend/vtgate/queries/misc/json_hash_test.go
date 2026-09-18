/*
Copyright 2026 The Vitess Authors.

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

package misc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
)

// TestJSONHashEquality checks that vtgate-side operators that dedupe or match
// rows by hash (DISTINCT, UNION DISTINCT, hash joins, the compiled IN table)
// treat same-cardinality JSON arrays and objects as distinct values, and
// numerically equal JSON numbers as equal, matching MySQL.
func TestJSONHashEquality(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// Documents with the same cardinality but different contents, spread across both shards.
	mcmp.Exec(`insert into all_types(id, json_col) values
		(1, '[1]'), (2, '[2]'), (3, '[1]'),
		(4, '{"a": 1}'), (5, '{"b": 1}'), (6, '{"a": 1}'),
		(7, '1'), (8, '1.0'), (9, '[1, {"b": 2}]'), (10, '[1.0, {"b": 2}]')`)

	queries := []struct {
		name         string
		query        string
		planContains string
	}{{
		name:         "scatter distinct",
		query:        `select distinct json_col from all_types`,
		planContains: `"OperatorType": "Distinct"`,
	}, {
		name:         "union distinct",
		query:        `select json_col from all_types union select json_col from all_types`,
		planContains: `"OperatorType": "Distinct"`,
	}, {
		name:         "hash join on json",
		query:        `select a.id, b.id from (select id, json_col from all_types order by id limit 100) a join (select id, json_col from all_types order by id limit 100) b on a.json_col = b.json_col order by a.id, b.id`,
		planContains: `"Variant": "HashJoin"`,
	}, {
		name:         "vtgate-evaluated IN over json literals",
		query:        `select min(id), cast((count(*) > 0 and json_col in (json_array(1), json_object('a', 1))) as signed) from all_types group by json_col order by min(id)`,
		planContains: `"OperatorType": "Projection"`,
	}, {
		name:         "vtgate-evaluated NOT IN over json literals",
		query:        `select min(id), cast((count(*) > 0 and json_col not in (json_array(1), json_object('a', 1))) as signed) from all_types group by json_col order by min(id)`,
		planContains: `"OperatorType": "Projection"`,
	}, {
		name:         "pushed-down IN over json literals",
		query:        `select id from all_types where json_col in (json_array(1), json_object('a', 1)) order by id`,
		planContains: `"OperatorType": "Route"`,
	}}

	for _, q := range queries {
		mcmp.Run(q.name, func(mcmp *utils.MySQLCompare) {
			plan := mcmp.VExplain(q.query)
			t.Logf("%s plan:\n%s", q.name, plan)
			assert.Contains(t, plan, q.planContains)
			res := mcmp.Exec(q.query)
			require.NotNil(t, res)
			assert.NotEmpty(t, res.Rows)
		})
	}
}
