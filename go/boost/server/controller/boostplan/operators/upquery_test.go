package operators_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/boost/server/controller/boostplan/upquery"
	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestOpsToAST(t *testing.T) {
	schema, err := boostplan.LoadExternalDDLSchema([]boostplan.RawDDL{
		{Keyspace: "ks", SQL: "create table user (id int, a bigint, primary key(id))"},
		{Keyspace: "ks", SQL: "create table product (id int, price bigint, primary key(id))"},
		{Keyspace: "ks", SQL: "create table num (pk bigint not null auto_increment, a int, b int, j json, primary key(pk))"},
		{Keyspace: "ks", SQL: "create table num2 (pk bigint not null auto_increment, a int, b smallint, c tinyint, d mediumint, e bigint, f float, g double, h decimal, primary key(pk))"},
	})
	require.NoError(t, err)

	c := operators.NewConverter()
	greatGrandParent := func(n *operators.Node) *operators.Node {
		return n.Ancestors[0].Ancestors[0].Ancestors[0]
	}
	grandParent := func(n *operators.Node) *operators.Node {
		return n.Ancestors[0].Ancestors[0]
	}
	parent := func(n *operators.Node) *operators.Node {
		return n.Ancestors[0]
	}
	testCases := []struct {
		query string
		keys  []int
		res   string
		get   func(n *operators.Node) *operators.Node
	}{{
		query: "select id, id+a, 420 from user",
		keys:  []int{2},
		res:   "select ks_user_0.id as id, ks_user_0.id + ks_user_0.a as `id + a`, 420 as `literal-420`, 0 as `literal-0` from ks.`user` as ks_user_0 where 420 = :vtg0 /* NULL_TYPE */",
		get:   parent,
	}, {
		query: "select u1.id, u2.a from user u1 join user u2 on u1.a = u2.id",
		keys:  []int{2},
		res: "select ks_user_0.a, ks_user_0.id, ks_user_1.a " +
			"from ks.`user` as ks_user_0, ks.`user` as ks_user_1 " +
			"where ks_user_0.a = ks_user_1.id and ks_user_1.a = :vtg0 /* NULL_TYPE */",
		get: grandParent,
	}, {
		query: "select u1.id, u1.a, u2.a from user u1 left join user u2 on u1.id = u2.id",
		keys:  []int{1, 3},
		res: "select ks_user_0.id, ks_user_1.id, ks_user_0.a, ks_user_1.a " +
			"from ks.`user` as ks_user_0 left join ks.`user` as ks_user_1 on ks_user_0.id = ks_user_1.id " +
			"where ks_user_1.id = :vtg0 /* NULL_TYPE */ and ks_user_1.a = :vtg1 /* NULL_TYPE */",
		get: grandParent,
	}, {
		query: "select u1.id, u1.a, u2.a from user u1 left join user u2 on u1.id = u2.id",
		keys:  []int{1, 2},
		res: "select ks_user_0.id as id, ks_user_0.a as a, ks_user_1.a as a, 0 as `literal-0` " +
			"from ks.`user` as ks_user_0 left join ks.`user` as ks_user_1 on ks_user_0.id = ks_user_1.id " +
			"where ks_user_0.a = :vtg0 /* NULL_TYPE */ and ks_user_1.a = :vtg1 /* NULL_TYPE */",
		get: parent,
	}, {
		query: "select count(*), a from user join product on user.id = product.id where price = ? group by a",
		keys:  []int{2},
		res: "select count(*) as `count(*)`, ks_user_0.a as a, ks_product_0.price as price " +
			"from ks.`user` as ks_user_0, ks.product as ks_product_0 " +
			"where ks_user_0.id = ks_product_0.id and ks_product_0.price = :vtg0 /* NULL_TYPE */ " +
			"group by ks_user_0.a, ks_product_0.price",
		get: parent,
	}, {
		query: "select * from user join product on user.id = product.id where price = 12 and a = ?",
		keys:  []int{2},
		// notice that we are returning the `ks_user_0.id` twice.
		// since we know from the join that `user.id = product.id`, we can return user.id in place of product.id
		res: "select ks_user_0.id as id, ks_user_0.a as a, ks_user_0.id as id, ks_product_0.price as price " +
			"from ks.`user` as ks_user_0, ks.product as ks_product_0 " +
			"where ks_product_0.price = 12 and ks_user_0.id = ks_product_0.id and ks_user_0.id = :vtg0 /* NULL_TYPE */",
		get: parent,
	}, {
		query: "select user.a, product.price from user left join product on user.id = product.id and a > 12",
		keys:  []int{},
		res: "select ks_user_0.a as a, ks_product_0.price as price, 0 as `literal-0` " +
			"from ks.`user` as ks_user_0 left join ks.product as ks_product_0 " +
			"on ks_user_0.id = ks_product_0.id and ks_user_0.a > 12",
		get: parent,
	}, {
		query: "select id from user order by a limit 10",
		keys:  []int{},
		res:   "select ks_user_0.id as id, ks_user_0.a as a, 0 as `literal-0` from ks.`user` as ks_user_0 order by ks_user_0.a asc limit 10",
		get:   grandParent,
	}, {
		query: "select id from user UNION ALL select id from product",
		keys:  []int{},
		res:   "select ks_user_0.id as id, 0 as `literal-0` from ks.`user` as ks_user_0 union all select ks_product_0.id as id, 0 as `literal-0` from ks.product as ks_product_0",
		get:   parent,
	}, {
		query: "select id, 12 from user UNION select id, 13 from product",
		keys:  []int{},
		res:   "select ks_user_0.id as id, 12 as `12`, 0 as `literal-0` from ks.`user` as ks_user_0 union select ks_product_0.id as id, 13 as `12`, 0 as `literal-0` from ks.product as ks_product_0",
		get:   parent,
	}, {
		query: "select max(a) from user",
		keys:  []int{},
		res:   "select 0 as `literal-0`, max(ks_user_0.a) from ks.`user` as ks_user_0 group by `literal-0`",
		get:   grandParent,
	}, {
		query: "select 'toto', max(a) from user group by 'toto'",
		keys:  []int{},
		res:   "select 'toto', max(ks_user_0.a) from ks.`user` as ks_user_0 group by 'toto'",
		get:   grandParent,
	}, {
		query: "select * from (select 'toto', max(a) from user group by 'toto') x",
		keys:  []int{},
		res:   "select x_0.toto, x_0.`max(a)` from (select 'toto', max(ks_user_0.a) as `max(a)` from ks.`user` as ks_user_0 group by 'toto') as x_0",
		get:   grandParent,
	}, {
		query: "select sum(a) from user union select sum(price) from product",
		keys:  []int{},
		res:   "select sum(ks_user_0.a) as `sum(a)`, 0 as `literal-0` from ks.`user` as ks_user_0 group by `literal-0` union all select sum(ks_product_0.price) as `sum(a)`, 0 as `literal-0` from ks.product as ks_product_0 group by `literal-0`",
		get:   grandParent,
	}, {
		query: "select count(*) from user where POW(a, 2) = ?",
		keys:  []int{},
		res:   "select POW(ks_user_0.a, 2) from ks.`user` as ks_user_0",
		get:   greatGrandParent,
	}, {
		query: "select count(*) from user where POW(a, 2) = ?",
		keys:  []int{},
		res:   "select count(*) as `count(*)`, POW(ks_user_0.a, 2) from ks.`user` as ks_user_0 group by POW(ks_user_0.a, 2)",
		get:   parent,
	}, {
		query: "SELECT num.a, num.b, 420 FROM num WHERE num.a > :x AND JSON_EXTRACT(num.j, '$.n') = ?",
		keys:  []int{},
		res:   "select ks_num_0.a as a, ks_num_0.b as b, 420 as `literal-420`, json_extract(ks_num_0.j, '$.n') from ks.num as ks_num_0",
		get:   parent,
	}, {
		query: "SELECT col1 + col2, 420 FROM (SELECT a as col1, a+b as col2 FROM num2) apa WHERE col1 = ?",
		keys:  []int{},
		res:   "select apa_0.col1 + apa_0.col2 as `col1 + col2`, 420 as `literal-420`, apa_0.col1 as col1 from (select ks_num2_0.a as col1, ks_num2_0.a + ks_num2_0.b as col2 from ks.num2 as ks_num2_0) as apa_0",
		get:   parent,
	}, {
		query: "SELECT a as col1, b + c as col2 FROM num2 WHERE a = ?",
		keys:  []int{},
		res:   "select ks_num2_0.a as col1, ks_num2_0.b + ks_num2_0.c as col2 from ks.num2 as ks_num2_0",
		get:   parent,
	}}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%s_%d", testCase.query, i), func(t *testing.T) {
			boostSI := boostplan.SchemaInformation{Schema: schema, SkipColumns: false}
			si := boostSI.Semantics("ks")

			stmt, err := sqlparser.Parse(testCase.query)
			require.NoError(t, err)
			node, _, err := c.Plan(schema, si, stmt, "ks", "toto")
			require.NoError(t, err)

			upqueryNode := testCase.get(node)
			require.NotNil(t, upqueryNode.Upquery, "missing upquery for node %q (%T)", upqueryNode.Name, upqueryNode.Op)

			up, err := upquery.New(upqueryNode.Upquery, testCase.keys, true, config.UpqueryMode_TRACK_GTID)
			require.NoError(t, err)

			require.Equal(t, testCase.res, up.String())
		})
	}
}