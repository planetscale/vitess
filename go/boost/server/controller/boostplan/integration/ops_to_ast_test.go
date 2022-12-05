package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/boostplan/integration/utils"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestOpsToAST(t *testing.T) {
	c := operators.NewConverter()
	ddlSchema := utils.LoadExternalDDLSchema(t, []utils.RawDDL{
		{
			Keyspace: "ks",
			SQL:      "create table user (id int, a bigint, primary key(id))",
		},
		{
			Keyspace: "ks",
			SQL:      "create table product (id int, price bigint, primary key(id))",
		},
	})
	boostSI := boostplan.SchemaInformation{
		Schema:      ddlSchema,
		SkipColumns: false,
	}
	si := boostSI.Semantics("ks")

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
		query: "select x.id from user as x",
		keys:  []int{0},
		res: "select ks_user_0.id, ks_user_0.a " +
			"from ks.`user` as ks_user_0 " +
			"where ks_user_0.id = :v0",
		get: grandParent,
	}, { // full materialization
		query: "select user_0.id from user as user_0",
		keys:  []int{},
		res: "select ks_user_0.id, ks_user_0.a " +
			"from ks.`user` as ks_user_0",
		get: grandParent,
	}, {
		query: "select u1.id, u2.a from user u1 join user u2 on u1.a = u2.id",
		keys:  []int{2},
		res: "select ks_user_0.a, ks_user_0.id, ks_user_1.a " +
			"from ks.`user` as ks_user_0, ks.`user` as ks_user_1 " +
			"where ks_user_0.a = ks_user_1.id and ks_user_1.a = :v0",
		get: grandParent,
	}, {
		query: "select u1.id, u1.a, u2.a from user u1 left join user u2 on u1.id = u2.id",
		keys:  []int{1, 3},
		res: "select ks_user_0.id, ks_user_1.id, ks_user_0.a, ks_user_1.a " +
			"from ks.`user` as ks_user_0 left join ks.`user` as ks_user_1 on ks_user_0.id = ks_user_1.id " +
			"where ks_user_1.id = :v0 and ks_user_1.a = :v1",
		get: grandParent,
	}, {
		query: "select u1.id, u1.a, u2.a from user u1 left join user u2 on u1.id = u2.id",
		keys:  []int{1, 2},
		res: "select ks_user_0.id, ks_user_0.a, ks_user_1.a, 0 " +
			"from ks.`user` as ks_user_0 left join ks.`user` as ks_user_1 on ks_user_0.id = ks_user_1.id " +
			"where ks_user_0.a = :v0 and ks_user_1.a = :v1",
		get: parent,
	}, {
		query: "select count(*), a from user join product on user.id = product.id where price = ? group by a",
		keys:  []int{2},
		res: "select count(*), ks_user_0.a, ks_product_0.price " +
			"from ks.`user` as ks_user_0, ks.product as ks_product_0 " +
			"where ks_user_0.id = ks_product_0.id and ks_product_0.price = :v0",
		get: parent,
	}, {
		query: "select * from user join product on user.id = product.id where price = 12 and a = ?",
		keys:  []int{2},
		// notice that we are returning the `ks_user_0.id` twice.
		// since we know from the join that `user.id = product.id`, we can return user.id in place of product.id
		res: "select ks_user_0.id, ks_user_0.a, ks_user_0.id, ks_product_0.price " +
			"from ks.`user` as ks_user_0, ks.product as ks_product_0 " +
			"where ks_product_0.price = 12 and ks_user_0.id = ks_product_0.id and ks_user_0.id = :v0",
		get: parent,
	}, {
		query: "select user.a, product.price from user left join product on user.id = product.id and a > 12",
		keys:  []int{},
		res: "select ks_user_0.a, ks_product_0.price, 0 " +
			"from ks.`user` as ks_user_0 left join ks.product as ks_product_0 " +
			"on ks_user_0.id = ks_product_0.id and ks_user_0.a > 12",
		get: parent,
	}, {
		query: "select id from user order by a limit 10",
		keys:  []int{},
		res:   "select ks_user_0.id, 0, ks_user_0.a from ks.`user` as ks_user_0 order by a asc limit 10",
		get:   parent,
	}}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%s_%d", testCase.query, i), func(t *testing.T) {
			stmt, err := sqlparser.Parse(testCase.query)
			require.NoError(t, err)
			semTable, node, _, err := c.Plan(ddlSchema, si, stmt, "ks", "toto")
			require.NoError(t, err)
			ctx := &operators.PlanContext{SemTable: semTable}
			get := testCase.get(node)
			res, err := operators.ToSQL(ctx, get, testCase.keys)
			require.NoError(t, err)
			require.Equal(t, testCase.res, sqlparser.String(res))
		})
	}
}
