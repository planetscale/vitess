package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/boostplan/integration/utils"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/boost/server/controller/boostplan/upquery"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestOpsToAST(t *testing.T) {
	t.Skipf("disabled: upqueries are not enabled yet")

	var schemaBasic = utils.LoadExternalDDLSchema(t, []utils.RawDDL{
		{Keyspace: "ks", SQL: "create table user (id int, a bigint, primary key(id))"},
		{Keyspace: "ks", SQL: "create table product (id int, price bigint, primary key(id))"},
	})

	var schemaAlbums = utils.LoadExternalDDLSchema(t, []utils.RawDDL{
		{Keyspace: "ks", SQL: "CREATE TABLE friend (usera int, userb int, primary key(usera, userb));"},
		{Keyspace: "ks", SQL: "CREATE TABLE album (a_id varchar(255), u_id int, public tinyint(1), primary key(a_id));"},
		{Keyspace: "ks", SQL: "CREATE TABLE photo (p_id varchar(255), album varchar(255), primary key(p_id));"},
	})

	c := operators.NewConverter()
	grandParent := func(n *operators.Node) *operators.Node {
		return n.Ancestors[0].Ancestors[0]
	}
	parent := func(n *operators.Node) *operators.Node {
		return n.Ancestors[0]
	}
	testCases := []struct {
		schema *boostplan.DDLSchema
		query  string
		keys   []int
		res    string
		get    func(n *operators.Node) *operators.Node
	}{{
		schema: schemaBasic,
		query:  "select u1.id, u2.a from user u1 join user u2 on u1.a = u2.id",
		keys:   []int{2},
		res: "select ks_user_0.a, ks_user_0.id, ks_user_1.a " +
			"from ks.`user` as ks_user_0, ks.`user` as ks_user_1 " +
			"where ks_user_0.a = ks_user_1.id and ks_user_1.a = :vtg0",
		get: grandParent,
	}, {
		schema: schemaBasic,
		query:  "select u1.id, u1.a, u2.a from user u1 left join user u2 on u1.id = u2.id",
		keys:   []int{1, 3},
		res: "select ks_user_0.id, ks_user_1.id, ks_user_0.a, ks_user_1.a " +
			"from ks.`user` as ks_user_0 left join ks.`user` as ks_user_1 on ks_user_0.id = ks_user_1.id " +
			"where ks_user_1.id = :vtg0 and ks_user_1.a = :vtg1",
		get: grandParent,
	}, {
		schema: schemaBasic,
		query:  "select u1.id, u1.a, u2.a from user u1 left join user u2 on u1.id = u2.id",
		keys:   []int{1, 2},
		res: "select ks_user_0.id, ks_user_0.a, ks_user_1.a, 0 " +
			"from ks.`user` as ks_user_0 left join ks.`user` as ks_user_1 on ks_user_0.id = ks_user_1.id " +
			"where ks_user_0.a = :vtg0 and ks_user_1.a = :vtg1",
		get: parent,
	}, {
		schema: schemaBasic,
		query:  "select count(*), a from user join product on user.id = product.id where price = ? group by a",
		keys:   []int{2},
		res: "select count(*), ks_user_0.a, ks_product_0.price " +
			"from ks.`user` as ks_user_0, ks.product as ks_product_0 " +
			"where ks_user_0.id = ks_product_0.id and ks_product_0.price = :vtg0",
		get: parent,
	}, {
		schema: schemaBasic,
		query:  "select * from user join product on user.id = product.id where price = 12 and a = ?",
		keys:   []int{2},
		// notice that we are returning the `ks_user_0.id` twice.
		// since we know from the join that `user.id = product.id`, we can return user.id in place of product.id
		res: "select ks_user_0.id, ks_user_0.a, ks_user_0.id, ks_product_0.price " +
			"from ks.`user` as ks_user_0, ks.product as ks_product_0 " +
			"where ks_product_0.price = 12 and ks_user_0.id = ks_product_0.id and ks_user_0.id = :vtg0",
		get: parent,
	}, {
		schema: schemaBasic,
		query:  "select user.a, product.price from user left join product on user.id = product.id and a > 12",
		keys:   []int{},
		res: "select ks_user_0.a, ks_product_0.price, 0 " +
			"from ks.`user` as ks_user_0 left join ks.product as ks_product_0 " +
			"on ks_user_0.id = ks_product_0.id and ks_user_0.a > 12",
		get: parent,
	}, {
		schema: schemaBasic,
		query:  "select id from user order by a limit 10",
		keys:   []int{},
		res:    "select ks_user_0.id, 0, ks_user_0.a from ks.`user` as ks_user_0 order by a asc limit 10",
		get:    parent,
	}, {
		schema: schemaAlbums,
		query: `
(SELECT /*vt+ VIEW=album_friends */ album.a_id AS aid, friend.userb AS uid
 FROM album JOIN friend ON (album.u_id = friend.usera)
 WHERE album.public = 0)
UNION ALL
(SELECT album.a_id AS aid, friend.usera AS uid
 FROM album JOIN friend ON (album.u_id = friend.userb)
 WHERE album.public = 0)`,
		get: parent,
	}}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%s_%d", testCase.query, i), func(t *testing.T) {
			boostSI := boostplan.SchemaInformation{Schema: testCase.schema, SkipColumns: false}
			si := boostSI.Semantics("ks")

			stmt, err := sqlparser.Parse(testCase.query)
			require.NoError(t, err)
			node, _, err := c.Plan(testCase.schema, si, stmt, "ks", "toto")
			require.NoError(t, err)

			upqueryNode := testCase.get(node)
			if upqueryNode.Upquery == nil {
				t.Fatalf("missing upquery for node %q (%T)", upqueryNode.Name, upqueryNode.Op)
			}

			up, err := upquery.New(upqueryNode.Upquery, testCase.keys, boostpb.UpqueryMode_TRACK_GTID)
			require.NoError(t, err)

			require.Equal(t, testCase.res, up.String())
		})
	}
}
