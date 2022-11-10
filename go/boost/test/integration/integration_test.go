package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller"
	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/sqltypes"
)

func TestIntegrationPermutations(t *testing.T) {
	var permutations = []struct {
		Name   string
		T      func(t *testing.T, g *boosttest.Cluster)
		Shards []uint
		Nodes  []uint
	}{
		{
			Name:   "Basic",
			T:      testBasic,
			Shards: []uint{0, 2},
			Nodes:  []uint{1, 2},
		},
		{
			Name:   "ShardedShuffle",
			T:      testShardedSuffle,
			Shards: []uint{0, 2},
			Nodes:  []uint{1, 2},
		},
		{
			Name:   "BroadRecursingUpquery",
			T:      testBroadRecursingSubquery,
			Shards: []uint{1, 4, 8, 16},
			Nodes:  []uint{1},
		},
	}

	for _, tc := range permutations {
		for _, nodes := range tc.Nodes {
			for _, shards := range tc.Shards {
				t.Run(fmt.Sprintf("%s/%dx/%dshard", tc.Name, nodes, shards), func(t *testing.T) {
					g := boosttest.New(t, boosttest.WithMemoryTopo(), boosttest.WithInstances(nodes), boosttest.WithShards(shards))
					tc.T(t, g)
				})
			}
		}
	}
}

func TestBasic(t *testing.T) {
	g := boosttest.New(t, boosttest.WithMemoryTopo(), boosttest.WithShards(2))
	testBasic(t, g)
}

func testBasic(t *testing.T, g *boosttest.Cluster) {
	err := g.Controller().Migrate(context.Background(), func(_ context.Context, mig *controller.Migration) error {
		schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64)
		a := mig.AddBase("a", []string{"a", "b"}, flownode.NewBase([]int{0}, schema, nil))
		b := mig.AddBase("b", []string{"a", "b"}, flownode.NewBase([]int{0}, schema, nil))

		emits := map[graph.NodeIdx][]int{
			a: {0, 1},
			b: {0, 1},
		}
		u := flownode.NewUnion(emits)
		c := mig.AddIngredient("c", []string{"a", "b"}, u)
		mig.MaintainAnonymous(c, []int{0})
		return nil
	})
	require.NoError(t, err, "migration failed")

	var id = sqltypes.NewInt64(1)
	var cq = g.View("c")
	var muta = g.Table("a")
	var mutb = g.Table("b")

	require.Equal(t, "a", muta.Table.TableName())
	require.Equal(t, []string{"a", "b"}, muta.Table.Columns())

	muta.Insert([]sqltypes.Value{id, sqltypes.NewInt64(2)})

	cq.AssertLookup(
		[]sqltypes.Value{id},
		[]sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}},
	)

	mutb.Insert([]sqltypes.Value{id, sqltypes.NewInt64(4)})

	cq.AssertLookup(
		[]sqltypes.Value{id},
		[]sqltypes.Row{{id, sqltypes.NewInt64(2)}, {id, sqltypes.NewInt64(4)}},
	)

	muta.Delete([]sqltypes.Value{id})

	cq.AssertLookup(
		[]sqltypes.Value{id},
		[]sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewInt64(4)}},
	)
}

func TestShardedSuffle(t *testing.T) {
	g := boosttest.New(t, boosttest.WithMemoryTopo(), boosttest.WithShards(2), boosttest.WithInstances(2))
	testShardedSuffle(t, g)
}

func testShardedSuffle(t *testing.T, g *boosttest.Cluster) {
	// in this test, we have a single sharded base node that is keyed on one column, and a sharded
	// reader that is keyed by a different column. this requires a shuffle. we want to make sure
	// that that shuffle happens correctly.
	err := g.Controller().Migrate(context.Background(), func(_ context.Context, mig *controller.Migration) error {
		schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64)
		a := mig.AddBase("base", []string{"id", "non_id"}, flownode.NewBase([]int{0}, schema, nil))
		mig.MaintainAnonymous(a, []int{1})
		return nil
	})
	require.NoError(t, err)

	base := g.Table("base")
	view := g.View("base")

	// make sure there is data on >1 shard, and that we'd get multiple rows by querying the reader
	// for a single key.
	var batch []sqltypes.Row
	for i := int64(0); i < 100; i++ {
		batch = append(batch, sqltypes.Row{sqltypes.NewInt64(i), sqltypes.NewInt64(1)})
	}

	base.BatchInsert(batch)

	// drum roll, please...
	view.AssertLookupLen(sqltypes.Row{sqltypes.NewInt64(1)}, 100)
}

func TestBroadRecursingSubquery(t *testing.T) {
	g := boosttest.New(t, boosttest.WithMemoryTopo(), boosttest.WithShards(16))
	testBroadRecursingSubquery(t, g)
}

func testBroadRecursingSubquery(t *testing.T, g *boosttest.Cluster) {
	// our goal here is to have a recursive upquery such that both levels of the upquery require
	// contacting _all_ shards. in this setting, any miss at the leaf requires the upquery to go to
	// all shards of the intermediate operator, and each miss there requires an upquery to each
	// shard of the top level. as a result, we would expect every base to receive 2 upqueries for
	// the same key, and a total of n^2+n upqueries. crucially, what we want to test is that the
	// partial logic correctly manages all these requests, and the resulting responses (especially
	// at the shard mergers). to achieve this, we're going to use this layout:
	//
	// base x    base y [sharded by a]
	//   |         |
	//   +----+----+ [lookup by b]
	//        |
	//      join [sharded by b]
	//        |
	//     reader [sharded by c]
	//
	// we basically _need_ a join in order to get this layout, since only joins allow us to
	// introduce a new sharding without also dropping all columns that are not the sharding column
	// (like aggregations would). with an aggregation for example, the downstream view could not be
	// partial, since it would have no way to know the partial key to upquery for given a miss,
	// since the miss would be on an _output_ column of the aggregation. we _could_ use a
	// multi-column aggregation group by, but those have their own problems that we do not want to
	// exercise here.
	//
	// we're also going to make the join a left join so that we know the upquery will go to base_x.
	err := g.Controller().Migrate(context.Background(), func(_ context.Context, mig *controller.Migration) error {
		schemaX := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64, sqltypes.Int64)
		schemaY := boostpb.TestSchema(sqltypes.Int64)

		x := mig.AddBase("base_x", []string{"base_col", "join_col", "reader_col"}, flownode.NewBase([]int{0}, schemaX, nil))
		y := mig.AddBase("base_y", []string{"id"}, flownode.NewBase([]int{0}, schemaY, nil))

		join := mig.AddIngredient("join", []string{"base_col", "join_col", "reader_col"},
			flownode.NewJoin(x, y, flownode.JoinTypeOuter,
				[]flownode.JoinSource{flownode.JoinSourceLeft(0),
					flownode.JoinSourceBoth(1, 0),
					flownode.JoinSourceLeft(2)}))

		mig.Maintain("reader", join, []int{2}, []boostpb.ViewParameter{{Name: "k0"}}, 0)
		return nil
	})
	require.NoError(t, err)

	const N = 10_000
	base := g.Table("base_x")
	reader := g.View("reader")

	// we want to make sure that all the upqueries recurse all the way to cause maximum headache
	// for the partial logic. we do this by ensuring that every shard at every operator has at
	// least one record. we also ensure that we can get _all_ the rows by querying a single key on
	// the reader.
	var batch []sqltypes.Row
	for i := int64(0); i < N; i++ {
		batch = append(batch, sqltypes.Row{sqltypes.NewInt64(i), sqltypes.NewInt64(i % int64(g.Config.Shards)), sqltypes.NewInt64(1)})
	}

	base.BatchInsert(batch)

	// moment of truth
	rows := reader.AssertLookupLen(sqltypes.Row{sqltypes.NewInt64(1)}, N)
	for i := int64(0); i < N; i++ {
		var found bool
		for _, r := range rows.Rows {
			if ii, _ := r[0].ToInt64(); ii == i {
				found = true
				break
			}
		}
		require.Truef(t, found, "value %d is missing from rows", i)
	}
}

func TestShardedInterdomainAncestors(t *testing.T) {
	g := boosttest.New(t, boosttest.WithMemoryTopo(), boosttest.WithShards(2))

	err := g.Controller().Migrate(context.Background(), func(ctx context.Context, mig *controller.Migration) error {
		schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64)
		a := mig.AddBase("a", []string{"a", "b"}, flownode.NewBase(nil, schema, nil))

		u1 := flownode.NewUnion(map[graph.NodeIdx][]int{a: {0, 1}})
		b := mig.AddIngredient("b", []string{"a", "b"}, u1)
		mig.MaintainAnonymous(b, []int{0})

		u2 := flownode.NewUnion(map[graph.NodeIdx][]int{a: {0, 1}})
		c := mig.AddIngredient("c", []string{"a", "b"}, u2)
		mig.MaintainAnonymous(c, []int{0})

		return nil
	})
	require.NoError(t, err)

	bq := g.View("b")
	cq := g.View("c")
	muta := g.Table("a")
	id := sqltypes.NewInt64(1)

	muta.Insert([]sqltypes.Value{id, sqltypes.NewInt64(2)})

	bq.AssertLookup(
		[]sqltypes.Value{id},
		[]sqltypes.Row{{id, sqltypes.NewInt64(2)}},
	)

	cq.AssertLookup(
		[]sqltypes.Value{id},
		[]sqltypes.Row{{id, sqltypes.NewInt64(2)}},
	)

	id = sqltypes.NewInt64(2)
	muta.Insert([]sqltypes.Value{id, sqltypes.NewInt64(4)})

	bq.AssertLookup(
		[]sqltypes.Value{id},
		[]sqltypes.Row{{id, sqltypes.NewInt64(4)}},
	)

	cq.AssertLookup(
		[]sqltypes.Value{id},
		[]sqltypes.Row{{id, sqltypes.NewInt64(4)}},
	)
}

func TestWithMaterialization(t *testing.T) {
	g := boosttest.New(t, boosttest.WithMemoryTopo(), boosttest.WithShards(2))

	err := g.Controller().Migrate(context.Background(), func(ctx context.Context, mig *controller.Migration) error {
		schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64)
		a := mig.AddBase("a", []string{"a", "b"}, flownode.NewBase(nil, schema, nil))
		b := mig.AddBase("b", []string{"a", "b"}, flownode.NewBase(nil, schema, nil))
		u := flownode.NewUnion(map[graph.NodeIdx][]int{
			a: {0, 1},
			b: {0, 1},
		})
		c := mig.AddIngredient("c", []string{"a", "b"}, u)
		mig.MaintainAnonymous(c, []int{0})
		return nil
	})
	require.NoError(t, err)

	cq := g.View("c")
	muta := g.Table("a")
	mutb := g.Table("b")
	id := sqltypes.NewInt64(1)

	muta.Insert(sqltypes.Row{id, sqltypes.NewInt64(1)})
	muta.Insert(sqltypes.Row{id, sqltypes.NewInt64(2)})
	muta.Insert(sqltypes.Row{id, sqltypes.NewInt64(3)})

	cq.AssertLookup(
		[]sqltypes.Value{id},
		[]sqltypes.Row{
			{id, sqltypes.NewInt64(1)},
			{id, sqltypes.NewInt64(2)},
			{id, sqltypes.NewInt64(3)},
		},
	)

	mutb.Insert(sqltypes.Row{id, sqltypes.NewInt64(4)})
	mutb.Insert(sqltypes.Row{id, sqltypes.NewInt64(5)})
	mutb.Insert(sqltypes.Row{id, sqltypes.NewInt64(6)})

	cq.AssertLookup(
		[]sqltypes.Value{id},
		[]sqltypes.Row{
			{id, sqltypes.NewInt64(1)},
			{id, sqltypes.NewInt64(2)},
			{id, sqltypes.NewInt64(3)},
			{id, sqltypes.NewInt64(4)},
			{id, sqltypes.NewInt64(5)},
			{id, sqltypes.NewInt64(6)},
		},
	)
}

func TestWithPartialMaterialization(t *testing.T) {
	g := boosttest.New(t, boosttest.WithMemoryTopo(), boosttest.WithShards(2))

	var a graph.NodeIdx
	var b graph.NodeIdx

	err := g.Controller().Migrate(context.Background(), func(ctx context.Context, mig *controller.Migration) error {
		schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64)
		a = mig.AddBase("a", []string{"a", "b"}, flownode.NewBase(nil, schema, nil))
		b = mig.AddBase("b", []string{"a", "b"}, flownode.NewBase(nil, schema, nil))
		return nil
	})
	require.NoError(t, err)

	muta := g.Table("a")
	id := sqltypes.NewInt64(1)

	muta.Insert(sqltypes.Row{id, sqltypes.NewInt64(1)})
	muta.Insert(sqltypes.Row{id, sqltypes.NewInt64(2)})
	muta.Insert(sqltypes.Row{id, sqltypes.NewInt64(3)})

	err = g.Controller().Migrate(context.Background(), func(ctx context.Context, mig *controller.Migration) error {
		u := flownode.NewUnion(map[graph.NodeIdx][]int{
			a: {0, 1},
			b: {0, 1},
		})
		c := mig.AddIngredient("c", []string{"a", "b"}, u)
		mig.MaintainAnonymous(c, []int{0})
		return nil
	})
	require.NoError(t, err)

	cq := g.View("c")
	// TODO: check cq.Len()

	cq.AssertLookup(
		[]sqltypes.Value{id},
		[]sqltypes.Row{
			{id, sqltypes.NewInt64(1)},
			{id, sqltypes.NewInt64(2)},
			{id, sqltypes.NewInt64(3)},
		},
	)
}
