package boosttest

import (
	"context"
	"fmt"
	"log"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"storj.io/drpc"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/common/graphviz"
	"vitess.io/vitess/go/boost/dataflow/domain"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/server"
	"vitess.io/vitess/go/boost/server/controller"
	"vitess.io/vitess/go/boost/server/worker"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testexecutor"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
	"vitess.io/vitess/go/boost/topo/client"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	topowatcher "vitess.io/vitess/go/boost/topo/watcher"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtgate"
)

const DefaultLocalCell = "zone1"

func Settle() {
	time.Sleep(500 * time.Millisecond)
}

type Cluster struct {
	t testing.TB

	servers []*server.Server
	ctrl    *controller.Server

	wg     errgroup.Group
	cancel context.CancelFunc

	cachedConns   *common.SyncMap[string, drpc.Conn]
	cachedDomains *common.SyncMap[string, boostrpc.DomainClient]
	cachedNodes   map[string][]*flownode.Node

	Config        *boostpb.Config
	Instances     uint
	UUID          string
	RecipeVersion int64
	Topo          *topo.Server
	TabletManager toposerver.TabletManager
	Executor      *testexecutor.Executor

	externalExecutor bool
	localCell        string
	cellsToWatch     string
	schemaChangeUser string
	defaultRecipe    *testrecipe.Recipe
}

type Option func(c *Cluster)

func WithInstances(count uint) Option {
	return func(c *Cluster) {
		c.Instances = count
	}
}

func WithTopoServer(ts *topo.Server) Option {
	return func(c *Cluster) {
		c.Topo = ts
	}
}

func WithMemoryTopo() Option {
	return func(c *Cluster) {
		c.Topo = memorytopo.NewServer()
	}
}

func WithUpqueryMode(mode boostpb.UpqueryMode) Option {
	return func(c *Cluster) {
		c.Config.DomainConfig.UpqueryMode = mode
	}
}

func WithShards(s uint) Option {
	return func(c *Cluster) {
		c.Config.Shards = s
	}
}

func WithLocalCell(cell string) Option {
	return func(c *Cluster) {
		c.localCell = cell
	}
}

func WithCellsToWatch(cells string) Option {
	return func(c *Cluster) {
		c.cellsToWatch = cells
	}
}

func WithSchemaChangeUser(user string) Option {
	return func(c *Cluster) {
		c.schemaChangeUser = user
	}
}

func WithFakeExecutor(executor *testexecutor.Executor) Option {
	return func(c *Cluster) {
		c.Executor = executor
	}
}

func WithFakeExecutorLatency(latency time.Duration) Option {
	return func(c *Cluster) {
		if c.Executor == nil {
			c.t.Fatalf("no FakeExecutor configured")
		}
		c.Executor.SetVStreamLatency(latency)
	}
}

func WithVitessExecutor() Option {
	return func(c *Cluster) {
		c.externalExecutor = true
	}
}

func WithTabletManager(tm toposerver.TabletManager) Option {
	return func(c *Cluster) {
		c.TabletManager = tm
	}
}

func WithTestRecipe(recipe *testrecipe.Recipe) Option {
	return func(c *Cluster) {
		c.defaultRecipe = recipe
	}
}

func WithClusterUUID(uuid string) Option {
	return func(c *Cluster) {
		c.UUID = uuid
	}
}

func New(t testing.TB, options ...Option) *Cluster {
	var cluster = &Cluster{
		t:             t,
		cachedConns:   common.NewSyncMap[string, drpc.Conn](),
		cachedDomains: common.NewSyncMap[string, boostrpc.DomainClient](),
		cachedNodes:   make(map[string][]*flownode.Node),
		localCell:     DefaultLocalCell,

		Instances: 1,
		Config:    boostpb.DefaultConfig(),
		UUID:      uuid.New().String(),
	}
	cluster.Config.Reuse = boostpb.ReuseType_NO_REUSE

	for _, opt := range options {
		opt(cluster)
	}

	t.Cleanup(cluster.shutdown)

	var ctx context.Context
	ctx, cluster.cancel = context.WithCancel(context.Background())

	var logger *zap.Logger
	if testing.Verbose() {
		var err error
		logger, err = zap.NewDevelopment()
		if err != nil {
			t.Fatal(err)
		}
	} else {
		logger = zap.NewNop()
	}

	if cluster.Instances == 0 {
		panic("need at least one node")
	}

	for n := uint(0); n < cluster.Instances; n++ {
		listen, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("cluster[%d] listening at %s\n", n, listen.Addr().String())

		logger := logger.With(zap.Uint("proc", n))
		s := server.NewBoostInstance(logger, cluster.Topo, cluster.TabletManager, cluster.Config, cluster.UUID)

		switch {
		case cluster.Executor != nil:
			s.Worker.SetExecutor(cluster.Executor)
			s.Worker.SetResolver(testexecutor.NewResolver(cluster.Executor))
		case cluster.externalExecutor:
			if cluster.cellsToWatch != "" {
				vtgate.CellsToWatch = cluster.cellsToWatch
			}
			err := s.ConfigureVitessExecutor(ctx, logger, cluster.Topo, cluster.localCell, cluster.schemaChangeUser)
			if err != nil {
				t.Fatal(err)
			}
		}

		cluster.servers = append(cluster.servers, s)
		cluster.wg.Go(func() error {
			return s.Serve(ctx, listen)
		})
	}

	cli := client.NewClient(cluster.Topo)
	if _, err := cli.AddCluster(context.Background(), &vtboost.AddClusterRequest{
		Uuid:                cluster.UUID,
		ExpectedWorkerCount: uint32(cluster.Instances),
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := cli.MakePrimaryCluster(context.Background(), &vtboost.PrimaryClusterRequest{Uuid: cluster.UUID}); err != nil {
		t.Fatal(err)
	}

	cluster.checkDomainSerialization()

	if cluster.defaultRecipe != nil {
		cluster.ApplyRecipe(cluster.defaultRecipe)
	}

	return cluster
}

func (c *Cluster) Controller() *controller.Server {
	if c.ctrl == nil {
		start := time.Now()
		tick := time.NewTicker(1 * time.Millisecond)
		defer tick.Stop()
		timeout := time.NewTimer(10 * time.Second)
		defer timeout.Stop()

		for {
			select {
			case <-tick.C:
				for _, node := range c.servers {
					if node.Controller.IsReady() {
						log.Printf("waited %v for the controller", time.Since(start))
						c.ctrl = node.Controller
						return c.ctrl
					}
				}
			case <-timeout.C:
				log.Fatal("no leader found")
			}
		}
	}

	return c.ctrl
}

func (c *Cluster) TestExecute(sqlwithparams string, args ...any) *sqltypes.Result {
	return c.Executor.TestExecute(sqlwithparams, args...)
}

func (c *Cluster) checkDomainSerialization() {
	ctrl := c.Controller()
	ctrl.Inner().BuildDomain = func(idx boostpb.DomainIndex, shard, numShards uint, nodes *flownode.Map, config *boostpb.DomainConfig) (*boostpb.DomainBuilder, error) {
		dom, err := domain.ToProto(idx, shard, numShards, nodes, config)
		if err != nil {
			return nil, err
		}

		converted := flownode.NewMapFromProto(dom.Nodes)
		options := []cmp.Option{
			// btree.Map: compare keys and values directly, not the maps themselves
			cmp.Comparer(func(a, b btree.Map[boostpb.LocalNodeIndex, []int]) bool {
				return reflect.DeepEqual(a.Keys(), b.Keys()) && reflect.DeepEqual(a.Values(), b.Values())
			}),
			cmp.Comparer(func(a, b btree.Map[boostpb.LocalNodeIndex, int]) bool {
				return reflect.DeepEqual(a.Keys(), b.Keys()) && reflect.DeepEqual(a.Values(), b.Values())
			}),
			// btree.BTreeG: compare items directly, not the trees themselves
			cmp.Comparer(func(a, b *btree.BTreeG[*flownode.UnionReplay]) bool {
				return reflect.DeepEqual(a.Items(), b.Items())
			}),
			// sqlparser.Offset: only the actual position for the offset matters; offset.Original is not preserved
			cmp.Comparer(func(a, b *sqlparser.Offset) bool {
				return a.V == b.V
			}),
			// Exporter: ensure _all_ private fields for all the Domain data are compared
			cmp.Exporter(func(reflect.Type) bool {
				return true
			}),
		}
		if diff := cmp.Diff(nodes, converted, options...); diff != "" {
			return nil, fmt.Errorf("domain.ToProto() mismatch (-want +got):\n%s", diff)
		}
		return dom, nil
	}
}

func (c *Cluster) ServerInstances() []*server.Server {
	if c.servers != nil {
		return c.servers
	}
	return nil
}

func (c *Cluster) DebugEviction(renderGraphviz bool, forceLimits map[string]int64) *controller.EvictionPlan {
	if renderGraphviz {
		resp, err := c.Controller().Graphviz(context.Background(),
			&vtboost.GraphvizRequest{
				MemoryStats:       true,
				ForceMemoryLimits: forceLimits,
				Clustering:        vtboost.GraphvizRequest_QUERY,
			})
		require.NoError(c.t, err)
		graphviz.RenderGraphviz(c.t, resp.Dot)
	}

	ep, err := c.Controller().PerformDistributedEviction(context.Background(), forceLimits)
	require.NoError(c.t, err)

	return ep
}

func (c *Cluster) FindGraphNodes(check func(n *flownode.Node) bool) []*flownode.Node {
	var found []*flownode.Node
	for _, srv := range c.servers {
		domains := srv.Worker.ActiveDomains()
		domains.ForEach(func(_ boostpb.DomainAddr, dom *domain.Domain) {
			for _, n := range dom.Nodes() {
				if check(n) {
					found = append(found, n)
				}
			}
		})
	}
	if len(found) == 0 {
		c.t.Fatalf("failed to find required graph nodes")
	}
	for _, n := range found {
		shards := common.UnwrapOr(n.Sharding().TryGetShards(), 1)
		if int(shards) != len(found) {
			c.t.Fatalf("node %q has %d shards, but only %d shards found in all active domains", n.Name, shards, len(found))
		}
	}
	return found
}

func (c *Cluster) WaitForNode(kind, name string, wantProcessed uint64) {
	cachedname := fmt.Sprintf("%s(%q)", kind, name)

	nodes, ok := c.cachedNodes[cachedname]
	if !ok {
		nodes = c.FindGraphNodes(func(n *flownode.Node) bool {
			return n.Name == name && n.Kind() == kind
		})
		c.cachedNodes[cachedname] = nodes
	}

	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	for {
		var processed uint64
		for _, n := range nodes {
			processed += atomic.LoadUint64(&n.Stats.Processed)
		}
		if processed >= wantProcessed {
			return
		}
		select {
		case <-timeout.C:
			c.t.Fatalf("timed out while waiting for %s to process %d packets (processed=%d)", cachedname, wantProcessed, processed)
		case <-tick.C:
		}
	}
}

func (c *Cluster) shutdown() {
	c.cachedConns.ForEach(func(_ string, conn drpc.Conn) {
		conn.Close()
	})

	c.cancel()

	err := c.wg.Wait()
	if err != nil && err != context.Canceled {
		c.t.Errorf("error when shutting down: %v", err)
	}
	c.Topo.Close()
}

func (c *Cluster) ViewGraphviz() {
	gz, err := c.Controller().Graphviz(context.Background(), &vtboost.GraphvizRequest{MemoryStats: true})
	if err != nil {
		c.t.Fatal(err)
	}
	graphviz.RenderGraphviz(c.t, gz.Dot)
}

type RowMismatchError struct {
	err       error
	want, got []sqltypes.Row
}

func (e *RowMismatchError) Error() string {
	return fmt.Sprintf("results differ: %v\n\twant: %v\n\tgot:  %v", e.err, e.want, e.got)
}

func RowsEquals(want, got []sqltypes.Row) error {
	if len(want) != len(got) {
		return &RowMismatchError{
			err:  fmt.Errorf("expected %d rows in result, got %d", len(want), len(got)),
			want: want,
			got:  got,
		}
	}

	var matched = make([]bool, len(want))
	for _, aa := range want {
		var ok bool
		for i, bb := range got {
			if matched[i] {
				continue
			}
			if reflect.DeepEqual(aa, bb) {
				matched[i] = true
				ok = true
			}
		}
		if !ok {
			return &RowMismatchError{
				err:  fmt.Errorf("row %v is missing from result", aa),
				want: want,
				got:  got,
			}
		}
	}
	for _, m := range matched {
		if !m {
			panic("did not match properly?")
		}
	}
	return nil
}

type TestView struct {
	View *topowatcher.View
	t    testing.TB
}

func (tv *TestView) LookupByFields(fields map[string]sqltypes.Value) *sqltypes.Result {
	tv.t.Helper()

	res, err := tv.View.LookupByFields(context.Background(), fields, true)
	require.NoError(tv.t, err)
	return res
}

type Lookup struct {
	t   testing.TB
	try func() (*sqltypes.Result, error)
}

func (tv *TestView) LookupBvar(govals ...any) *Lookup {
	tv.t.Helper()

	var bvar []*querypb.BindVariable
	for _, f := range govals {
		v, err := sqltypes.BuildBindVariable(f)
		if err != nil {
			tv.t.Fatal(err)
		}
		bvar = append(bvar, v)
	}

	return &Lookup{
		t: tv.t,
		try: func() (*sqltypes.Result, error) {
			return tv.View.LookupByBindVar(context.Background(), bvar, true)
		},
	}
}

func (tv *TestView) Lookup(govals ...any) *Lookup {
	tv.t.Helper()

	var values []sqltypes.Value
	for _, goval := range govals {
		switch goval := goval.(type) {
		case nil:
			values = append(values, sqltypes.NULL)
		case []byte:
			values = append(values, sqltypes.MakeTrusted(sqltypes.VarBinary, goval))
		case int:
			values = append(values, sqltypes.NewInt64(int64(goval)))
		case int64:
			values = append(values, sqltypes.NewInt64(goval))
		case int32:
			values = append(values, sqltypes.NewInt32(goval))
		case int8:
			values = append(values, sqltypes.NewInt8(goval))
		case uint64:
			values = append(values, sqltypes.NewUint64(goval))
		case uint32:
			values = append(values, sqltypes.NewUint32(goval))
		case float64:
			values = append(values, sqltypes.NewFloat64(goval))
		case string:
			values = append(values, sqltypes.NewVarChar(goval))
		case sqltypes.Value:
			values = append(values, goval)
		default:
			tv.t.Fatalf("unexpected value %T", goval)
		}
	}

	if values == nil {
		values = append(values, sqltypes.NewInt64(0))
	}

	return &Lookup{
		t: tv.t,
		try: func() (*sqltypes.Result, error) {
			return tv.View.Lookup(context.Background(), values, true)
		},
	}
}

func (l *Lookup) Expect(expected []sqltypes.Row) *sqltypes.Result {
	l.t.Helper()
	return l.expect(func(result *sqltypes.Result) error {
		return RowsEquals(expected, result.Rows)
	})
}

func (l *Lookup) ExpectSorted(expected []sqltypes.Row) *sqltypes.Result {
	l.t.Helper()
	return l.expect(func(result *sqltypes.Result) error {
		expectedS := fmt.Sprintf("%v", expected)
		resultS := fmt.Sprintf("%v", result.Rows)
		if expectedS != resultS {
			return fmt.Errorf("mismatch in sorted rows;\nwant: %s\ngot:  %s", expectedS, resultS)
		}
		return nil
	})
}

func (l *Lookup) ExpectLen(expected int) *sqltypes.Result {
	l.t.Helper()
	return l.expect(func(result *sqltypes.Result) error {
		if len(result.Rows) != expected {
			return fmt.Errorf("wrong row count: got %d, expected %d", len(result.Rows), expected)
		}
		return nil
	})
}

func (l *Lookup) expect(check func(result *sqltypes.Result) error) *sqltypes.Result {
	l.t.Helper()

	var rs *sqltypes.Result
	var err error
	for tries := 0; tries < 3; tries++ {
		rs, err = l.try()
		if err != nil {
			continue
		}
		if err = check(rs); err == nil {
			return rs
		}
		time.Sleep(3 * time.Millisecond)
	}
	l.t.Fatal(err)
	return nil
}

func (l *Lookup) ExpectError() {
	l.t.Helper()

	_, err := l.try()
	if err == nil {
		l.t.Fatalf("expected lookup to fail")
	}
}

type TestTable struct {
	Table *worker.Table
	t     testing.TB
}

func (tt *TestTable) Insert(row sqltypes.Row) {
	tt.t.Helper()

	err := tt.Table.Insert(context.Background(), row)
	require.NoError(tt.t, err)
}

func (tt *TestTable) BatchInsert(rows []sqltypes.Row) {
	tt.t.Helper()

	err := tt.Table.BatchInsert(context.Background(), rows)
	require.NoError(tt.t, err)
}

func (tt *TestTable) Delete(key []sqltypes.Value) {
	tt.t.Helper()

	err := tt.Table.Delete(context.Background(), key)
	require.NoError(tt.t, err)
}

func (c *Cluster) FindView(name string) *TestView {
	c.t.Helper()

	descriptor, err := c.Controller().GetViewDescriptor_(name)
	if err != nil {
		return nil
	}

	view, err := topowatcher.NewViewClientFromProto(descriptor, c.cachedConns)
	require.NoError(c.t, err)
	return &TestView{View: view, t: c.t}
}

func (c *Cluster) View(name string) *TestView {
	view := c.FindView(name)
	if view == nil {
		c.t.Fatalf("missing View in cluster: %q", name)
	}
	return view
}

func (c *Cluster) Table(name string) *TestTable {
	c.t.Helper()

	descriptor, err := c.Controller().GetTableDescriptor_(name)
	require.NoError(c.t, err)

	table, err := worker.NewTableClientFromProto(descriptor, c.cachedDomains)
	require.NoError(c.t, err)

	return &TestTable{Table: table, t: c.t}
}

func (c *Cluster) ApplyRecipeEx(recipe *testrecipe.Recipe, mysql, boost bool) {
	recipe.Update(c.t)

	if mysql && c.Executor != nil && c.RecipeVersion == 0 {
		c.Executor.TestApplyRecipe(recipe)
	}
	if boost {
		c.RecipeVersion++
		recipepb := &vtboost.Recipe{
			Queries: recipe.Queries,
			Version: c.RecipeVersion,
		}
		if err := c.Controller().PutRecipeWithOptions(context.Background(), recipepb, recipe.SchemaInformation()); err != nil {
			c.t.Fatalf("failed to PutRecipeWithOptions(): %v", err)
		}
	}
}

func (c *Cluster) ApplyRecipe(recipe *testrecipe.Recipe) {
	c.ApplyRecipeEx(recipe, true, true)
}

func (c *Cluster) AlterRecipe(recipe *testrecipe.Recipe, ddl string) {
	if c.Executor == nil {
		c.t.Fatalf("cannot alter table without a Executor")
	}
	c.Executor.TestAlterRecipe(recipe, c.Executor.Keyspace(), ddl)
}

type Metric interface {
	Counts() map[string]int64
}

func (c *Cluster) WorkerStats(metric Metric) (total int) {
	counts := metric.Counts()
	for _, instance := range c.ServerInstances() {
		total += int(counts[instance.Worker.UUID().String()])
	}
	return total
}

func (c *Cluster) WorkerReads() int {
	return c.WorkerStats(worker.StatViewReads)
}
