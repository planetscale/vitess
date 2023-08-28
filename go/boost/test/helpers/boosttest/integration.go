package boosttest

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/slice"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/boostrpc/service"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/common/graphviz"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/server"
	"vitess.io/vitess/go/boost/server/controller"
	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/boost/server/controller/materialization"
	"vitess.io/vitess/go/boost/sql"
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

	cachedConns   topowatcher.Dialer
	cachedDomains *common.SyncMap[string, boostrpc.DomainClient]

	Config        *config.Config
	Instances     uint
	UUID          string
	RecipeVersion int64
	Topo          *topo.Server
	TabletManager toposerver.TabletManager
	Executor      *testexecutor.Executor

	externalExecutor bool
	localCell        string
	cellsToWatch     string
	defaultRecipe    *testrecipe.Recipe
	ignore           []string
	seed             func(*Cluster)
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

func WithCustomBoostConfig(configure func(cfg *config.Config)) Option {
	return func(c *Cluster) {
		configure(c.Config)
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

func WithFakeExecutor(executor *testexecutor.Executor) Option {
	return func(c *Cluster) {
		c.Executor = executor
	}
}

func WithFakeExecutorOptions(configure func(options *testexecutor.Options)) Option {
	return func(c *Cluster) {
		if c.Executor == nil {
			c.t.Fatalf("no FakeExecutor configured")
		}
		c.Executor.Configure(configure)
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

func Ignore(query ...string) Option {
	return func(c *Cluster) {
		c.ignore = append(c.ignore, query...)
	}
}

func WithSeed(fn func(g *Cluster)) Option {
	return func(c *Cluster) {
		c.seed = fn
	}
}

func New(t testing.TB, options ...Option) *Cluster {
	t.Helper()
	var cluster = &Cluster{
		t:             t,
		cachedConns:   topowatcher.NewCachedDialer(),
		cachedDomains: common.NewSyncMap[string, boostrpc.DomainClient](),
		localCell:     DefaultLocalCell,

		Instances: 1,
		Config:    config.DefaultConfig(),
		UUID:      uuid.New().String(),
	}
	cluster.Config.Reuse = config.ReuseType_NO_REUSE

	for _, opt := range options {
		opt(cluster)
	}

	t.Cleanup(cluster.shutdown)

	var ctx context.Context
	ctx, cluster.cancel = context.WithCancel(context.Background())

	var logger *zap.Logger
	if testing.Verbose() {
		var err error
		cfg := zap.NewDevelopmentConfig()
		if os.Getenv("CI") == "true" {
			cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
		}
		logger, err = cfg.Build()
		if err != nil {
			t.Fatal(err)
		}
	} else {
		logger = zap.NewNop()
	}

	if cluster.Instances == 0 {
		t.Fatal("need at least one node")
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
			err := s.ConfigureVitessExecutor(ctx, logger, cluster.Topo, cluster.localCell, cluster.cellsToWatch, 2*time.Millisecond, time.Minute)
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
	c.t.Helper()
	return c.Executor.TestExecute(sqlwithparams, args...)
}

func (c *Cluster) checkDomainSerialization() {
	c.t.Helper()
	ctrl := c.Controller()
	ctrl.Inner().BuildDomain = func(idx dataflow.DomainIdx, shard, numShards uint, nodes *flownode.Map, cfg *config.Domain) (*service.DomainBuilder, error) {
		dom, err := domain.ToProto(idx, shard, numShards, nodes, cfg)
		if err != nil {
			return nil, err
		}

		converted := flownode.MapFromProto(dom.Nodes)
		options := []cmp.Option{
			// btree.Map: compare keys and values directly, not the maps themselves
			cmp.Comparer(func(a, b btree.Map[dataflow.LocalNodeIdx, []int]) bool {
				return reflect.DeepEqual(a.Keys(), b.Keys()) && reflect.DeepEqual(a.Values(), b.Values())
			}),
			cmp.Comparer(func(a, b btree.Map[dataflow.LocalNodeIdx, int]) bool {
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
			cmp.Comparer(func(a, b sql.EvalExpr) bool {
				_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
					if offset, ok := node.(*sqlparser.Offset); ok {
						offset.Original = nil
					}
					return true, nil
				}, a.Expr, b.Expr)
				return sqlparser.Equals.Expr(a.Expr, b.Expr)
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

func (c *Cluster) DebugEviction(renderGraphviz bool, forceLimits map[string]int64) *materialization.EvictionPlan {
	c.t.Helper()
	if renderGraphviz {
		resp, err := c.Controller().Graphviz(context.Background(),
			&vtboost.GraphvizRequest{
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

func (c *Cluster) shutdown() {
	c.t.Helper()
	c.cachedConns.Close()
	c.cancel()

	err := c.wg.Wait()
	if err != nil && err != context.Canceled {
		c.t.Errorf("error when shutting down: %v", err)
	}
	c.Topo.Close()
}

func (c *Cluster) ViewGraphviz() {
	c.t.Helper()
	gz, err := c.Controller().Graphviz(context.Background(), &vtboost.GraphvizRequest{})
	if err != nil {
		c.t.Fatal(err)
	}
	graphviz.RenderGraphviz(c.t, gz.Dot)
}

type TestView struct {
	View *topowatcher.View
	t    testing.TB
}

func (tv *TestView) LookupByFields(fields map[string]sqltypes.Value) *sqltypes.Result {
	if tv == nil {
		return nil
	}

	tv.t.Helper()

	res, err := tv.View.LookupByFields(context.Background(), fields, true)
	require.NoError(tv.t, err)
	return res
}

type Lookup struct {
	t   testing.TB
	try func() (*sqltypes.Result, error)
}

func (tv *TestView) bvars(govals []any) []*querypb.BindVariable {
	tv.t.Helper()

	var bvar []*querypb.BindVariable
	for _, f := range govals {
		v, err := sqltypes.BuildBindVariable(f)
		if err != nil {
			tv.t.Fatal(err)
		}
		bvar = append(bvar, v)
	}
	return bvar
}

func (tv *TestView) LookupBvar(govals ...any) *Lookup {
	if tv == nil {
		return nil
	}

	tv.t.Helper()
	bvar := tv.bvars(govals)
	return &Lookup{
		t: tv.t,
		try: func() (*sqltypes.Result, error) {
			return tv.View.LookupByBVar(context.Background(), bvar, true)
		},
	}
}

func (tv *TestView) Lookup(govals ...any) *Lookup {
	if tv == nil {
		return nil
	}

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

func (l *Lookup) ExpectRow(expected []sqltypes.Row) *sqltypes.Result {
	if l == nil {
		return nil
	}

	l.t.Helper()
	return l.expect(func(result *sqltypes.Result) error {
		return sqltypes.RowsEquals(expected, result.Rows)
	})
}

func (l *Lookup) Expect(expected string) *sqltypes.Result {
	if l == nil {
		return nil
	}

	l.t.Helper()

	expectedRows, err := sqltypes.ParseRows(expected)
	if err != nil {
		l.t.Fatalf("malformed row string: %s (%v)", expected, err)
	}

	return l.expect(func(result *sqltypes.Result) error {
		return sqltypes.RowsEquals(expectedRows, result.Rows)
	})
}

func (l *Lookup) ExpectSorted(expected string) *sqltypes.Result {
	if l == nil {
		return nil
	}

	l.t.Helper()
	return l.expect(func(result *sqltypes.Result) error {
		resultStr := fmt.Sprintf("%v", result.Rows)
		if expected == resultStr {
			return nil
		}
		return fmt.Errorf("expected %q = %q", expected, resultStr)
	})
}

func (l *Lookup) ExpectLen(expected int) *sqltypes.Result {
	if l == nil {
		return nil
	}

	l.t.Helper()
	return l.expect(func(result *sqltypes.Result) error {
		if len(result.Rows) != expected {
			return fmt.Errorf("wrong row count: got %d, expected %d", len(result.Rows), expected)
		}
		return nil
	})
}

const MaxTries = 10

func (l *Lookup) expect(check func(result *sqltypes.Result) error) *sqltypes.Result {
	l.t.Helper()

	var rs *sqltypes.Result
	var err error
	for tries := 0; tries < MaxTries; tries++ {
		if rs, err = l.try(); err == nil {
			if err = check(rs); err == nil {
				return rs
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	l.t.Fatal(err)
	return nil
}

func (l *Lookup) ExpectError() {
	if l == nil {
		return
	}

	l.t.Helper()

	_, err := l.try()
	if err == nil {
		l.t.Fatalf("expected lookup to fail")
	}
}

func (l *Lookup) ExpectErrorEventually() {
	if l == nil {
		return
	}

	l.t.Helper()

	for i := 0; i < 100; i++ {
		_, err := l.try()
		if err != nil {
			l.t.Logf("failed after %d attempts: %v", i, err)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	l.t.Fatalf("expected lookup to fail")
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
	c.t.Helper()
	view := c.FindView(name)
	if view == nil && !slices.Contains(c.ignore, name) {
		c.t.Fatalf("missing View in cluster: %q", name)
	}
	return view
}

func (c *Cluster) ApplyRecipe(recipe *testrecipe.Recipe) {
	c.t.Helper()
	if err := c.TryApplyRecipe(recipe); err != nil {
		c.t.Fatalf("failed to PutRecipeWithOptions(): %v", err)
	}
}

func (c *Cluster) TryApplyRecipe(recipe *testrecipe.Recipe) error {
	c.t.Helper()
	recipe.Update(c.t)

	if c.Executor != nil && c.RecipeVersion == 0 {
		c.Executor.TestApplyRecipe(recipe)

		if c.seed != nil {
			c.seed(c)
		}
	}

	c.RecipeVersion++
	recipepb := &vtboost.Recipe{Version: c.RecipeVersion}

	for _, q := range recipe.Queries {
		if !slices.Contains(c.ignore, q.PublicId) {
			recipepb.Queries = append(recipepb.Queries, q)
		}
	}

	return c.Controller().PutRecipeWithOptions(context.Background(), recipepb, recipe.SchemaInformation())
}

func (c *Cluster) AlterRecipe(recipe *testrecipe.Recipe, ddl string) {
	c.t.Helper()
	if c.Executor == nil {
		c.t.Fatalf("cannot alter table without a Executor")
	}
	c.Executor.TestAlterRecipe(recipe, c.Executor.Keyspace(), ddl)
}

type Metric interface {
	Counts() map[string]int64
}

func (c *Cluster) workerStats(metric Metric) (total int) {
	c.t.Helper()
	workers := slice.Map(c.ServerInstances(), func(srv *server.Server) string {
		return srv.Worker.UUID().String()
	})

	for key, m := range metric.Counts() {
		if slices.ContainsFunc(workers, func(w string) bool { return strings.HasPrefix(key, w) }) {
			total += int(m)
		}
	}
	return total
}

func (c *Cluster) AssertWorkerStats(expected int, metric Metric) {
	c.t.Helper()
	var stats int
	for tries := 0; tries < MaxTries; tries++ {
		stats = c.workerStats(metric)
		if stats == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Errorf("expected %v = %d, got %d", metric, expected, stats)
}
