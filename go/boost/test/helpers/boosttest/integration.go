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
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/boost/server/worker"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testexecutor"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
	"vitess.io/vitess/go/boost/topo/client"
	toposerver "vitess.io/vitess/go/boost/topo/server"
	topowatcher "vitess.io/vitess/go/boost/topo/watcher"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtboost"
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

func WithUpqueryMode(mode string) Option {
	m, ok := boostpb.UpqueryMode_value[mode]
	if !ok {
		panic("invalid upquery mode")
	}
	return func(c *Cluster) {
		c.Config.DomainConfig.UpqueryMode = boostpb.UpqueryMode(m)
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

func New(t *testing.T, options ...Option) *Cluster {
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
				vtgate.CellsToWatch = &cluster.cellsToWatch
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

	if err := cluster.checkDomainSerialization(); err != nil {
		t.Fatal(err)
	}

	if cluster.defaultRecipe != nil {
		cluster.ApplyRecipe(cluster.defaultRecipe)
	}

	return cluster
}

func IsNotSupportedError(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(*operators.UnsupportedError)
	return ok
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

func (c *Cluster) checkDomainSerialization() error {
	ctrl := c.Controller()
	var err error
	ctrl.Inner().BuildDomain = func(idx boostpb.DomainIndex, shard, numShards uint, nodes *flownode.Map, config *boostpb.DomainConfig) *boostpb.DomainBuilder {
		dom := domain.ToProto(idx, shard, numShards, nodes, config)

		converted := flownode.NewMapFromProto(dom.Nodes)

		options := []cmp.Option{
			cmp.Exporter(func(reflect.Type) bool {
				return true
			}),
		}
		if diff := cmp.Diff(nodes, converted, options...); diff != "" {
			err = fmt.Errorf("domain.ToProto() mismatch (-want +got):\n%s", diff)
		}
		return dom
	}
	if err != nil {
		return err
	}
	return nil
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

func (tv *TestView) Lookup(key []sqltypes.Value) *sqltypes.Result {
	tv.t.Helper()

	if key == nil {
		// Inject bogokey
		key = []sqltypes.Value{sqltypes.NewInt64(0)}
	}

	res, err := tv.View.Lookup(context.Background(), key, true)
	require.NoError(tv.t, err)
	return res
}

func (tv *TestView) LookupByFields(fields map[string]sqltypes.Value) *sqltypes.Result {
	tv.t.Helper()

	res, err := tv.View.LookupByFields(context.Background(), fields, true)
	require.NoError(tv.t, err)
	return res
}

func (tv *TestView) LookupByBindVar(bvars []*querypb.BindVariable) *sqltypes.Result {
	tv.t.Helper()

	res, err := tv.View.LookupByBindVar(context.Background(), bvars, true)
	require.NoError(tv.t, err)
	return res
}

func (tv *TestView) AssertLookupBindVars(bvars []*querypb.BindVariable, expected []sqltypes.Row) *sqltypes.Result {
	tv.t.Helper()

	var err error
	for tries := 0; tries < 3; tries++ {
		rs := tv.LookupByBindVar(bvars)
		if err = RowsEquals(expected, rs.Rows); err == nil {
			return rs
		}
		time.Sleep(3 * time.Millisecond)
	}
	tv.t.Fatal(err)
	return nil
}

func (tv *TestView) AssertLookup(key []sqltypes.Value, expected []sqltypes.Row) *sqltypes.Result {
	tv.t.Helper()

	var err error
	var rs *sqltypes.Result
	for tries := 0; tries < 3; tries++ {
		rs = tv.Lookup(key)
		if err = RowsEquals(expected, rs.Rows); err == nil {
			return rs
		}
		time.Sleep(5 * time.Millisecond)
	}
	tv.t.Fatal(err)
	return nil
}

func (tv *TestView) AssertLookupLen(key []sqltypes.Value, expected int) *sqltypes.Result {
	tv.t.Helper()

	rs := tv.Lookup(key)
	require.Len(tv.t, rs.Rows, expected)
	return rs
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
