package testexecutor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/dataflow/domain"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func Default(t *testing.T, options ...Option) *Executor {
	return New(t, []*querypb.Target{
		{
			Keyspace:   testrecipe.DefaultKeyspace,
			Shard:      "-",
			TabletType: topodatapb.TabletType_PRIMARY,
		},
	}, options...)
}

type Option func(ex *Executor)

func WithVStreamLatency(latency time.Duration) Option {
	return func(ex *Executor) {
		ex.vstreamLatency = latency
	}
}

func New(t *testing.T, targets []*querypb.Target, options ...Option) *Executor {
	ks := targets[0].Keyspace
	fakeTargets := make(map[string]*memTarget, len(targets))
	for _, target := range targets {
		if target.Keyspace != ks {
			t.Fatal("Executor only supports one keyspace")
		}
		fakeTargets[targetName(target)] = newMemoryTarget(t, target)
	}
	ex := &Executor{
		t:        t,
		keyspace: ks,
		targets:  fakeTargets,
	}

	for _, opt := range options {
		opt(ex)
	}
	return ex
}

type Executor struct {
	keyspace       string
	t              *testing.T
	targets        map[string]*memTarget
	vstreamLatency time.Duration
	maxBatchSize   int
}

func (ex *Executor) SetVStreamLatency(latency time.Duration) {
	ex.vstreamLatency = latency
}

func (ex *Executor) SetMaxBatchSize(maxBatchSize int) {
	ex.maxBatchSize = maxBatchSize
}

func (ex *Executor) Rollback(ctx context.Context, safeSession *vtgate.SafeSession) error {
	return nil
}

func (ex *Executor) StreamExecute(ctx context.Context, method string, safeSession *vtgate.SafeSession, sql string, bvars map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	if cid := callerid.EffectiveCallerIDFromContext(ctx); cid == nil {
		ex.t.Fatalf("missing: EffectiveCallerID passed on context")
	}
	if cid := callerid.ImmediateCallerIDFromContext(ctx); cid == nil {
		ex.t.Fatalf("missing: ImmediateCallerID passed on context")
	}
	for _, tgt := range ex.targets {
		if tgt.target.TabletType == topodatapb.TabletType_PRIMARY {
			results, err := ex.target(tgt.target).execute(sql, bvars)
			if err != nil {
				return err
			}

			if err := callback(results.Metadata()); err != nil {
				return err
			}

			rows := results.Rows
			if ex.maxBatchSize > 0 {
				for len(rows) >= ex.maxBatchSize {
					if err := callback(&sqltypes.Result{Rows: rows[:ex.maxBatchSize]}); err != nil {
						return err
					}
					rows = rows[ex.maxBatchSize:]
				}
			}
			return callback(&sqltypes.Result{Rows: rows})
		}
	}
	return fmt.Errorf("no primaries found in Executor")
}

func (ex *Executor) VSchema() *vindexes.VSchema {
	panic("should not be called")
}

func targetName(tgt *querypb.Target) string {
	return fmt.Sprintf("%s:%s:%s", tgt.Keyspace, tgt.Shard, tgt.TabletType.String())
}

func (ex *Executor) target(target *querypb.Target) *memTarget {
	ft, ok := ex.targets[targetName(target)]
	if !ok {
		ex.t.Fatalf("missing target %+v", target)
	}
	return ft
}

func (ex *Executor) TestExecute(sqlwithparams string, args ...any) *sqltypes.Result {
	var sql = fmt.Sprintf(sqlwithparams, args...)
	for _, tgt := range ex.targets {
		if tgt.target.TabletType == topodatapb.TabletType_PRIMARY {
			res, err := ex.target(tgt.target).execute(sql, nil)
			require.NoError(ex.t, err)
			return res
		}
	}
	ex.t.Fatalf("no primary found")
	return nil
}

func (ex *Executor) TestAlterRecipe(recipe *testrecipe.Recipe, keyspace, ddl string) {
	for _, tgt := range ex.targets {
		if tgt.target.Keyspace != keyspace {
			continue
		}
		executor := ex.target(tgt.target)
		_, err := executor.execute(ddl, nil)
		require.NoError(ex.t, err)

		recipe.DDL[keyspace] = executor.getSchema()
	}
	recipe.Update(ex.t)
}

func (ex *Executor) TestApplyRecipe(recipe *testrecipe.Recipe) {
	for keyspace, ddls := range recipe.DDL {
		for _, tgt := range ex.targets {
			if tgt.target.Keyspace == keyspace {
				for _, ddl := range ddls {
					_, err := ex.target(tgt.target).execute(ddl, nil)
					require.NoError(ex.t, err)
				}
			}
		}
	}
}

func (ex *Executor) TestUpdateTopoServer(ts *topo.Server, cell string) {
	var tabletuid = uint32(1)
	var shardsForKeyspace = make(map[string][]string)

	for _, t := range ex.targets {
		ks := t.target.Keyspace
		shard := t.target.Shard
		shardsForKeyspace[ks] = append(shardsForKeyspace[ks], shard)
	}

	for ks, shards := range shardsForKeyspace {
		if err := ts.CreateKeyspace(context.Background(), ks, &topodatapb.Keyspace{}); err != nil {
			ex.t.Fatalf("failed to create Keyspace: %v", err)
		}
		for _, shard := range shards {
			if err := ts.CreateShard(context.Background(), ks, shard); err != nil {
				ex.t.Fatalf("failed to create Shard: %v", err)
			}
		}
	}

	for _, t := range ex.targets {
		tablet := &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: cell,
				Uid:  tabletuid,
			},
			Hostname:             fmt.Sprintf("tablet-%d", tabletuid),
			Keyspace:             t.target.Keyspace,
			Shard:                t.target.Shard,
			Type:                 t.target.TabletType,
			PrimaryTermStartTime: protoutil.TimeToProto(time.Now()),
		}

		err := ts.CreateTablet(context.Background(), tablet)
		if err != nil {
			ex.t.Fatalf("failed to create Tablet: %v", err)
		}

		if tablet.Type == topodatapb.TabletType_PRIMARY {
			_, err := ts.UpdateShardFields(context.Background(), tablet.Keyspace, tablet.Shard, func(si *topo.ShardInfo) error {
				if si.IsPrimaryServing && si.PrimaryAlias != nil {
					return fmt.Errorf("shard %v/%v already has a serving primary (%v)", tablet.Keyspace, tablet.Shard, topoproto.TabletAliasString(si.PrimaryAlias))
				}

				si.PrimaryAlias = tablet.Alias
				si.IsPrimaryServing = true
				si.PrimaryTermStartTime = tablet.PrimaryTermStartTime
				return nil
			})
			require.NoError(ex.t, err, "UpdateShardFields(%s, %s) to set %s as serving primary failed", tablet.Keyspace, tablet.Shard, topoproto.TabletAliasString(tablet.Alias))
		}

		tabletuid++
	}
	ex.t.Logf("registered %d tablets (%d keyspaces) in topology server", len(ex.targets), len(shardsForKeyspace))
}

func (ex *Executor) Keyspace() string {
	return ex.keyspace
}

var _ domain.Executor = (*Executor)(nil)
