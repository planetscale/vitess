package testexecutor

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/boost/server/worker"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

type Resolver struct {
	executor *Executor
}

func (f *Resolver) GetAllShards(ctx context.Context, keyspace string, tabletType topodatapb.TabletType) ([]*srvtopo.ResolvedShard, *topodatapb.SrvKeyspace, error) {
	if keyspace != f.executor.keyspace {
		return nil, nil, fmt.Errorf("unknown keyspace %q", keyspace)
	}

	var shards []*srvtopo.ResolvedShard
	for _, tgt := range f.executor.targets {
		if tgt.target.TabletType != tabletType {
			continue
		}
		shards = append(shards, &srvtopo.ResolvedShard{
			Target:  proto.Clone(tgt.target).(*querypb.Target),
			Gateway: &fakeGateway{executor: f.executor},
		})
	}
	return shards, nil, nil
}

var _ worker.Resolver = (*Resolver)(nil)

func NewResolver(g *Executor) *Resolver {
	return &Resolver{executor: g}
}

type fakeGateway struct {
	executor *Executor
}

func (f *fakeGateway) GetServingKeyspaces() []string {
	// TODO implement me
	panic("implement me")
}

func (f *fakeGateway) GetSchema(ctx context.Context, target *querypb.Target, tableType querypb.SchemaTableType, tableNames []string, callback func(schemaRes *querypb.GetSchemaResponse) error) error {
	// TODO implement me
	panic("implement me")
}

func (f *fakeGateway) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (queryservice.TransactionState, error) {
	panic("implement me")
}

func (f *fakeGateway) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	panic("implement me")
}

func (f *fakeGateway) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	panic("implement me")
}

func (f *fakeGateway) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	panic("implement me")
}

func (f *fakeGateway) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	panic("implement me")
}

func (f *fakeGateway) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	panic("implement me")
}

func (f *fakeGateway) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	panic("implement me")
}

func (f *fakeGateway) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	panic("implement me")
}

func (f *fakeGateway) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	panic("implement me")
}

func (f *fakeGateway) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	panic("implement me")
}

func (f *fakeGateway) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	panic("implement me")
}

func (f *fakeGateway) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	panic("implement me")
}

func (f *fakeGateway) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (f *fakeGateway) BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (queryservice.TransactionState, *sqltypes.Result, error) {
	panic("implement me")
}

func (f *fakeGateway) BeginStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.TransactionState, error) {
	panic("implement me")
}

func (f *fakeGateway) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (f *fakeGateway) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	panic("implement me")
}

func (f *fakeGateway) VStream(ctx context.Context, request *binlogdatapb.VStreamRequest, send func([]*binlogdatapb.VEvent) error) error {
	return f.executor.target(request.Target).vstream(ctx, request.Position, request.Filter, send, &f.executor.options)
}

func (f *fakeGateway) VStreamRows(ctx context.Context, request *binlogdatapb.VStreamRowsRequest, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	panic("implement me")
}

func (f *fakeGateway) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	panic("implement me")
}

func (f *fakeGateway) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	panic("implement me")
}

func (f *fakeGateway) HandlePanic(err *error) {
	panic("implement me")
}

func (f *fakeGateway) ReserveBeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (queryservice.ReservedTransactionState, *sqltypes.Result, error) {
	panic("implement me")
}

func (f *fakeGateway) ReserveBeginStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.ReservedTransactionState, error) {
	panic("implement me")
}

func (f *fakeGateway) ReserveExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (queryservice.ReservedState, *sqltypes.Result, error) {
	panic("implement me")
}

func (f *fakeGateway) ReserveStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.ReservedState, error) {
	panic("implement me")
}

func (f *fakeGateway) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	panic("implement me")
}

func (f *fakeGateway) Close(ctx context.Context) error {
	panic("implement me")
}

func (f *fakeGateway) QueryServiceByAlias(alias *topodatapb.TabletAlias, target *querypb.Target) (queryservice.QueryService, error) {
	panic("implement me")
}
