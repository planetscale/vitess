package testexecutor

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	sqle "vitess.io/vitess/go/test/go-mysql-server"
	"vitess.io/vitess/go/test/go-mysql-server/memory"
	"vitess.io/vitess/go/test/go-mysql-server/sql"
	"vitess.io/vitess/go/test/go-mysql-server/sql/information_schema"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

type memTarget struct {
	t testing.TB

	engine *sqle.Engine
	target *querypb.Target
	db     sql.Database

	lock sync.Mutex
	gtid *GTIDTracker
}

func rowToVitess(ctx *sql.Context, s sql.Schema, row sql.Row) sqltypes.Row {
	var err error
	var o = make([]sqltypes.Value, len(row))
	for i, v := range row {
		if v == nil {
			o[i] = sqltypes.NULL
			continue
		}

		o[i], err = s[i].Type.SQL(ctx, nil, v)
		if err != nil {
			panic(err)
		}
	}
	return o
}

func rowsToVitess(ctx *sql.Context, s sql.Schema, rows []sql.Row) (vtrows []sqltypes.Row) {
	for _, row := range rows {
		if sql.IsOkResult(row) {
			continue
		}
		vtrows = append(vtrows, rowToVitess(ctx, s, row))
	}
	return
}

func schemaToVitess(s sql.Schema) []*querypb.Field {
	fields := make([]*querypb.Field, len(s))
	for i, c := range s {
		var charset uint32 = collations.CollationUtf8ID
		if sqltypes.IsBinary(c.Type.Type()) {
			charset = collations.CollationBinaryID
		}

		fields[i] = &querypb.Field{
			Name:    c.Name,
			Type:    c.Type.Type(),
			Charset: charset,
		}
	}
	return fields
}

func (target *memTarget) execute(querySQL string, variables map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	target.lock.Lock()
	defer target.lock.Unlock()

	if len(variables) > 0 {
		stmt, _, err := sqlparser.Parse2(querySQL)
		if err != nil {
			return nil, err
		}

		pquery := sqlparser.NewParsedQuery(stmt)
		querySQL, err = pquery.GenerateQuery(variables, nil)
		if err != nil {
			return nil, err
		}

		// HACK: our in-memory SQL engine does not like the backticks around `gtid_executed`
		querySQL = strings.Replace(querySQL, "@@global.`gtid_executed`", "@@global.gtid_executed", 1)
	}

	ctx := sql.NewContext(context.Background(), sql.WithServices(sql.Services{
		LogTransaction: target.gtid.Log,
	}))
	if err := ctx.SetSessionVariable(ctx, "gtid_executed", target.gtid.current()); err != nil {
		panic(err)
	}
	ctx.SetCurrentDatabase(target.target.Keyspace)

	schema, rowiter, err := target.engine.Query(ctx, querySQL)
	if err != nil {
		return nil, err
	}

	rows, err := sql.RowIterToRows(ctx, schema, rowiter)
	if err != nil {
		return nil, err
	}

	vtrows := rowsToVitess(ctx, schema, rows)
	target.t.Logf("executed query %q with %d results", querySQL, len(vtrows))

	return &sqltypes.Result{
		Fields: schemaToVitess(schema),
		Rows:   vtrows,
	}, nil
}

func (target *memTarget) vstream(ctx context.Context, pos string, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error, latency time.Duration) error {
	stream := make(chan []*binlogdatapb.VEvent, 32)

	if err := target.gtid.Subscribe(pos, stream); err != nil {
		return err
	}
	defer target.gtid.Unsubscribe(stream)

	var tableMatches map[string]struct{}
	if filter != nil && len(filter.Rules) > 0 {
		tableMatches = make(map[string]struct{})
		for _, rule := range filter.Rules {
			if strings.HasPrefix(rule.Match, "/") {
				panic("unsupported: Regexp filter rules")
			}
			tableMatches[rule.Match] = struct{}{}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case events := <-stream:
			if tableMatches != nil {
				filtered := events[:0]
				for _, ev := range events {
					var match bool
					switch ev.Type {
					case binlogdatapb.VEventType_ROW:
						_, match = tableMatches[ev.RowEvent.TableName]
					case binlogdatapb.VEventType_FIELD:
						_, match = tableMatches[ev.FieldEvent.TableName]
					default:
						match = true
					}
					if match {
						filtered = append(filtered, ev)
					}
				}
				events = filtered
			}
			if len(events) == 0 {
				continue
			}
			if latency > 0 {
				time.Sleep(latency)
			}
			if err := send(events); err != nil {
				return err
			}
		}
	}
}

func (target *memTarget) getSchema() []string {
	sqlCtx := sql.NewEmptyContext()
	tableNames, err := target.db.GetTableNames(sqlCtx)
	if err != nil {
		target.t.Fatal(err)
	}

	spec := make([]string, 0, len(tableNames))
	for _, tableName := range tableNames {
		result, err := target.execute(fmt.Sprintf("SHOW CREATE TABLE %s", sqlescape.EscapeID(tableName)), nil)
		if err != nil {
			target.t.Fatal(err)
		}
		if len(result.Rows) != 1 {
			target.t.Fatalf("not a single row result: %v", result.Rows)
		}
		row := result.Rows[0]
		if len(row) != 2 {
			target.t.Fatalf("wrong number of columns in row: %v", row)
		}

		schema := row[1].ToString()
		spec = append(spec, schema)
	}
	return spec
}

func newMemoryTarget(t testing.TB, target *querypb.Target) *memTarget {
	dbMem := memory.NewDatabase(target.Keyspace)
	engine := sqle.NewDefault(
		memory.NewMemoryDBProvider(
			dbMem,
			information_schema.NewInformationSchemaDatabase(),
		))

	db := &memTarget{
		t:      t,
		target: target,
		engine: engine,
		db:     dbMem,
		gtid: &GTIDTracker{
			sequence: 1,
			shard:    target.Shard,
			subs:     make(map[chan []*binlogdatapb.VEvent]map[string]struct{}),
		},
	}
	_, _ = rand.Read(db.gtid.sid[:])

	return db
}
