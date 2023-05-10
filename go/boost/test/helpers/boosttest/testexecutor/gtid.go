package testexecutor

import (
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/go-mysql-server/sql"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

func init() {
	// enable this flag so `go-mysql-server` results match MySQL's more closely
	sql.VitessCompat = true

	// Inject a custom `gtid_executed` into the default system variables used by
	// the in-memory MySQL engine, so we can customize it per-call.
	sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
		{
			Name:              "gtid_executed",
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemStringType("gtid_executed"),
			Default:           "",
		},
	})
}

type GTIDTracker struct {
	sid      mysql.SID
	sequence int64
	shard    string

	mu     sync.Mutex
	txlog  []*binlogdatapb.VEvent
	seqlog []mysql.Mysql56GTID
	subs   []*subscription
}

type subscription struct {
	ch     chan []*binlogdatapb.VEvent
	fields map[string]sql.Schema
	shard  string
}

func (sub *subscription) updateSchema(tname string, schema sql.Schema) bool {
	currentSchema, ok := sub.fields[tname]
	if !ok || !schema.Equals(currentSchema) {
		sub.fields[tname] = schema
		return true
	}
	return false
}

func (sub *subscription) broadcast(events []*binlogdatapb.VEvent, log *sql.TransactionLog) {
	tname := log.Database + "." + log.Table

	if sub.updateSchema(tname, log.Schema) {
		fieldsEvent := &binlogdatapb.VEvent{
			Type:      binlogdatapb.VEventType_FIELD,
			Timestamp: events[0].Timestamp,
			FieldEvent: &binlogdatapb.FieldEvent{
				TableName: log.Table,
				Fields:    schemaToVitess(log.Schema),
				Keyspace:  log.Database,
				Shard:     sub.shard,
			},
			Keyspace: log.Database,
			Shard:    sub.shard,
		}

		events = append([]*binlogdatapb.VEvent{fieldsEvent}, events...)
	}

	sub.ch <- events
}

func (sub *subscription) heartbeat() {
	sub.ch <- []*binlogdatapb.VEvent{
		{
			Type:      binlogdatapb.VEventType_HEARTBEAT,
			Timestamp: time.Now().Unix(),
			Shard:     sub.shard,
		},
	}
}

func (gt *GTIDTracker) Log(ctx *sql.Context, log *sql.TransactionLog) error {
	gt.mu.Lock()
	defer gt.mu.Unlock()

	var changes []*binlogdatapb.RowChange
	for _, rev := range log.Rows {
		rowchange := &binlogdatapb.RowChange{
			Before: nil,
			After:  nil,
		}

		if rev.Before != nil {
			rowchange.Before = sqltypes.RowToProto3(rowToVitess(ctx, log.Schema, rev.Before))
		}
		if rev.After != nil {
			rowchange.After = sqltypes.RowToProto3(rowToVitess(ctx, log.Schema, rev.After))
		}

		changes = append(changes, rowchange)
	}

	gt.nextGTID()

	ts := time.Now()
	var events []*binlogdatapb.VEvent

	events = append(events, &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_GTID,
		Timestamp: ts.Unix(),
		Gtid:      mysql.Mysql56FlavorID + "/" + gt.current(),
		Keyspace:  log.Database,
		Shard:     gt.shard,
	})
	events = append(events, &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_ROW,
		Timestamp: ts.Unix(),
		RowEvent: &binlogdatapb.RowEvent{
			TableName:  log.Table,
			RowChanges: changes,
			Keyspace:   log.Database,
			Shard:      gt.shard,
		},
		Keyspace: log.Database,
		Shard:    gt.shard,
	})

	gt.txlog = append(gt.txlog, events...)

	for _, sub := range gt.subs {
		sub.broadcast(events, log)
	}

	return nil
}

func (gt *GTIDTracker) nextGTID() {
	gt.sequence++
	gtid := mysql.Mysql56GTID{Server: gt.sid, Sequence: gt.sequence}
	gt.seqlog = append(gt.seqlog, gtid, gtid)
}

func (gt *GTIDTracker) Subscribe(startpos string, ch chan []*binlogdatapb.VEvent) error {
	gt.mu.Lock()
	defer gt.mu.Unlock()

	switch startpos {
	case "", "current":
	default:
		position, err := mysql.DecodePosition(startpos)
		if err != nil {
			return err
		}

		gtidSet := position.GTIDSet.(mysql.Mysql56GTIDSet)
		for pos, seq := range gt.seqlog {
			if !gtidSet.ContainsGTID(seq) {
				ch <- gt.txlog[pos:]
				break
			}
		}
	}

	sub := &subscription{
		ch:     ch,
		fields: make(map[string]sql.Schema),
		shard:  gt.shard,
	}
	sub.heartbeat()
	gt.subs = append(gt.subs, sub)

	return nil
}

func (gt *GTIDTracker) Unsubscribe(ch chan []*binlogdatapb.VEvent) {
	gt.mu.Lock()
	defer gt.mu.Unlock()

	slices2.DeleteFunc(gt.subs, func(sub *subscription) bool {
		return sub.ch == ch
	})
}

func (gt *GTIDTracker) current() string {
	return fmt.Sprintf("%s:%d-%d", gt.sid.String(), 1, gt.sequence)
}

func (gt *GTIDTracker) emitDDL(target *querypb.Target, statement sqlparser.DDLStatement) {
	gt.mu.Lock()
	defer gt.mu.Unlock()

	for _, sub := range gt.subs {
		sub.ch <- []*binlogdatapb.VEvent{
			{
				Type:      binlogdatapb.VEventType_DDL,
				Timestamp: time.Now().Unix(),
				Statement: sqlparser.CanonicalString(statement),
				Keyspace:  target.Keyspace,
				Shard:     gt.shard,
			},
		}
	}
}
