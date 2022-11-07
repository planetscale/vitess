package testexecutor

import (
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/go-mysql-server/sql"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func init() {
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
	subs   map[chan []*binlogdatapb.VEvent]map[string]struct{}
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

	ts := time.Now()
	events := make([]*binlogdatapb.VEvent, 3)
	events[1] = &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_GTID,
		Timestamp: ts.Unix(),
		Gtid:      mysql.Mysql56FlavorID + "/" + gt.current(),
		Keyspace:  log.Database,
		Shard:     gt.shard,
	}
	events[2] = &binlogdatapb.VEvent{
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
	}
	gtid := mysql.Mysql56GTID{
		Server:   gt.sid,
		Sequence: gt.sequence,
	}

	gt.txlog = append(gt.txlog, events[1], events[2])
	gt.seqlog = append(gt.seqlog, gtid, gtid)

	tname := log.Database + "." + log.Table
	for sub, seenFields := range gt.subs {
		if _, seen := seenFields[tname]; !seen {
			seenFields[tname] = struct{}{}
			if events[0] == nil {
				events[0] = &binlogdatapb.VEvent{
					Type:      binlogdatapb.VEventType_FIELD,
					Timestamp: ts.Unix(),
					FieldEvent: &binlogdatapb.FieldEvent{
						TableName: log.Table,
						Fields:    schemaToVitess(log.Schema),
						Keyspace:  log.Database,
						Shard:     gt.shard,
					},
					Keyspace: log.Database,
					Shard:    gt.shard,
				}
			}
			sub <- events
		} else {
			sub <- events[1:]
		}
	}
	gt.sequence++
	return nil
}

func (gt *GTIDTracker) Subscribe(startpos string, sub chan []*binlogdatapb.VEvent) error {
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
				sub <- gt.txlog[pos:]
				break
			}
		}
	}

	gt.subs[sub] = map[string]struct{}{}
	return nil
}

func (gt *GTIDTracker) Unsubscribe(sub chan []*binlogdatapb.VEvent) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	delete(gt.subs, sub)
}

func (gt *GTIDTracker) current() string {
	return fmt.Sprintf("%s:%d-%d", gt.sid.String(), 1, gt.sequence)
}
