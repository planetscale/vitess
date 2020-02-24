package vstream

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	_ "vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	planbuilder "vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
)

var PacketSize int
var HeartbeatTime = 900 * time.Millisecond
var vschemaUpdateCount sync2.AtomicInt64

var (
	ctx    = context.Background()
	cancel func()

	cp             *mysql.ConnParams
	se             *schema.Engine
	startPos       string
	filter         *binlogdatapb.Filter
	send           func([]*binlogdatapb.VEvent) error
	plans          map[uint64]*streamerPlan
	journalTableID uint64

	// format and pos are updated by parseEvent.
	format mysql.BinlogFormat
	pos    mysql.Position
)

type streamerPlan struct {
	*planbuilder.Plan
	TableMap *mysql.TableMap
}

func TestVstreamReplication(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host:  "127.0.0.1",
		Port:  11000,
		Uname: "ripple",
	}
	pos, err := mysql.DecodePosition("MySQL56/75e11b14-524e-11ea-bbc4-40234316aeb5:1")
	require.NoError(t, err)
	//conn, err := binlog.NewSlaveConnection(&vtParams)
	//require.NoError(t, err)
	//defer conn.Close()
	vsClient := vreplication.NewMySQLVStreamerClientWithConn(vtParams.Host, vtParams.Host, vtParams.Port)
	err = vsClient.Open(ctx)
	require.NoError(t, err)
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}

	_ = vsClient.VStream(ctx, mysql.EncodePosition(pos), filter, func(events []*binlogdatapb.VEvent) error {
		println("%v", events)
		return nil
	})

	//
	//
	//events, err := conn.StartBinlogDumpFromPosition(context.Background(), pos)
	//require.NoError(t, err)
	//
	//assert.NotNil(t, events)
	//vstreamEvents(context.Background(), events)
}

func vstreamEvents(ctx context.Context, events <-chan mysql.BinlogEvent) error {
	var (
		bufferedEvents []*binlogdatapb.VEvent
		curSize        int
	)
	// Buffering only takes row lengths into consideration.
	// Length of other events is considered negligible.
	// If a new row event causes the packet size to be exceeded,
	// all existing rows are sent without the new row.
	// If a single row exceeds the packet size, it will be in its own packet.
	bufferAndTransmit := func(vevent *binlogdatapb.VEvent) error {
		switch vevent.Type {
		case binlogdatapb.VEventType_GTID, binlogdatapb.VEventType_BEGIN, binlogdatapb.VEventType_FIELD, binlogdatapb.VEventType_JOURNAL:
			// We never have to send GTID, BEGIN, FIELD events on their own.
			bufferedEvents = append(bufferedEvents, vevent)
		case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_DDL, binlogdatapb.VEventType_OTHER, binlogdatapb.VEventType_HEARTBEAT:
			// COMMIT, DDL, OTHER and HEARTBEAT must be immediately sent.
			bufferedEvents = append(bufferedEvents, vevent)
			vevents := bufferedEvents
			bufferedEvents = nil
			curSize = 0
			println(fmt.Sprintf("%v", vevents))
			return nil
		case binlogdatapb.VEventType_INSERT, binlogdatapb.VEventType_DELETE, binlogdatapb.VEventType_UPDATE, binlogdatapb.VEventType_REPLACE:
			newSize := len(vevent.GetDml())
			if curSize+newSize > PacketSize {
				vevents := bufferedEvents
				bufferedEvents = []*binlogdatapb.VEvent{vevent}
				curSize = newSize
				println(fmt.Sprintf("%v", vevents))
				return nil
			}
			curSize += newSize
			bufferedEvents = append(bufferedEvents, vevent)
		case binlogdatapb.VEventType_ROW:
			// ROW events happen inside transactions. So, we can chunk them.
			// Buffer everything until packet size is reached, and then send.
			newSize := 0
			for _, rowChange := range vevent.RowEvent.RowChanges {
				if rowChange.Before != nil {
					newSize += len(rowChange.Before.Values)
				}
				if rowChange.After != nil {
					newSize += len(rowChange.After.Values)
				}
			}
			if curSize+newSize > PacketSize {
				vevents := bufferedEvents
				bufferedEvents = []*binlogdatapb.VEvent{vevent}
				curSize = newSize
				println(fmt.Sprintf("%v", vevents))
				return nil
			}
			curSize += newSize
			bufferedEvents = append(bufferedEvents, vevent)
		default:
			return fmt.Errorf("unexpected event: %v", vevent)
		}
		return nil
	}

	// Main loop: calls bufferAndTransmit as events arrive.
	timer := time.NewTimer(HeartbeatTime)
	defer timer.Stop()
	for {
		timer.Reset(HeartbeatTime)
		// Drain event if timer fired before reset.
		select {
		case <-timer.C:
		default:
		}

		select {
		case ev, ok := <-events:
			if !ok {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				return fmt.Errorf("unexpected server EOF")
			}
			vevents, err := parseEvent(ev)
			if err != nil {
				return err
			}
			for _, vevent := range vevents {
				if err := bufferAndTransmit(vevent); err != nil {
					if err == io.EOF {
						return nil
					}
					return fmt.Errorf("error sending event: %v", err)
				}
			}

		case <-ctx.Done():
			return nil
		case <-timer.C:
			now := time.Now().UnixNano()
			if err := bufferAndTransmit(&binlogdatapb.VEvent{
				Type:        binlogdatapb.VEventType_HEARTBEAT,
				Timestamp:   now / 1e9,
				CurrentTime: now,
			}); err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("error sending event: %v", err)
			}
		}
	}
}

func parseEvent(ev mysql.BinlogEvent) ([]*binlogdatapb.VEvent, error) {
	// Validate the buffer before reading fields from it.
	if !ev.IsValid() {
		return nil, fmt.Errorf("can't parse binlog event: invalid data: %#v", ev)
	}

	// We need to keep checking for FORMAT_DESCRIPTION_EVENT even after we've
	// seen one, because another one might come along (e.g. on log rotate due to
	// binlog settings change) that changes the format.
	if ev.IsFormatDescription() {
		var err error
		format, err = ev.Format()
		if err != nil {
			return nil, fmt.Errorf("can't parse FORMAT_DESCRIPTION_EVENT: %v, event data: %#v", err, ev)
		}
		return nil, nil
	}

	// We can't parse anything until we get a FORMAT_DESCRIPTION_EVENT that
	// tells us the size of the event header.
	if format.IsZero() {
		// The only thing that should come before the FORMAT_DESCRIPTION_EVENT
		// is a fake ROTATE_EVENT, which the master sends to tell us the name
		// of the current log file.
		if ev.IsRotate() {
			return nil, nil
		}
		return nil, fmt.Errorf("got a real event before FORMAT_DESCRIPTION_EVENT: %#v", ev)
	}

	// Strip the checksum, if any. We don't actually verify the checksum, so discard it.
	ev, _, err := ev.StripChecksum(format)
	if err != nil {
		return nil, fmt.Errorf("can't strip checksum from binlog event: %v, event data: %#v", err, ev)
	}
	var vevents []*binlogdatapb.VEvent
	switch {
	case ev.IsGTID():
		gtid, hasBegin, err := ev.GTID(format)
		if err != nil {
			return nil, fmt.Errorf("can't get GTID from binlog event: %v, event data: %#v", err, ev)
		}
		if hasBegin {
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_BEGIN,
			})
		}
		pos = mysql.AppendGTID(pos, gtid)
	case ev.IsXID():
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_GTID,
			Gtid: mysql.EncodePosition(pos),
		}, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_COMMIT,
		})
	case ev.IsQuery():
		q, err := ev.Query(format)
		println("_----------------------")
		println(fmt.Sprintf("%v", q))
		println(q.SQL)
		println(fmt.Sprintf("Vevents %v", vevents))
		println("-----------------------")
		if err != nil {
			return nil, fmt.Errorf("can't get query from binlog event: %v, event data: %#v", err, ev)
		}

	case ev.IsTableMap():
		// This is very frequent. It precedes every row event.
		id := ev.TableID(format)
		println(id)
		plan := plans[id]
		if plan == nil {
			return nil, nil
		}
		rows, err := ev.Rows(format, plan.TableMap)
		if err != nil {
			return nil, err
		}
		if id == journalTableID {
			println("Journal event not handled")
		} else {
			vevents, err = processRowEvent(vevents, plan, rows)
		}
		if err != nil {
			return nil, err
		}

	case ev.IsWriteRows() || ev.IsDeleteRows() || ev.IsUpdateRows():
		// The existence of before and after images can be used to
		// identify statememt types. It's also possible that the
		// before and after images end up going to different shards.
		// If so, an update will be treated as delete on one shard
		// and insert on the other.
		id := ev.TableID(format)
		println(id)
		if err != nil {
			return nil, err
		}
	}
	for _, vevent := range vevents {
		vevent.Timestamp = int64(ev.Timestamp())
		vevent.CurrentTime = time.Now().UnixNano()
	}
	return vevents, nil
}

func mustSendStmt(query mysql.Query, dbname string) bool {
	if query.Database != "" && query.Database != dbname {
		return false
	}
	return true
}
func mustSendDDL(query mysql.Query, dbname string, filter *binlogdatapb.Filter) bool {
	if query.Database != "" && query.Database != dbname {
		return false
	}
	ast, err := sqlparser.Parse(query.SQL)
	// If there was a parsing error, we send it through. Hopefully,
	// recipient can handle it.
	if err != nil {
		return true
	}
	switch stmt := ast.(type) {
	case *sqlparser.DBDDL:
		return false
	case *sqlparser.DDL:
		if !stmt.Table.IsEmpty() {
			return tableMatches(stmt.Table, dbname, filter)
		}
		for _, table := range stmt.FromTables {
			if tableMatches(table, dbname, filter) {
				return true
			}
		}
		for _, table := range stmt.ToTables {
			if tableMatches(table, dbname, filter) {
				return true
			}
		}
		return false
	}
	return true
}

func tableMatches(table sqlparser.TableName, dbname string, filter *binlogdatapb.Filter) bool {
	if !table.Qualifier.IsEmpty() && table.Qualifier.String() != dbname {
		return false
	}
	for _, rule := range filter.Rules {
		switch {
		case strings.HasPrefix(rule.Match, "/"):
			expr := strings.Trim(rule.Match, "/")
			result, err := regexp.MatchString(expr, table.Name.String())
			if err != nil {
				continue
			}
			if !result {
				continue
			}
			return true
		case table.Name.String() == rule.Match:
			return true
		}
	}
	return false
}

func processRowEvent(vevents []*binlogdatapb.VEvent, plan *streamerPlan, rows mysql.Rows) ([]*binlogdatapb.VEvent, error) {
	rowChanges := make([]*binlogdatapb.RowChange, 0, len(rows.Rows))
	for _, row := range rows.Rows {
		beforeOK, beforeValues, err := extractRowAndFilter(plan, row.Identify, rows.IdentifyColumns, row.NullIdentifyColumns)
		if err != nil {
			return nil, err
		}
		afterOK, afterValues, err := extractRowAndFilter(plan, row.Data, rows.DataColumns, row.NullColumns)
		if err != nil {
			return nil, err
		}
		if !beforeOK && !afterOK {
			continue
		}
		rowChange := &binlogdatapb.RowChange{}
		if beforeOK {
			rowChange.Before = sqltypes.RowToProto3(beforeValues)
		}
		if afterOK {
			rowChange.After = sqltypes.RowToProto3(afterValues)
		}
		rowChanges = append(rowChanges, rowChange)
	}
	if len(rowChanges) != 0 {
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_ROW,
			RowEvent: &binlogdatapb.RowEvent{
				TableName:  plan.Table.Name,
				RowChanges: rowChanges,
			},
		})
	}
	return vevents, nil
}

func extractRowAndFilter(plan *streamerPlan, data []byte, dataColumns, nullColumns mysql.Bitmap) (bool, []sqltypes.Value, error) {
	if len(data) == 0 {
		return false, nil, nil
	}
	values := make([]sqltypes.Value, dataColumns.Count())
	valueIndex := 0
	pos := 0
	for colNum := 0; colNum < dataColumns.Count(); colNum++ {
		if !dataColumns.Bit(colNum) {
			return false, nil, fmt.Errorf("partial row image encountered: ensure binlog_row_image is set to 'full'")
		}
		if nullColumns.Bit(valueIndex) {
			valueIndex++
			continue
		}
		value, l, err := mysql.CellValue(data, pos, plan.TableMap.Types[colNum], plan.TableMap.Metadata[colNum], plan.Table.Columns[colNum].Type)
		if err != nil {
			return false, nil, err
		}
		pos += l
		values[colNum] = value
		valueIndex++
	}
	return true, values, nil
}
