package packet

import (
	"encoding/json"
	"fmt"
	"strings"

	"vitess.io/vitess/go/boost/sql"
)

type DebugRecords []sql.Record

func (records DebugRecords) parts() []string {
	var out []string
	var remainder int
	if len(records) > 10 {
		remainder = len(records) - 10
		records = records[:10]
	}
	for _, record := range records {
		if record.Positive {
			out = append(out, "+"+record.Row.String())
		} else {
			out = append(out, "-"+record.Row.String())
		}
	}
	if remainder > 0 {
		out = append(out, fmt.Sprintf("[...] +%d records", remainder))
	}
	return out
}

func (records DebugRecords) MarshalJSON() ([]byte, error) {
	return json.Marshal(records.parts())
}

func (records DebugRecords) String() string {
	return strings.Join(records.parts(), ", ")
}

type DebugRows []sql.Row

func (rows DebugRows) parts() []string {
	var out []string
	var remainder int
	if len(rows) > 10 {
		remainder = len(rows) - 10
		rows = rows[:10]
	}
	for _, r := range rows {
		out = append(out, r.String())
	}
	if remainder > 0 {
		out = append(out, fmt.Sprintf("[...] +%d rows", remainder))
	}
	return out
}

func (rows DebugRows) MarshalJSON() ([]byte, error) {
	return json.Marshal(rows.parts())
}

func (rows DebugRows) String() string {
	return strings.Join(rows.parts(), ", ")
}
