/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqltypes

import (
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// RowNamedValues contains a row's values as a map based on Field (aka table column) name
type RowNamedValues map[string]Value

// NamedResult represents a query result with named values as opposed to ordinal values.
type NamedResult struct {
	Fields       []*querypb.Field `json:"fields"`
	RowsAffected uint64           `json:"rows_affected"`
	InsertID     uint64           `json:"insert_id"`
	Rows         []RowNamedValues `json:"rows"`
}

// ToNamedResult converts a Result struct into a new NamedResult struct
func ToNamedResult(result *Result) (r *NamedResult) {
	if result == nil {
		return r
	}
	r = &NamedResult{
		Fields:       result.Fields,
		RowsAffected: result.RowsAffected,
		InsertID:     result.InsertID,
	}
	columnOrdinals := make(map[int]string)
	for i, field := range result.Fields {
		columnOrdinals[i] = field.Name
	}
	r.Rows = make([]RowNamedValues, len(result.Rows))
	for rowIndex, row := range result.Rows {
		namedRow := make(RowNamedValues)
		for i, value := range row {
			namedRow[columnOrdinals[i]] = value
		}
		r.Rows[rowIndex] = namedRow
	}
	return r
}

// Row assumes this result has exactly one row, and returns it, or else returns nil.
// It is useful for queries like:
// - select count(*) from ...
// - select @@read_only
// - select 1 from dual
func (r *NamedResult) Row() RowNamedValues {
	if len(r.Rows) != 1 {
		return nil
	}
	return r.Rows[0]
}
