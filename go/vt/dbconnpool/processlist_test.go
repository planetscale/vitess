/*
Copyright 2023 The Vitess Authors.

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

package dbconnpool

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TestGetRedactedProcessList tests the functionality of getRedactedList
func TestGetRedactedProcessList(t *testing.T) {
	tests := []struct {
		name   string
		result *sqltypes.Result
		want   *sqltypes.Result
	}{
		{
			name: "test pass through",
			result: &sqltypes.Result{
				Fields: []*querypb.Field{{
					Name: "ID",
					Type: sqltypes.Int32,
				}, {
					Name: "TIME",
					Type: sqltypes.Time,
				}, {
					Name: "STATE",
					Type: sqltypes.VarChar,
				}, {
					Name: "INFO",
					Type: sqltypes.VarChar,
				}},
				Rows: [][]sqltypes.Value{{
					sqltypes.NewInt32(5),
					sqltypes.NewTime("1624449"),
					sqltypes.NewVarChar("Query"),
					sqltypes.NewVarChar("select `name` from `user`"),
				}},
			},
		}, {
			name: "test redaction",
			result: &sqltypes.Result{
				Fields: []*querypb.Field{{
					Name: "ID",
					Type: sqltypes.Int32,
				}, {
					Name: "TIME",
					Type: sqltypes.Time,
				}, {
					Name: "STATE",
					Type: sqltypes.VarChar,
				}, {
					Name: "INFO",
					Type: sqltypes.VarChar,
				}},
				Rows: [][]sqltypes.Value{{
					sqltypes.NewInt32(5),
					sqltypes.NewTime("1624449"),
					sqltypes.NewVarChar("Query"),
					sqltypes.NewVarChar("select name from users where id = 32"),
				}},
			},
			want: &sqltypes.Result{
				Fields: []*querypb.Field{{
					Name: "ID",
					Type: sqltypes.Int32,
				}, {
					Name: "TIME",
					Type: sqltypes.Time,
				}, {
					Name: "STATE",
					Type: sqltypes.VarChar,
				}, {
					Name: "INFO",
					Type: sqltypes.VarChar,
				}},
				Rows: [][]sqltypes.Value{{
					sqltypes.NewInt32(5),
					sqltypes.NewTime("1624449"),
					sqltypes.NewVarChar("Query"),
					sqltypes.NewVarChar("select `name` from users where id = :id /* INT64 */"),
				}},
			},
		}, {
			name: "test fail parsing",
			result: &sqltypes.Result{
				Fields: []*querypb.Field{{
					Name: "ID",
					Type: sqltypes.Int32,
				}, {
					Name: "TIME",
					Type: sqltypes.Time,
				}, {
					Name: "STATE",
					Type: sqltypes.VarChar,
				}, {
					Name: "INFO",
					Type: sqltypes.VarChar,
				}},
				Rows: [][]sqltypes.Value{{
					sqltypes.NewInt32(5),
					sqltypes.NewTime("1624449"),
					sqltypes.NewVarChar("Query"),
					sqltypes.NewVarChar("error parsing"),
				}},
			},
			want: &sqltypes.Result{
				Fields: []*querypb.Field{{
					Name: "ID",
					Type: sqltypes.Int32,
				}, {
					Name: "TIME",
					Type: sqltypes.Time,
				}, {
					Name: "STATE",
					Type: sqltypes.VarChar,
				}, {
					Name: "INFO",
					Type: sqltypes.VarChar,
				}},
				Rows: [][]sqltypes.Value{{
					sqltypes.NewInt32(5),
					sqltypes.NewTime("1624449"),
					sqltypes.NewVarChar("Query"),
					sqltypes.NewVarChar("error encountered in redacting query: Code: INVALID_ARGUMENT\nsyntax error at position 6 near 'error'\n"),
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new fakesqldb and add a query for the one getRedactedProcessList will run.
			db := fakesqldb.New(t)
			defer db.Close()
			db.AddQuery(fmt.Sprintf(showProcessListQuery, "test_user"), tt.result)

			// Get the connection parameters and create a pooledConn.
			cp := db.ConnParams()
			conn, err := cp.Connect(context.Background())
			require.NoError(t, err)
			pooledConn := &PooledDBConnection{
				DBConnection: &DBConnection{
					Conn: conn,
				},
			}

			// Verify output
			got := getRedactedProcessList(pooledConn, "test_user")
			if tt.want == nil {
				tt.want = tt.result
			}
			wantStr, err := json.MarshalIndent(tt.want, "", "\t")
			require.NoError(t, err)
			require.Equal(t, string(wantStr), got)
		})
	}
}
