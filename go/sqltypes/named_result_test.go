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
	"reflect"
	"testing"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestToNamedResult(t *testing.T) {
	in := &Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: Int64,
		}, {
			Name: "status",
			Type: VarChar,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{TestValue(Int64, "1"), TestValue(VarChar, "first")},
			{TestValue(Int64, "2"), TestValue(VarChar, "second")},
			{TestValue(Int64, "3"), TestValue(VarChar, "third")},
		},
	}
	named := in.Named()
	for i := range in.Rows {
		{
			want := in.Rows[i][0]
			got := named.Rows[i]["id"]
			if !reflect.DeepEqual(want, got) {
				t.Errorf("Named:%+v\n, want:%+v\n", got, want)
			}
		}
		{
			want := in.Rows[i][1]
			got := named.Rows[i]["status"]
			if !reflect.DeepEqual(want, got) {
				t.Errorf("Named:%+v\n, want:%+v\n", got, want)
			}
		}
	}
}
