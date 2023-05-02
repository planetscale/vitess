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

package workflow

import (
	"github.com/stretchr/testify/require"
	"testing"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestGenerateInsertPart(t *testing.T) {
	stm := &SidecarTableMigrator{}
	sidecarDBName := "_vt"
	tableName := "t1"
	qr := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("1"), sqltypes.NewVarChar("val1")},
			{sqltypes.NewVarBinary("2"), sqltypes.NewVarChar("val2")},
		},
	}
	insertPart := stm.generateInsertPart(sidecarDBName, tableName, qr)
	require.NotNil(t, insertPart)
	require.Equal(t, "insert into `_vt`.`t1` (`id`, `name`)", insertPart.Query)
	valuesPart := stm.generateValuesPart(qr)
	require.NotNil(t, valuesPart)
	require.Equal(t, "(:a_id, :a_name)", valuesPart.Query)

	table := &SidecarTable{
		name:       tableName,
		insertPart: insertPart,
		valuesPart: valuesPart,
		qr:         sqltypes.ResultToProto3(qr),
	}
	query, err := stm.generateInsertQuery(table)
	require.Nil(t, err)
	require.Equal(t, "insert into `_vt`.`t1` (`id`, `name`) values (1, 'val1'), (2, 'val2')", query)
}
