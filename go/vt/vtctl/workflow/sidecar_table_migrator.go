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
	"context"
	"fmt"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
)

var tablesToMigrate = []string{
	"views",
}

type SidecarTable struct {
	name       string
	insertPart *sqlparser.ParsedQuery
	valuesPart *sqlparser.ParsedQuery
	qr         *querypb.QueryResult
}

type SidecarTableMigrator struct {
	ts     ITrafficSwitcher
	logger logutil.Logger

	tables map[string]*SidecarTable
}

func (stm SidecarTableMigrator) generateInsertQuery(table *SidecarTable) (string, error) {
	log.Infof("about to generate insert query for %s", table.name)
	var sqlbuffer bytes2.Buffer
	//table := stm.tables[tableName]
	//if table == nil || table.insertPart == nil || table.valuesPart == nil || table.qr == nil {
	//	return "", fmt.Errorf("SidecarTable not initialized for %s:%v", tableName, table)
	//}
	sqlbuffer.Reset()
	sqlbuffer.WriteString(table.insertPart.Query)
	sqlbuffer.WriteString(" values ")
	for i, row := range table.qr.Rows {
		if i > 0 {
			sqlbuffer.WriteString(", ")
		}
		if err := table.valuesPart.AppendFromRow(&sqlbuffer, table.qr.Fields, row, nil); err != nil {
			return "", err
		}
	}
	return sqlbuffer.StringUnsafe(), nil
}

func (stm SidecarTableMigrator) MigrateTables(ctx context.Context) error {
	log.Infof("In MigrateTables")
	if len(stm.tables) == 0 {
		log.Infof("No sidecar tables to migrate")
		return nil
	}
	return stm.ts.ForAllTargets(func(target *MigrationTarget) error {
		for tableName, table := range stm.tables {
			log.Infof("Migrating sidecar table %s", tableName)
			query, err := stm.generateInsertQuery(table)
			if err != nil {
				return err
			}
			log.Infof("MigrateTables query is %s", query)
			tmc := stm.ts.TabletManagerClient()
			req := &tabletmanagerdata.ExecuteFetchAsDbaRequest{
				Query:   []byte(query),
				DbName:  "_vt",
				MaxRows: 10000,
			}
			_, err = tmc.ExecuteFetchAsDba(ctx, target.GetPrimary().Tablet, true, req)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (stm SidecarTableMigrator) generateInsertPart(sidecarDBName, tableName string, qr *sqltypes.Result) *sqlparser.ParsedQuery {
	insertBuf := sqlparser.NewTrackedBuffer(nil)
	insertBuf.Myprintf("insert ignore into %s.%s (", sqlescape.EscapeID(sidecarDBName), sqlescape.EscapeID(tableName))
	for i, field := range qr.Fields {
		if i > 0 {
			insertBuf.Myprintf(", ")
		}
		insertBuf.Myprintf("%s", sqlescape.EscapeID(field.Name))
	}
	insertBuf.Myprintf(")")

	return insertBuf.ParsedQuery()
}

func (stm SidecarTableMigrator) generateValuesPart(qr *sqltypes.Result) *sqlparser.ParsedQuery {
	valuesBuf := sqlparser.NewTrackedBuffer(nil)

	valuesBuf.Myprintf("(")
	for i, field := range qr.Fields {
		if i > 0 {
			valuesBuf.Myprintf(", ")
		}
		valuesBuf.WriteArg(":", fmt.Sprintf("a_%s", field.Name))
	}
	valuesBuf.Myprintf(")")

	return valuesBuf.ParsedQuery()
}
func BuildSidecarTableMigrator(ctx context.Context, ts ITrafficSwitcher) (*SidecarTableMigrator, error) {
	log.Infof("In BuildSidecarTableMigrator")
	stm := &SidecarTableMigrator{
		ts:     ts,
		logger: ts.Logger(),
		tables: make(map[string]*SidecarTable),
	}
	var err error
	if stm.ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		// sidecar tables are not migrated for table types
		log.Infof("Sidecar tables are not migrated in MoveTables")
		return stm, nil
	}
	var oneSourcePrimary *topo.TabletInfo
	for _, source := range stm.ts.Sources() {
		oneSourcePrimary = source.GetPrimary()
		break
	}
	tmc := stm.ts.TabletManagerClient()
	for _, tableName := range tablesToMigrate {
		log.Infof("building table %s", tableName)
		// todo: replace with sidecardb name
		query := fmt.Sprintf("select * from %s.%s", "_vt", tableName)
		req := &tabletmanagerdata.ExecuteFetchAsDbaRequest{
			Query:   []byte(query),
			DbName:  "_vt",
			MaxRows: 10000,
		}
		p3qr, err := tmc.ExecuteFetchAsDba(ctx, oneSourcePrimary.Tablet, true, req)
		//p3qr, err := stm.ts.TabletManagerClient().VReplicationExec(ctx, oneSourcePrimary.Tablet, query)
		if err != nil {
			log.Error(err)
			return nil, err
		}
		qr := sqltypes.Proto3ToResult(p3qr)
		if len(qr.Rows) == 0 {
			log.Infof("Table %s has no rows", tableName)
			continue
		}
		insertPart := stm.generateInsertPart(sidecardb.GetName(), tableName, qr)
		valuesPart := stm.generateValuesPart(qr)
		log.Infof("insert part is %s, values part is %s", insertPart.Query, valuesPart.Query)
		stm.tables[tableName] = &SidecarTable{
			name:       tableName,
			insertPart: insertPart,
			valuesPart: valuesPart,
			qr:         p3qr,
		}
	}

	return stm, err
}
