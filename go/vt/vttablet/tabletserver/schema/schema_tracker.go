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

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

const (
	GetAllTrackedVersions = "select id, pos, ddl, schema, time_updated from _vt.schema_tracking order by id desc"
	GetTrackedVersion     = "select id, pos, ddl, schema, time_updated from _vt.schema_tracking where pos = '%s'"
	InsertTrackedVersion  = "insert ignore into _vt.schema_tracking (pos, ddl, schema, time_updated) values ('%s', '%s', '%s', %d)"
)

var muLock sync.Mutex

type TrackedSchema struct {
	schema map[string]*Table
	pos    mysql.Position
	ddl    string
}

var CachedSchema TrackedSchema

func (ts *TrackedSchema) String() string {
	return fmt.Sprintf("Schema: at pos %s for ddl %s", ts.pos, ts.ddl)
}

type Tracker struct {
	se                  *Engine
	trackSchemaVersions bool
	schemas             []*TrackedSchema
}

func NewTracker(se *Engine) (*Tracker, error) {
	st := &Tracker{
		se:                  se,
		trackSchemaVersions: se.env.Config().TrackSchemaVersions,
	}
	log.Infof("NewTracker returning %s", st)
	return st, nil
}

func (st *Tracker) ReloadForPos(ctx context.Context, pos mysql.Position, ddl string) (bool, error) { //TODO bool returned for testing, remove?
	cached, err := st.getCached(ctx, pos, ddl)
	if err != nil {
		return false, nil
	}
	if cached == true {
		return true, nil
	}
	if err := st.se.Reload(ctx); err != nil {
		return false, err
	}
	if err := st.store(ctx, pos, ddl, st.se.GetSchema()); err != nil {
		return false, err
	}

	return false, nil
}

func (st *Tracker) String() string {
	var s string
	s = "Schema Tracker:"
	if !CachedSchema.pos.IsZero() {
		s += fmt.Sprintf(" Cached schema as of %s.", CachedSchema.pos.String())
	}
	if !st.trackSchemaVersions {
		s += " Not tracking schemas, only caching latest."
	} else {
		s += " Tracking schemas."
	}
	return s
}

func (st *Tracker) DebugString() string {
	var s string
	s = "Schema Tracker:"
	if !CachedSchema.pos.IsZero() {
		s += fmt.Sprintf(" Cached schema as of %s.", CachedSchema.pos.String())
	}
	if !st.trackSchemaVersions {
		s += " Not tracking schemas, only caching latest."
	} else {
		s += "\n Tracked schemas are: \n"
		for _, ts := range st.schemas {
			s += "\t" + ts.pos.String() + "\n"
		}
	}
	return s
}

func (st *Tracker) open(ctx context.Context) error {
	if !st.trackSchemaVersions {
		return nil
	}
	if err := st.loadAllFromDB(ctx); err != nil {
		return err
	}

	return nil
}

func (st *Tracker) updateEngineSchema(trackedSchema *TrackedSchema) {
	tables := make(map[string]*Table)
	for k, v := range trackedSchema.schema {
		tables[k] = v
	}
	st.se.mu.Lock()
	defer st.se.mu.Unlock()
	st.se.tables = tables
}

func (st *Tracker) getCached(ctx context.Context, pos mysql.Position, ddl string) (bool, error) {
	if CachedSchema.pos.Equal(pos) {
		muLock.Lock() //TODO: two locks will be held here: take a copy and unlock earlier?
		defer muLock.Unlock()
		log.Infof("ReloadForPos: schema is already recent for pos %s, ddl %s", pos, ddl)
		st.updateEngineSchema(&CachedSchema)
		return true, nil
	}
	if !st.trackSchemaVersions {
		return false, nil
	}
	if len(st.schemas) == 0 {
		log.Infof("No tracked schemas saved yet, loading for %s", pos)
	} else {
		idx := sort.Search(len(st.schemas), func(i int) bool {
			return !pos.AtLeast(st.schemas[i].pos)
		})
		if idx >= len(st.schemas) || idx == 0 && !st.schemas[idx].pos.Equal(pos) {
			log.Infof("Tracked schema not found for %s", pos)
		} else {
			log.Infof("Found tracked schema. Looking for %s, found %s", pos, st.schemas[idx])
			st.updateEngineSchema(st.schemas[idx])
			return true, nil
		}
	}
	exists, err := st.loadOneFromDB(ctx, pos)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	return true, nil
}

func (st *Tracker) cache(pos mysql.Position, ddl string, schema map[string]*Table) {
	log.Infof("Caching schema globally for pos %s, ddl %s", pos, ddl)
	muLock.Lock()
	defer muLock.Unlock()
	CachedSchema.schema = schema
	CachedSchema.pos = pos
	CachedSchema.ddl = ddl
}

func (st *Tracker) sortSchemas() {
	sort.Slice(st.schemas, func(i int, j int) bool {
		return st.schemas[j].pos.AtLeast(st.schemas[i].pos)
	})
}

func createSchemaTrackingTable() string {
	query := `CREATE TABLE IF NOT EXISTS _vt.schema_tracking (
  id INT AUTO_INCREMENT,
  pos VARBINARY(10000) NOT NULL,
  time_updated BIGINT(20) NOT NULL,
  ddl VARBINARY(1000) DEFAULT NULL,
  schema LONGBLOB(10000) NOT NULL,
  PRIMARY KEY (gtid)
) ENGINE=InnoDB`
	return query
}

func (st *Tracker) store(ctx context.Context, pos mysql.Position, ddl string, schema map[string]*Table) error {
	log.Infof("Storing schema for pos %s, ddl %s %t", pos, ddl, st.trackSchemaVersions)
	st.cache(pos, ddl, schema)
	if st.trackSchemaVersions {
		st.schemas = append(st.schemas, &TrackedSchema{schema, pos, ddl})
		st.sortSchemas()
		st.storeInDB(ctx, pos, ddl, schema)
	}
	return nil
}

func (st *Tracker) storeInDB(ctx context.Context, pos mysql.Position, ddl string, schema map[string]*Table) error {
	log.Infof("Storing in db %s", pos)
	conn, err := st.se.conns.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	jsonSchema, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(InsertTrackedVersion, pos, ddl, jsonSchema, time.Now().Unix())
	_, err = conn.Exec(ctx, query, 1, false)
	if err != nil {
		log.Errorf("Error inserting into schema_tracking table %v", err)
		return err
	} else {
		log.Infof("Success inserting into schema_tracking table pos %s", pos)
	}
	log.Infof("Stored in db %s", pos)
	return nil
}

func (st *Tracker) loadAllFromDB(ctx context.Context) error {
	conn, err := st.se.conns.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	log.Infof("Creating schema tracking table")
	_, err = conn.Exec(ctx, createSchemaTrackingTable(), 1, false)
	if err != nil {
		log.Errorf("Error creating schema_tracking table %v", err)
		return err
	}
	tableData, err := conn.Exec(ctx, GetAllTrackedVersions, 1000, false)
	if err != nil {
		log.Errorf("Error reading schema_tracking table %v", err)
		return err
	}
	log.Infof("Number of rows is %d", tableData.Rows)
	schemas := make([]*TrackedSchema, 0)
	for _, row := range tableData.Rows {
		trackedSchema, err := st.readRow(row)
		if err != nil {
			return err
		}
		schemas = append(schemas, trackedSchema)
	}
	muLock.Lock()
	defer muLock.Unlock()
	st.schemas = schemas
	st.sortSchemas()
	return nil
}

func (st *Tracker) readRow(row []sqltypes.Value) (*TrackedSchema, error) {
	var tables map[string]*Table
	id, _ := evalengine.ToInt64(row[0])
	pos, err := mysql.DecodePosition(string(row[1].ToBytes()))
	if err != nil {
		return nil, err
	}
	ddl := string(row[2].ToBytes())
	if err := json.Unmarshal(row[3].ToBytes(), &tables); err != nil {
		return nil, err
	}
	timeUpdated, _ := evalengine.ToInt64(row[4])
	log.Infof("Read tracked schema from db: id %d, pos %v, ddl %s, schema len %d, time_updated %d \n", id, pos, ddl, len(tables), timeUpdated)
	trackedSchema := &TrackedSchema{
		schema: tables,
		pos:    pos,
		ddl:    ddl,
	}
	return trackedSchema, nil
}

func (st *Tracker) loadOneFromDB(ctx context.Context, pos mysql.Position) (bool, error) {
	conn, err := st.se.conns.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Recycle()

	tableData, err := conn.Exec(ctx, fmt.Sprintf(GetTrackedVersion, pos), 1, false)
	if err != nil {
		log.Errorf("Error reading schema_tracking table %v", err)
		return false, err
	}
	if len(tableData.Rows) == 1 {
		trackedSchema, err := st.readRow(tableData.Rows[0])
		if err != nil {
			return false, err
		}
		muLock.Lock()
		defer muLock.Unlock()
		st.schemas = append(st.schemas, trackedSchema)
		st.sortSchemas()
		return true, nil
	}
	return false, nil
}
