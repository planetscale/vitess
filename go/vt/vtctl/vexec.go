/*
Copyright 2020 The Vitess Authors.

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

package vtctl

import (
	"bytes"
	"context"
	"encoding/json"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	//"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/TylerBrock/colorjson"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/wrangler"
)

type CommandRunner struct {
	Context context.Context
	Workflow    string
	Keyspace    string
	Shard       string
	TabletAlias string

	ts *topo.Server
	wr *wrangler.Wrangler

	Masters []*topo.TabletInfo
}

var timeout = time.Duration(10 * time.Second) //FIXME: flag

// NewCommandRunner returns a validated and prepared VExec object
func NewCommandRunner(workflow, keyspace, shard, tabletAlias string) (*CommandRunner, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ts := topo.Open()
	defer ts.Close()

	//fixme vtctl.WorkflowManager = workflow.NewManager(ts)

	//fixme installSignalHandlers(cancel)
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	runner := &CommandRunner{
		Context: ctx,
		Workflow:    workflow,
		Keyspace:    keyspace,
		Shard:       shard,
		TabletAlias: tabletAlias,
		ts:          wr.TopoServer(),
		wr:          wr,
	}

	if err := runner.build(ctx); err != nil {
		return nil, err
	}
	return runner, nil
}

func (cr *CommandRunner) setStatus(status string) error {
	if _, err := cr.exec(cr.Context, fmt.Sprintf("update _vt.vreplication set state = %s", status)); err != nil {
		return err
	}
	return nil
}

func (cr *CommandRunner) StopStreams() error {
	return cr.setStatus( "Stopped")
}

func (cr *CommandRunner) StartStreams() error {
	return cr.setStatus( "Running")
}

func (cr *CommandRunner) getMastersForKeyspace(ctx context.Context, keyspace string) ([]*topo.TabletInfo, error) {
	var err error
	shards, err := cr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("no shards found in keyspace %s", cr.Keyspace)
	}
	var allMasters []*topo.TabletInfo
	var master *topo.TabletInfo
	for _, shard := range shards {
		if master, err = cr.getMasterForShard(ctx, shard); err != nil {
			return nil, err
		}
		if master == nil {
			return nil, fmt.Errorf("no master found for shard %s", shard)
		}
		allMasters = append(allMasters, master)
	}
	return allMasters, nil
}

func (cr *CommandRunner) exec(ctx context.Context, query string) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	var wg sync.WaitGroup
	workflow := cr.Workflow
	allErrors := &concurrency.AllErrorRecorder{}
	results := make(map[*topo.TabletInfo]*querypb.QueryResult)
	var mu sync.Mutex
	for _, master := range cr.Masters {
		wg.Add(1)
		go func(workflow string, master *topo.TabletInfo, query string) {
			defer wg.Done()
			if !strings.Contains(query, " where ") {
				query += " where "
			}
			query = fmt.Sprintf("%s workflow = %s and db_name = %s", query, encodeString(workflow), encodeString(master.DbName()))
			qr, err := cr.wr.ExecuteFetchAsApp(ctx, master.Alias, true, query, 10000)
			if err != nil {
				allErrors.RecordError(err)
			} else {
				if qr.Rows == nil || len(qr.Rows) == 0 {
					allErrors.RecordError(fmt.Errorf("workflow %s not found in tablet %s", workflow, master.Alias)) //fixme encodeString everywhere
				} else {
					mu.Lock()
					results[master] = qr
					mu.Unlock()
				}
			}

		}(workflow, master, query)
	}
	wg.Wait()
	return results, allErrors.AggrError(vterrors.Aggregate)

}

func (cr *CommandRunner) build(ctx context.Context) error {
	localCtx, cancel := context.WithTimeout(ctx, timeout)
	var masters []*topo.TabletInfo
	var err error
	defer cancel()
	
	if cr.Keyspace == "" || cr.Workflow == "" {
		return fmt.Errorf("keyspace and worfklow have to be specified")
	}
	if cr.Shard == "" && cr.TabletAlias == "" {
		if masters, err = cr.getMastersForKeyspace(localCtx, cr.Keyspace); err != nil {
			return err
		}
	} else if cr.Shard != "" && cr.TabletAlias == "" {
		master, err := cr.getMasterForShard(ctx, cr.Shard)
		if err != nil {
			return err
		}
		masters = append(masters, master)
	}
	if len(masters) == 0 {
		fmt.Errorf("no tablets found for given parameters")
	}
	cr.Masters = masters

	query := fmt.Sprintf("select id, state from _vt.vreplication")
	_, err = cr.exec(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func (cr *CommandRunner) getMasterForShard(ctx context.Context, shard string) (*topo.TabletInfo, error) {
	si, err := cr.ts.GetShard(ctx, cr.Keyspace, shard)
	if err != nil {
		return nil, err
	}
	if si.MasterAlias == nil {
		return nil, fmt.Errorf("no master found for shard %s", shard)
	}
	master, err := cr.ts.GetTablet(ctx, si.MasterAlias)
	if err != nil {
		return nil, err
	}
	if master == nil {
		return nil, fmt.Errorf("could not get tablet for %s:%s", cr.Keyspace, si.MasterAlias)
	}
	return master, nil
}

type replicationStatusResult struct {
	Workflow string
	SourceKeyspace string
	TargetKeyspace string

	Statuses []replicationStatus
}

type copyState struct {
	Table      string
	LastPK     string
	RowsCopied int64 //fixme use info schema
}

type replicationStatus struct {
	Shard             string
	Tablet            string
	ID                int64
	Bls               binlogdatapb.BinlogSource
	Pos               string
	StopPos           string
	State             string
	MaxReplicationLag int64
	DBName            string
	TimeUpdated       int64
	Message           string

	CopyState []copyState
}

func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}

func (cr *CommandRunner) getCopyState(ctx context.Context, workflow, dbName string) ([]copyState, error) {
	var cs []copyState
	query := fmt.Sprintf(`select table_name, lastpk from _vt.copy_state where vrepl_id in (select id from _vt.vreplication where workflow = "%s" and db_name = "%s" )`, workflow, dbName)
	//fmt.Println(query)
	result, err := cr.execQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	if result != nil {
		qr := sqltypes.Proto3ToResult(result)
		for _, row := range qr.Rows {
			table := row[0].ToString()
			lastPK := row[1].ToString()
			copyState := copyState{
				Table:  table,
				LastPK: lastPK,
			}
			query = fmt.Sprintf("select count(*) from %s", table)
			var p3qr *querypb.QueryResult
			p3qr, err = cr.execQuery(ctx, query)
			if err != nil {
				return nil, err
			}
			qr2 := sqltypes.Proto3ToResult(p3qr)
			if len(qr2.Rows) == 0 {
				return nil, fmt.Errorf("error getting row count of %s", table)
			}
			var rowsCopied int64
			if rowsCopied, err = evalengine.ToInt64(row[0]); err != nil {
				return nil, err
			}
			copyState.RowsCopied = rowsCopied

			cs = append(cs, copyState)
		}
	}

	return cs, nil
}

// GetWorkflowStatus shows status of all vreplication streams for a workflow
func (cr *CommandRunner) GetWorkflowStatus(ctx context.Context) error {
	var replStatus replicationStatusResult
	replStatus.Workflow = cr.Workflow
	replStatus.TargetKeyspace = cr.Keyspace
	var err error
	var results map[*topo.TabletInfo]*querypb.QueryResult
	query := fmt.Sprintf("select id, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, message from _vt.vreplication")
	if results, err = cr.exec(ctx, query); err != nil {
		return err
	}
	for master, result := range results {
		//fmt.Printf("master %s, result %v\n", master.Alias, result)
		qr := sqltypes.Proto3ToResult(result)
		var id, maxReplicationLag, timeUpdated int64
		var state, dbName, pos, stopPos, message string
		var bls binlogdatapb.BinlogSource
		for _, row := range qr.Rows {
			id, err = evalengine.ToInt64(row[0])
			if err != nil {
				return err
			}
			if err := proto.UnmarshalText(row[1].ToString(), &bls); err != nil {
				return err
			}
			replStatus.SourceKeyspace = bls.Keyspace
			pos = row[2].ToString()
			stopPos = row[3].ToString()
			maxReplicationLag, err = evalengine.ToInt64(row[4])
			if err != nil {
				return err
			}
			state = row[5].ToString()
			dbName = row[6].ToString()
			timeUpdated, err = evalengine.ToInt64(row[7])
			if err != nil {
				return err
			}
			message = row[8].ToString()
			//fmt.Printf("%d: %s\n", id, state)
		}
		status := replicationStatus{
			Shard:             master.Shard,
			Tablet:            master.AliasString(),
			ID:                id,
			Bls:               bls,
			Pos:               pos,
			StopPos:           stopPos,
			State:             state,
			DBName:            dbName,
			MaxReplicationLag: maxReplicationLag,
			TimeUpdated:       timeUpdated,
			Message:           message,
		}
		status.CopyState, err = cr.getCopyState(ctx, cr.Workflow, dbName)
		if err != nil {
			return err
		}
		if message != "" {
			status.State = "Error"
		} else if status.State == "Running" && int64(time.Now().Second())-timeUpdated > 10 /* seconds */ {
			status.State = "Lagging"
		} else if status.State == "Running" && len(status.CopyState) > 0 {
			status.State = "Copying"
		}

		replStatus.Statuses = append(replStatus.Statuses, status)
	}
	text, err := json.MarshalIndent(replStatus, "", "\t")
	if err != nil {
		return err
	}
	var obj map[string]interface{}
	json.Unmarshal([]byte(text), &obj)
	f := colorjson.NewFormatter()
	f.Indent = 4
	text2, err := f.Marshal(obj)
	if err != nil {
		fmt.Errorf("%s", err)
	}
	cr.wr.Logger().Printf("%s\n", text2)
	return nil
}

func (cr *CommandRunner) VExec(query string) (errors []string) {
	return nil
}

func (cr *CommandRunner) execQuery(ctx context.Context, query string) (*querypb.QueryResult, error) {

}
