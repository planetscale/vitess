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

package tabletmanager

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// ExecuteFetchAsDba will execute the given query, possibly disabling binlogs and reload schema.
func (tm *TabletManager) ExecuteFetchAsDba(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	fmt.Printf("===== QQQ req: %+v\n", req)
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}
	// get a connection
	conn, err := tm.MysqlDaemon.GetDbaConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// disable binlogs if necessary
	if req.DisableBinlogs {
		_, err := conn.ExecuteFetch("SET sql_log_bin = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	if req.DbName != "" {
		// This execute might fail if db does not exist.
		// Error is ignored because given query might create this database.
		_, _ = conn.ExecuteFetch("USE "+sqlescape.EscapeID(req.DbName), 1, false)
	}

	uq, err := tm.SQLParser.ReplaceTableQualifiersMultiQuery(string(req.Query), sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}

	// Handle special possible directives
	// TODO(shlomi): remove in v20. v19 introduces req.AllowZeroInDate as replacement, and in v19 both work concurrently.
	var directives *sqlparser.CommentDirectives
	if stmt, err := tm.SQLParser.Parse(string(req.Query)); err == nil {
		if cmnt, ok := stmt.(sqlparser.Commented); ok {
			directives = cmnt.GetParsedComments().Directives()
		}
	}
	// TODO(shlomi): remove `directives` clause in v20. `req.AllowZeroInDate` replaces it.
	if directives.IsSet("allowZeroInDate") || req.AllowZeroInDate {
		if _, err := conn.ExecuteFetch("set @@session.sql_mode=REPLACE(REPLACE(@@session.sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')", 1, false); err != nil {
			return nil, err
		}
	}

	defer func() {
		// re-enable binlogs if necessary
		if err == nil && req.ReloadSchema {
			reloadErr := tm.QueryServiceControl.ReloadSchema(ctx)
			if reloadErr != nil {
				log.Errorf("failed to reload the schema %v", reloadErr)
			}
		}
	}()

	if !req.AllowMultiQueries {
		result, err := conn.ExecuteFetch(uq, int(req.MaxRows), true /*wantFields*/)
		return sqltypes.ResultToProto3(result), err
	}

	// Allow multi queries. We only return the results of the first query, but we do return an error
	// upon any query result.
	result, more, err := conn.ExecuteFetchMulti(uq, int(req.MaxRows), true /*wantFields*/)
	if err != nil {
		return nil, err
	}
	for more {
		_, more, _, err = conn.ReadQueryResult(0, false)
		if err != nil {
			return nil, err
		}
	}
	return sqltypes.ResultToProto3(result), err
}

// ExecuteFetchAsAllPrivs will execute the given query, possibly reloading schema.
func (tm *TabletManager) ExecuteFetchAsAllPrivs(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (*querypb.QueryResult, error) {
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}
	// get a connection
	conn, err := tm.MysqlDaemon.GetAllPrivsConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if req.DbName != "" {
		// This execute might fail if db does not exist.
		// Error is ignored because given query might create this database.
		_, _ = conn.ExecuteFetch("USE "+sqlescape.EscapeID(req.DbName), 1, false)
	}

	// Replace any provided sidecar database qualifiers with the correct one.
	uq, err := tm.SQLParser.ReplaceTableQualifiers(string(req.Query), sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}
	result, err := conn.ExecuteFetch(uq, int(req.MaxRows), true /*wantFields*/)

	if err == nil && req.ReloadSchema {
		reloadErr := tm.QueryServiceControl.ReloadSchema(ctx)
		if reloadErr != nil {
			log.Errorf("failed to reload the schema %v", reloadErr)
		}
	}
	return sqltypes.ResultToProto3(result), err
}

// ExecuteFetchAsApp will execute the given query.
func (tm *TabletManager) ExecuteFetchAsApp(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*querypb.QueryResult, error) {
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}
	// get a connection
	conn, err := tm.MysqlDaemon.GetAppConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	// Replace any provided sidecar database qualifiers with the correct one.
	uq, err := tm.SQLParser.ReplaceTableQualifiers(string(req.Query), sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}
	result, err := conn.Conn.ExecuteFetch(uq, int(req.MaxRows), true /*wantFields*/)
	return sqltypes.ResultToProto3(result), err
}

// ExecuteQuery submits a new online DDL request
func (tm *TabletManager) ExecuteQuery(ctx context.Context, req *tabletmanagerdatapb.ExecuteQueryRequest) (*querypb.QueryResult, error) {
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}
	// get the db name from the tablet
	tablet := tm.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}
	// Replace any provided sidecar database qualifiers with the correct one.
	uq, err := tm.SQLParser.ReplaceTableQualifiers(string(req.Query), sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}
	result, err := tm.QueryServiceControl.QueryService().Execute(ctx, target, uq, nil, 0, 0, nil)
	return sqltypes.ResultToProto3(result), err
}
