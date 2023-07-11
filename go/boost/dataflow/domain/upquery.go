package domain

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtgate"
)

const callerIDUser = "root"

// Caller ID used for upqueries.
var vitessCallerID = &vtrpc.CallerID{
	Principal:    callerIDUser,
	Component:    "vtboost",
	Subcomponent: "upquery",
}

var vtgateCallerID = &querypb.VTGateCallerID{
	Username: callerIDUser,
}

func (d *Domain) performUpquery(ctx context.Context, queryStr string, bvars map[string]*querypb.BindVariable, callback func(string, []sql.Record) error) error {
	d.log.Debug("sending upquery", zap.String("query", queryStr))

	var records []sql.Record
	var gtid string

	ctx = callerid.NewContext(ctx, vitessCallerID, vtgateCallerID)
	session := vtgate.NewSafeSession(nil)

	switch d.upqueryMode {
	case config.UpqueryMode_SELECT_GTID:
		session.Autocommit = true
		err := d.executor.StreamExecute(ctx, nil, "Boost.Upquery", session, queryStr, bvars, func(result *sqltypes.Result) error {
			records = records[:0]

			for i, row := range result.Rows {
				if i == 0 {
					gtid = row[0].RawStr()
				} else {
					if row[0].RawStr() != gtid {
						return fmt.Errorf("unstable GTID in upquery")
					}
				}
				records = append(records, sql.RowFromVitess(row[1:]).ToRecord(true))
			}
			return callback(gtid, records)
		})
		if err != nil {
			return fmt.Errorf("failed to start upquery transaction: %w", err)
		}

	case config.UpqueryMode_TRACK_GTID:
		session.Options = &querypb.ExecuteOptions{
			IncludedFields:       querypb.ExecuteOptions_TYPE_ONLY,
			Workload:             querypb.ExecuteOptions_OLAP,
			TransactionIsolation: querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY,
		}

		defer func() {
			_ = d.executor.CloseSession(ctx, session)
		}()

		err := d.executor.StreamExecute(ctx, nil, "Boost.Upquery", session, queryStr, bvars, func(result *sqltypes.Result) error {
			records = records[:0]

			if gtid == "" {
				gtid = result.SessionStateChanges
			}
			for _, row := range result.Rows {
				records = append(records, sql.RowFromVitess(row).ToRecord(true))
			}
			return callback(gtid, records)
		})
		if err != nil {
			return fmt.Errorf("failed to start upquery transaction: %w", err)
		}

	}
	return nil
}
