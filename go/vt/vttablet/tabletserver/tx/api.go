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

package tx

import (
	"fmt"
	"strings"
	"time"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	// ConnID as type int64
	ConnID = int64

	// DTID as type string
	DTID = string

	// EngineStateMachine is used to control the state the transactional engine -
	// whether new connections and/or transactions are allowed or not.
	EngineStateMachine interface {
		Init() error
		AcceptReadWrite() error
		AcceptReadOnly() error
		StopGently()
	}

	// ReleaseReason as type int
	ReleaseReason int

	// Properties contains all information that is related to the currently running
	// transaction on the connection
	Properties struct {
		EffectiveCaller *vtrpcpb.CallerID
		ImmediateCaller *querypb.VTGateCallerID
		StartTime       time.Time
		EndTime         time.Time
		Queries         []Query
		Autocommit      bool
		Conclusion      string
		LogToFile       bool

		Stats *servenv.TimingsWrapper
	}
)

type Query struct {
	Sql    string
	Tables []string
}

const (
	// TxClose - connection released on close.
	TxClose ReleaseReason = iota

	// TxCommit - connection released on commit.
	TxCommit

	// TxRollback - connection released on rollback.
	TxRollback

	// TxKill - connection released on tx kill.
	TxKill

	// ConnInitFail - connection released on failed to start tx.
	ConnInitFail

	// ConnRelease - connection closed.
	ConnRelease

	// ConnRenewFail - reserve connection renew failed.
	ConnRenewFail
)

func (r ReleaseReason) String() string {
	return txResolutions[r]
}

// Name return the name of enum.
func (r ReleaseReason) Name() string {
	return txNames[r]
}

var txResolutions = map[ReleaseReason]string{
	TxClose:       "closed",
	TxCommit:      "transaction committed",
	TxRollback:    "transaction rolled back",
	TxKill:        "kill",
	ConnInitFail:  "initFail",
	ConnRelease:   "release connection",
	ConnRenewFail: "connection renew failed",
}

var txNames = map[ReleaseReason]string{
	TxClose:       "close",
	TxCommit:      "commit",
	TxRollback:    "rollback",
	TxKill:        "kill",
	ConnInitFail:  "initFail",
	ConnRelease:   "release",
	ConnRenewFail: "renewFail",
}

// RecordQueryDetail records the query and tables against this transaction.
func (p *Properties) RecordQueryDetail(query string, tables []string) {
	if p == nil {
		return
	}
	p.Queries = append(p.Queries, Query{
		Sql:    query,
		Tables: tables,
	})
}

// RecordQuery records the query and extract tables against this transaction.
func (p *Properties) RecordQuery(query string, parser *sqlparser.Parser) {
	if p == nil {
		return
	}
	stmt, err := parser.Parse(query)
	if err != nil {
		// This should neven happen, but if it does,
		// we would not be able to block cut-overs on this query.
		return
	}
	tables := sqlparser.ExtractAllTables(stmt)
	p.Queries = append(p.Queries, Query{
		Sql:    query,
		Tables: tables,
	})
}

// InTransaction returns true as soon as this struct is not nil
func (p *Properties) InTransaction() bool { return p != nil }

// String returns a printable version of the transaction
func (p *Properties) String(sanitize bool, parser *sqlparser.Parser) string {
	if p == nil {
		return ""
	}

	printQueries := func() string {
		sb := strings.Builder{}
		for _, query := range p.Queries {
			sql := query.Sql
			if sanitize {
				sql, _ = parser.RedactSQLQuery(sql)
			}
			sb.WriteString(sql)
			sb.WriteString(";")
		}
		return sb.String()
	}

	return fmt.Sprintf(
		"'%v'\t'%v'\t%v\t%v\t%.6f\t%v\t%v\t\n",
		p.EffectiveCaller,
		p.ImmediateCaller,
		p.StartTime.Format(time.StampMicro),
		p.EndTime.Format(time.StampMicro),
		p.EndTime.Sub(p.StartTime).Seconds(),
		p.Conclusion,
		printQueries(),
	)
}
