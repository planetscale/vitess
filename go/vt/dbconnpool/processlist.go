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
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

const showProcessListQuery = `SELECT ID, TIME, STATE, INFO FROM performance_schema.processlist WHERE user = "%s"`

// LogRedactedProcessList logs the redacted process list for the query being run by the given user.
func LogRedactedProcessList(conn *PooledDBConnection, user string) {
	res := getRedactedProcessList(conn, user)
	log.Errorf(fmt.Sprintf("Redacted SHOW FULL PROCESSLIST output - %v", res))
}

// getRedactedProcessList gets the redacted process list for the query being run by the given user.
func getRedactedProcessList(conn *PooledDBConnection, user string) string {
	processLists, err := conn.ExecuteFetch(fmt.Sprintf(showProcessListQuery, user), 10000, true)
	if err != nil {
		log.Errorf("error while reading process list: %v", err)
		return ""
	}
	// Go over all the rows and redact the 4th column values
	for _, row := range processLists.Rows {
		// failsafe check: this should never happen. We should get exactly 4 values in each row
		// corresponding to each field
		if len(row) != 4 {
			continue
		}
		query := row[3].ToString()
		redactedQuery, err := sqlparser.RedactSQLQuery(query)
		if err != nil {
			redactedQuery = fmt.Sprintf("error encountered in redacting query: %v", err)
		}
		row[3] = sqltypes.NewVarChar(redactedQuery)
	}

	// Marshal the output as a json
	res, err := json.MarshalIndent(processLists, "", "\t")
	if err != nil {
		log.Errorf("error while marshalling process list output: %v", err)
	}
	return string(res)
}
