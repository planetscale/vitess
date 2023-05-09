/*
Copyright 2022 The Vitess Authors.

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

package vtgate

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"vitess.io/vitess/go/vt/callerid"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/vt/vtgate/logstats"

	"github.com/pkg/errors"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/streamlog"
)

var (
	logger *streamlog.StreamLogger[*logstats.LogStats]
)

type setupOptions struct {
	bufSize, patternLimit, rowsReadThreshold, responseTimeThreshold, maxPerInterval *uint
	tableString                                                                     string
	maxRawQuerySize                                                                 *uint
	queryTimeSketch                                                                 *bool
	totalDurationSketchAlpha                                                        *float64
	totalDurationSketchUnits                                                        *int
}

func setup(t *testing.T, brokers, publicID, username, password string, options setupOptions) (*Insights, error) {
	logger = streamlog.New[*logstats.LogStats]("tests", 32)
	defaultUint := func(val *uint, dfault uint) uint {
		if val != nil {
			return *val
		}
		return dfault
	}

	defaultFloat64 := func(val *float64, dfault float64) float64 {
		if val != nil {
			return *val
		}
		return dfault
	}

	defaultBool := func(val *bool, dfault bool) bool {
		if val != nil {
			return *val
		}
		return dfault
	}

	defaultInt := func(val *int, dfault int) int {
		if val != nil {
			return *val
		}
		return dfault
	}

	insights, err := initInsightsInner(logger, brokers, publicID, username, password,
		defaultUint(options.bufSize, 5*1024*1024),
		defaultUint(options.patternLimit, 10000),
		defaultUint(options.rowsReadThreshold, 1000),
		defaultUint(options.responseTimeThreshold, 1000),
		defaultUint(options.maxPerInterval, 10000),
		0.1,
		1000,
		defaultUint(options.maxRawQuerySize, 64),
		5,
		15*time.Second,
		true,
		true,
		defaultBool(options.queryTimeSketch, true),
		defaultFloat64(options.totalDurationSketchAlpha, 0.01),
		defaultInt(options.totalDurationSketchUnits, -6))
	if insights != nil {
		t.Cleanup(func() { insights.Drain() })
	}
	return insights, err
}

func TestLimiter(t *testing.T) {
	limiter := limiter{global: rate.NewLimiter(rate.Every(1*time.Second), 6), intervalMax: 2}

	t0 := time.Now()
	t1 := t0.Add(1 * time.Second)
	t2 := t0.Add(2 * time.Second)
	t3 := t0.Add(3 * time.Second)
	t4 := t0.Add(4 * time.Second)

	tests := []struct {
		t                 time.Time
		intervalRemain    int
		global            int
		intervalAllotment int
		tick              bool
		runAllow          bool
		allow             bool
	}{
		{t: t0, intervalRemain: 2, intervalAllotment: 2, global: 4, tick: true},
		{t: t0, intervalRemain: 1, intervalAllotment: 2, global: 4, runAllow: true, allow: true},
		{t: t0, intervalRemain: 0, intervalAllotment: 2, global: 4, runAllow: true, allow: true},
		{t: t0, intervalRemain: 0, intervalAllotment: 2, global: 4, runAllow: true, allow: false},
		{t: t0, intervalRemain: 2, intervalAllotment: 2, global: 2, tick: true},
		{t: t0, intervalRemain: 1, intervalAllotment: 2, global: 2, runAllow: true, allow: true},
		{t: t0, intervalRemain: 2, intervalAllotment: 2, global: 1, tick: true},
		{t: t0, intervalRemain: 1, intervalAllotment: 2, global: 1, runAllow: true, allow: true},
		{t: t0, intervalRemain: 0, intervalAllotment: 2, global: 1, runAllow: true, allow: true},
		{t: t0, intervalRemain: 0, intervalAllotment: 2, global: 1, runAllow: true, allow: false},
		{t: t0, intervalRemain: 1, intervalAllotment: 1, global: 0, tick: true},
		{t: t0, intervalRemain: 0, intervalAllotment: 1, global: 0, runAllow: true, allow: true},
		{t: t0, intervalRemain: 0, intervalAllotment: 1, global: 0, runAllow: true, allow: false},
		{t: t0, intervalRemain: 0, intervalAllotment: 0, global: 0, tick: true},
		{t: t1, intervalRemain: 0, intervalAllotment: 0, global: 1},
		{t: t1, intervalRemain: 1, intervalAllotment: 1, global: 0, tick: true},
		{t: t2, intervalRemain: 1, intervalAllotment: 1, global: 1},
		{t: t2, intervalRemain: 2, intervalAllotment: 2, global: 0, tick: true},
		{t: t2, intervalRemain: 1, intervalAllotment: 2, global: 0, runAllow: true, allow: true},
		{t: t3, intervalRemain: 1, intervalAllotment: 2, global: 1},
		{t: t3, intervalRemain: 2, intervalAllotment: 2, global: 0, tick: true},
		{t: t4, intervalRemain: 2, intervalAllotment: 2, global: 1, tick: true},
	}

	for _, tc := range tests {
		name := fmt.Sprintf("t=%v/remain=%v/global=%v/tick=%v/runAllow=%v/allow=%v",
			tc.t.Sub(t0),
			tc.intervalRemain,
			tc.global,
			tc.tick,
			tc.runAllow,
			tc.allow)
		t.Run(name, func(t *testing.T) {
			if tc.tick {
				limiter.tickAt(tc.t)
			}

			if tc.runAllow {
				assert.Equal(t, tc.allow, limiter.allow())
			}

			assert.Equal(t, tc.intervalRemain, limiter.intervalRemain)
			assert.Equal(t, tc.intervalAllotment, limiter.intervalAllotment)
			assert.Equal(t, tc.global, int(limiter.global.TokensAt(tc.t)))
		})
	}
}

func TestInsightsNeedsDatabaseBranchID(t *testing.T) {
	_, err := setup(t, "localhost:1234", "", "", "", setupOptions{})
	assert.ErrorContains(t, err, "public_id is required")
}

func TestInsightsDisabled(t *testing.T) {
	_, err := setup(t, "", "", "", "", setupOptions{})
	assert.NoError(t, err)
}

func TestInsightsEnabled(t *testing.T) {
	_, err := setup(t, "localhost:1234", "mumblefoo", "", "", setupOptions{})
	assert.NoError(t, err)
}

func TestInsightsMissingUsername(t *testing.T) {
	_, err := setup(t, "localhost:1234", "mumblefoo", "", "password", setupOptions{})
	assert.ErrorContains(t, err, "without a username")
}

func TestInsightsMissingPassword(t *testing.T) {
	_, err := setup(t, "localhost:1234", "mumblefoo", "username", "", setupOptions{})
	assert.ErrorContains(t, err, "without a password")
}

func TestInsightsTotalDurationSketchAlphaValidation(t *testing.T) {
	tests := []struct {
		alpha      float64
		shouldFail bool
	}{
		{math.SmallestNonzeroFloat64, true}, // becomes 0 when cast to float32
		{-0.1, true},
		{0.0, true},
		{0.001, false},
		{0.99, false},
		{0.999999999, true}, // becomes 1 when cast to float32
		{1.0, true},
		{1.1, true},
	}

	for _, tc := range tests {
		_, err := setup(t, "localhost:1234", "mumblefoo", "username", "password", setupOptions{
			totalDurationSketchAlpha: &tc.alpha,
		})

		if tc.shouldFail {
			assert.ErrorContains(t, err, "-insights_total_duration_sketch_alpha must be between 0.0 and 1.0 (exclusive)", "alpha: %v", tc.alpha)
		} else {
			assert.NoError(t, err, "alpha: %v", tc.alpha)
		}
	}
}

func TestInsightsTotalDurationSketchUnitsValidation(t *testing.T) {
	tests := []struct {
		units      int
		shouldFail bool
	}{
		{-10, true},
		{-9, false},
		{-6, false},
		{-3, false},
		{0, false},
		{1, true},
	}

	for _, tc := range tests {
		_, err := setup(t, "localhost:1234", "mumblefoo", "username", "password", setupOptions{
			totalDurationSketchUnits: &tc.units,
		})

		if tc.shouldFail {
			assert.ErrorContains(t, err, "-insights_total_duration_sketch_units must be between -9 and 0 (inclusive)", "units: %v", tc.units)
		} else {
			assert.NoError(t, err, "units: %v", tc.units)
		}
	}
}

func TestInsightsConnectionRefused(t *testing.T) {
	// send to a real Kafka endpoint, will fail
	insights, err := setup(t, "localhost:1", "mumblefoo", "", "", setupOptions{})
	require.NoError(t, err)
	logger.Send(realize(t, lsSlowQuery))
	require.True(t, insights.Drain(), "did not drain")
}

func TestInsightsSlowQuery(t *testing.T) {
	insights, err := setup(t, "localhost:1234", "mumblefoo", "", "", setupOptions{})
	require.NoError(t, err)
	messages := 0
	insights.Sender = func(buf []byte, topic, key string) error {
		messages++
		assert.Contains(t, string(buf), "select sleep(?)")
		assert.Contains(t, string(buf), "planetscale-reader")
		assert.Contains(t, key, "mumblefoo/")
		assert.Equal(t, queryTopic, topic)
		return nil
	}
	logger.Send(realize(t, lsSlowQuery))
	require.True(t, insights.Drain(), "did not drain")
	assert.Equal(t, 1, messages)
}

func TestInsightsSummaries(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select sleep(5)", responseTime: 5 * time.Second},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond, rowsRead: 2},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond, rowsRead: 3},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond, rowsRead: 5},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond, rowsRead: 7},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select sleep(?)", "total_duration:{seconds:5}", `statement_type:{value:\"SELECT\"}`),
			expect(queryStatsBundleTopic, "select sleep(?)", "query_count:1", "sum_total_duration:{seconds:5}", "max_total_duration:{seconds:5}"),
			expect(queryStatsBundleTopic, "select * from foo", "query_count:4", "sum_total_duration:{nanos:40000000}",
				"max_total_duration:{nanos:10000000}", "sum_rows_read:17", "max_rows_read:7"),
		})
}

func TestInsightsSummariesByKeyspace(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select * from foo", responseTime: 1 * time.Nanosecond, keyspace: "a", activeKeyspace: "1"},
			{sql: "select * from foo", responseTime: 2 * time.Nanosecond, keyspace: "a", activeKeyspace: "1"},
			{sql: "select * from foo", responseTime: 3 * time.Nanosecond, keyspace: "a", activeKeyspace: "2"},
			{sql: "select * from foo", responseTime: 4 * time.Nanosecond, keyspace: "a", activeKeyspace: "2"},
			{sql: "select * from foo", responseTime: 5 * time.Nanosecond, keyspace: "b", activeKeyspace: "1"},
			{sql: "select * from foo", responseTime: 6 * time.Nanosecond, keyspace: "b", activeKeyspace: "1"},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, "select * from foo", "sum_total_duration:{nanos:3}", `keyspace:{value:\"a\"}`, `active_keyspace:{value:\"1\"}`),
			expect(queryStatsBundleTopic, "select * from foo", "sum_total_duration:{nanos:7}", `keyspace:{value:\"a\"}`, `active_keyspace:{value:\"2\"}`),
			expect(queryStatsBundleTopic, "select * from foo", "sum_total_duration:{nanos:11}", `keyspace:{value:\"b\"}`, `active_keyspace:{value:\"1\"}`),
		})
}

func TestInsightsBoostQueryId(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select * from foo", boostQueryID: "abc", responseTime: 5 * time.Second},
			{sql: "select * from foo", boostQueryID: "def", responseTime: 5 * time.Second},
			{sql: "select * from foo", boostQueryID: "def", responseTime: 5 * time.Second},
			{sql: "select * from bar", boostQueryID: "", responseTime: 5 * time.Second},
			{sql: "select * from bar", boostQueryID: "", responseTime: 5 * time.Second},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, "select * from foo", "boost_query_public_id:{value:\\\"abc\\\"}", "query_count:1"),
			expect(queryTopic, "select * from foo", "boost_query_public_id:{value:\\\"abc\\\"}").count(1),

			expect(queryStatsBundleTopic, "select * from foo", "boost_query_public_id:{value:\\\"def\\\"}", "query_count:2"),
			expect(queryTopic, "select * from foo", "boost_query_public_id:{value:\\\"def\\\"}").count(2),

			expect(queryStatsBundleTopic, "select * from bar", "query_count:2"),
			expect(queryTopic, "select * from bar").count(2),
		})
}

func TestInsightsSchemaChanges(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "create table foo (bar int)"},
			{sql: "alter table foo add column bar int"},
			{sql: "drop table foo"},
			{sql: "rename table foo to bar"},
			{sql: "alter view foo as select b,c from bar"},
			{sql: "create view foo(x,y,z) as select a,b from bar"},
			{sql: "drop view foo"},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, "create table foo"),
			expect(schemaChangeTopic, "CREATE TABLE `foo` (\\\\n\\\\t`bar` int\\\\n)", "operation:CREATE_TABLE", "normalized:true"),

			expect(queryStatsBundleTopic, "alter table foo"),
			expect(schemaChangeTopic, "ALTER TABLE `foo` ADD COLUMN `bar` int", "operation:ALTER_TABLE", "normalized:true"),

			expect(queryStatsBundleTopic, "drop table foo"),
			expect(schemaChangeTopic, "DROP TABLE `foo`", "operation:DROP_TABLE", "normalized:true"),

			expect(queryStatsBundleTopic, "rename table foo"),
			expect(schemaChangeTopic, "RENAME TABLE `foo` TO `bar`", "operation:RENAME_TABLE", "normalized:true"),

			expect(queryStatsBundleTopic, "alter view foo"),
			expect(schemaChangeTopic, "ALTER VIEW `foo` AS SELECT `b`, `c` FROM `bar`", "operation:ALTER_VIEW", "normalized:true"),

			expect(queryStatsBundleTopic, "create view foo"),
			expect(schemaChangeTopic, "CREATE VIEW `foo`(`x`, `y`, `z`) AS SELECT `a`, `b` FROM `bar`", "operation:CREATE_VIEW", "normalized:true"),

			expect(queryStatsBundleTopic, "drop view foo"),
			expect(schemaChangeTopic, "DROP VIEW `foo`", "operation:DROP_VIEW", "normalized:true"),
		})
}

func TestInsightsSchemaChangesNoTruncateTable(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "truncate table foo"},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, "truncate table foo"),
		})
}

func TestInsightsTooManyPatterns(t *testing.T) {
	var patternLimit uint = 3
	insightsTestHelper(t, true,
		setupOptions{patternLimit: &patternLimit},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 5 * time.Second},
			{sql: "select * from foo2", responseTime: 5 * time.Second},
			{sql: "select * from foo3", responseTime: 5 * time.Second},
			{sql: "select * from foo4", responseTime: 5 * time.Second},
			{sql: "select * from foo5", responseTime: 5 * time.Second},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select * from foo1", "total_duration:{seconds:5}"),
			expect(queryTopic, "select * from foo2", "total_duration:{seconds:5}"),
			expect(queryTopic, "select * from foo3", "total_duration:{seconds:5}"),
			expect(queryTopic, "select * from foo4", "total_duration:{seconds:5}"),
			expect(queryTopic, "select * from foo5", "total_duration:{seconds:5}"),
			expect(queryStatsBundleTopic, "select * from foo1", "query_count:1", "sum_total_duration:{seconds:5}", "max_total_duration:{seconds:5}"),
			expect(queryStatsBundleTopic, "select * from foo2", "query_count:1", "sum_total_duration:{seconds:5}", "max_total_duration:{seconds:5}"),
			expect(queryStatsBundleTopic, "select * from foo3", "query_count:1", "sum_total_duration:{seconds:5}", "max_total_duration:{seconds:5}"),
		})
}

func TestInsightsTooManyInteresting(t *testing.T) {
	var maxPerInterval uint = 4
	insightsTestHelper(t, true,
		setupOptions{maxPerInterval: &maxPerInterval},
		[]insightsQuery{
			{sql: "select 1", responseTime: 5 * time.Second},
			{sql: "select 1", rowsRead: 20000},
			{sql: "select 1", error: "thou shalt not"},
			{sql: "select 1", responseTime: 6 * time.Second},
			{sql: "select 1", rowsRead: 21000},
			{sql: "select 1", error: "no but seriously"},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select ?", "total_duration:{seconds:5}"),
			expect(queryTopic, "select ?", "rows_read:20000"),
			expect(queryTopic, "<error>", "thou shalt not"),
			expect(queryTopic, "select ?", "total_duration:{seconds:6}"),
			expect(queryStatsBundleTopic, "select ?", "query_count:4", "sum_total_duration:{seconds:11}", "max_total_duration:{seconds:6}", "sum_rows_read:41000"),
			expect(queryStatsBundleTopic, "<error>", "query_count:2", "error_count:2"),
		})
}

func TestInsightsResponseTimeThreshold(t *testing.T) {
	var responseTimeThreshold uint = 500
	insightsTestHelper(t, false,
		setupOptions{responseTimeThreshold: &responseTimeThreshold},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 400 * time.Millisecond},
			{sql: "select * from foo2", responseTime: 600 * time.Millisecond},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select * from foo2", "total_duration:{nanos:600000000}"),
		})
}

func TestInsightsRowsReadThreshold(t *testing.T) {
	var rowsReadThreshold uint = 42
	insightsTestHelper(t, false,
		setupOptions{rowsReadThreshold: &rowsReadThreshold},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 5 * time.Millisecond, rowsRead: 88},
			{sql: "select * from foo2", responseTime: 5 * time.Millisecond, rowsRead: 15},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select * from foo1", "total_duration:{nanos:5000000}", "rows_read:88"),
		})
}

func TestInsightsKafkaBufferSize(t *testing.T) {
	var bufSize uint = 5
	insightsTestHelper(t, true,
		setupOptions{bufSize: &bufSize},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 5 * time.Second},
		},
		nil)
}

func TestInsightsComments(t *testing.T) {
	insightsTestHelper(t, true,
		setupOptions{},
		[]insightsQuery{
			{sql: "select  /*abc='xxx%2fyyy%3azzz'*/ * from foo", responseTime: 5 * time.Second},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, `value:\"select * from foo\"`, `key:\"abc\" value:\"xxx/yyy:zzz\"`),
			expect(queryStatsBundleTopic, `value:\"select * from foo\"`).butNot("xxx"),
		})
}

func TestInsightsErrors(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select this does not parse", error: "syntax error at position 21 after 'does'"},
			{sql: "nor does this", error: "this is a fake error, BindVars: {'bar'}"},
			{sql: "third bogus", error: "another fake error, Sql: \"third bogus\""},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, `normalized_sql:{value:\"<error>\"}`, `error:{value:\"syntax error at position <position>\"}`, `statement_type:{value:\"ERROR\"}`).butNot("does"),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"<error>\"}`, `statement_type:\"ERROR\"`, "query_count:3", "error_count:3").butNot("does", "bar", "bogus"),
			expect(queryTopic, `normalized_sql:{value:\"<error>\"}`, `error:{value:\"this is a fake error\"}`, `statement_type:{value:\"ERROR\"}`).butNot("bar", "BindVars"),
			expect(queryTopic, `normalized_sql:{value:\"<error>\"}`, `error:{value:\"another fake error\"}`, `statement_type:{value:\"ERROR\"}`).butNot("Sql", "bogus"),
		})
}

func TestInsightsSafeErrors(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select :vtg1", normalized: YES, error: "target: commerce.0.primary: vttablet: rpc error: code = Canceled desc = (errno 2013) due to context deadline exceeded, elapsed time: 29.998788288s, killing query ID 58 (CallerID: userData1)"},
			{sql: "select :vtg1", normalized: YES, error: `target: commerce.0.primary: vttablet: rpc error: code = Aborted desc = Row count exceeded 10000 (errno 10001) (sqlstate HY000) (CallerID: userData1): Sql: "select * from bar as f1 join bar as f2 join bar as f3 join bar as f4 join bar as f5 join bar as f6 where f1.id > ?", BindVars: {#maxLimit: "type:INT64 value:\"10001\""vtg1: "type:INT64 value:\"0\"`},
			{sql: "select :vtg1", normalized: YES, error: "target: commerce.0.primary: vttablet: rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (18345369 vs. 16777216)"},
			{sql: "select :vtg1", normalized: YES, error: `target: commerce.0.primary: vttablet: rpc error: code = Canceled desc = EOF (errno 2013) (sqlstate HY000) (CallerID: userData1): Sql: "select ? from bar", BindVars: {#maxLimit: "type:INT64 value:\"10001\""vtg1: "type:INT64 value:\"1\"`},
			{sql: "select :vtg1", normalized: YES, error: `target: sharded.-40.primary: vttablet: rpc error: code = Unavailable desc = error reading from server: EOF`},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, `normalized_sql:{value:\"select ?`, `code = Canceled`).butNot("bar", "BindVars", "Sql").count(2),
			expect(queryTopic, `normalized_sql:{value:\"select ?`, `code = Aborted`).butNot("bar", "BindVars", "Sql"),
			expect(queryTopic, `normalized_sql:{value:\"select ?`, `code = ResourceExhausted`),
			expect(queryTopic, `normalized_sql:{value:\"select ?`, `code = Unavailable`),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"select ? from dual\"}`, `statement_type:\"ERROR\"`, "query_count:5", "error_count:5").butNot("bar", "BindVars"),
		})
}

func TestInsightsSavepoints(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "savepoint baz"},
			{sql: "savepoint bar"},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, `savepoint <id>`, "query_count:2", `statement_type:\"SAVEPOINT\"`).butNot("baz", "bar"),
		})
}

func TestInsightsExtraNormalization(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			// IN (...) lists get collapsed
			{sql: "select beam.`User`.id, beam.`User`.`name` from beam.`User` where beam.`User`.id in (:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9, :v10, :v11, :v12, :v13, :v14, :v15, :v16, :v17, :v18, :v19, :v20, :v21, :v22, :v23, :v24, :v25, :v26, :v27, :v28, :v29, :v30, :v31, :v32, :v33, :v34, :v35, :v36, :v37, :v38, :v39, :v40, :v41, :v42, :v43, :v44, :v45, :v46, :v47, :v48, :v49, :v50, :v51, :v52, :v53, :v54, :v55, :v56, :v57, :v58, :v59, :v60, :v61, :v62, :v63, :v64, :v65, :v66, :v67, :v68, :v69, :v70, :v71, :v72, :v73)", responseTime: 5 * time.Second},
			{sql: "select * from users where foo in (:v1, :v2) and bar in (:v3, :v4) and baz in (:v5) and blarg in (:v6)", responseTime: 5 * time.Second},

			// INSERT ... VALUES (...) lists get collapsed
			{sql: "insert into foo values (:v1, :vtg2), (?, null), (:v3, :v4)", responseTime: 5 * time.Second},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select beam.`User`.id, beam.`User`.`name` from beam.`User` where beam.`User`.id in (<elements>)").butNot(":v73", "?"),
			expect(queryStatsBundleTopic, "select beam.`User`.id, beam.`User`.`name` from beam.`User` where beam.`User`.id in (<elements>)").butNot(":v73", "?"),
			expect(queryTopic, "select * from users where foo in (<elements>) and bar in (<elements>) and baz in (<elements>) and blarg in (<elements>)").butNot(":v2", ":v4", ":v5", "?"),
			expect(queryStatsBundleTopic, "select * from users where foo in (<elements>) and bar in (<elements>) and baz in (<elements>) and blarg in (<elements>)").butNot(":v2", ":v4", ":v5", "?"),
			expect(queryTopic, "insert into foo values <values>", `statement_type:{value:\"INSERT\"}`).butNot(":v1", ":vtg1", "null", "?"),
			expect(queryStatsBundleTopic, "insert into foo values <values>", `statement_type:\"INSERT\"`).butNot(":v1", ":vtg1", "null", "?"),
		})
}

func TestInsightsInsertColumnOrderNormalization(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			// filler statements that shouldn't count toward the ii.ReorderInsertColumns threshold
			{sql: "select 1 from foo"},
			{sql: "select 1 from foo"},
			{sql: "select 1 from foo"},
			{sql: "select 1 from foo"},
			{sql: "select 1 from foo"},
			{sql: "select 1 from foo"},
			{sql: "select 1 from foo"},
			{sql: "select 1 from bar"},

			// INSERTs without a column list are unchanged
			{sql: "insert into foo values (222, 333, 444, 555)", responseTime: 5 * time.Second},

			// INSERT INTO table(...) VALUES (...) column lists get alphabetized, after a while
			{sql: "insert into foo(d, c, b, a) values (111, 222, 333, 444)", responseTime: 5 * time.Second},
			{sql: "insert into foo(d, c, a, b) values (555, 666, 777, 888)", responseTime: 5 * time.Second},
			{sql: "insert into foo(d, c, a, b) values (555, 666, 777, 888)", responseTime: 5 * time.Second}, // same as above, doesn't count toward reorder threshold
			{sql: "insert into foo(d, b, c, a) values (999, 111, 222, 333)", responseTime: 5 * time.Second},
			{sql: "insert into foo(d, b, a, c) values (444, 555, 666, 777)", responseTime: 5 * time.Second},
			{sql: "insert into foo(d, a, b, c) values (888, 999, 111, 222)", responseTime: 5 * time.Second},
			// five (distinct) patterns above, so the two below get alphabetized
			{sql: "insert into foo(d, a, c, b) values (333, 444, 555, 666)", responseTime: 5 * time.Second},
			{sql: "insert into foo(c, d, b, a) values (777, 888, 999, 111)", responseTime: 5 * time.Second},

			// Still unchanged, even after ReorderInsertColumns is enabled
			{sql: "insert into foo values (222, 333, 444, 555)", responseTime: 5 * time.Second},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, "select ? from foo", `statement_type:\"SELECT\"`),
			expect(queryStatsBundleTopic, "select ? from bar", `statement_type:\"SELECT\"`),

			expect(queryTopic, "insert into foo(d, c, b, a) values <values>", `statement_type:{value:\"INSERT\"}`).butNot(":v1", ":vtg1", "null", "?"),
			expect(queryStatsBundleTopic, "insert into foo(d, c, b, a) values <values>", `statement_type:\"INSERT\"`).butNot(":v1", ":vtg1", "null", "?"),

			expect(queryTopic, "insert into foo(d, c, a, b) values <values>", `statement_type:{value:\"INSERT\"}`).butNot(":v1", ":vtg1", "null", "?").count(2),
			expect(queryStatsBundleTopic, "insert into foo(d, c, a, b) values <values>", `statement_type:\"INSERT\"`, `query_count:2`).butNot(":v1", ":vtg1", "null", "?"),

			expect(queryTopic, "insert into foo(d, b, c, a) values <values>", `statement_type:{value:\"INSERT\"}`).butNot(":v1", ":vtg1", "null", "?"),
			expect(queryStatsBundleTopic, "insert into foo(d, b, c, a) values <values>", `statement_type:\"INSERT\"`).butNot(":v1", ":vtg1", "null", "?"),

			expect(queryTopic, "insert into foo(d, b, a, c) values <values>", `statement_type:{value:\"INSERT\"}`).butNot(":v1", ":vtg1", "null", "?"),
			expect(queryStatsBundleTopic, "insert into foo(d, b, a, c) values <values>", `statement_type:\"INSERT\"`).butNot(":v1", ":vtg1", "null", "?"),

			expect(queryTopic, "insert into foo(d, a, b, c) values <values>", `statement_type:{value:\"INSERT\"}`).butNot(":v1", ":vtg1", "null", "?"),
			expect(queryStatsBundleTopic, "insert into foo(d, a, b, c) values <values>", `statement_type:\"INSERT\"`).butNot(":v1", ":vtg1", "null", "?"),

			// final two get alphabetized
			expect(queryTopic, "insert into foo(a, b, c, d) values <values>", `statement_type:{value:\"INSERT\"}`).butNot(":v1", ":vtg1", "null", "?").count(2),
			expect(queryStatsBundleTopic, "insert into foo(a, b, c, d) values <values>", `statement_type:\"INSERT\"`, `query_count:2`).butNot(":v1", ":vtg1", "null", "?"),

			expect(queryTopic, "insert into foo values <values>", `statement_type:{value:\"INSERT\"}`).butNot(":v1", ":vtg1", "null", "?").count(2),
			expect(queryStatsBundleTopic, "insert into foo values <values>", `statement_type:\"INSERT\"`, `query_count:2`).butNot(":v1", ":vtg1", "null", "?"),
		})
}

func TestInsightsPreparesExcluded(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select foo", responseTime: 5 * time.Second},
			{sql: "select bar", responseTime: 5 * time.Second, method: "Prepare"},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, `select foo`, "query_count:1"),
			expect(queryTopic, `select foo`),
		})

}

func TestMakeKafkaKeyIsDeterministic(t *testing.T) {
	insights, err := setup(t, "localhost:1234", "mumblefoo", "", "", setupOptions{})
	require.NoError(t, err)

	sql := `this isn't even real sql`
	key := insights.makeKafkaKey(sql)
	assert.Equal(t, "mumblefoo/6edc967a", key)

	sql = `another string value`
	key = insights.makeKafkaKey(sql)
	assert.Equal(t, "mumblefoo/67374b03", key)
}

func TestTables(t *testing.T) {
	testCases := []struct {
		name, input    string
		split          []string
		message, avoid string
	}{
		{
			"empty",
			"",
			nil,
			"",
			"tables",
		},
		{
			"one table",
			"foo",
			[]string{"foo"},
			`tables:\"foo\"`,
			"",
		},
		{
			"two tables",
			"foo, bar",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			",",
		},
		{
			"two tables without a space",
			"foo,bar",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			",",
		},
		{
			"one table name with backticks",
			"`foo`",
			[]string{"foo"},
			`tables:\"foo\"`,
			"`",
		},
		{
			"two table names with backticks",
			"`foo`, `bar`",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			"`",
		},
		{
			"two table names with backticks, no space",
			"`foo`,`bar`",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			"`",
		},
		{
			"table name has a comma",
			"`foo, bar`",
			[]string{"foo, bar"},
			`tables:\"foo, bar\"`,
			"`",
		},
		{
			"table name is only partially quoted",
			"foo.`order`,b,`c,d`.e",
			[]string{"foo.order", "b", "c,d.e"},
			`tables:\"foo.order\" tables:\"b\" tables:\"c,d.e\"`,
			"`",
		},
		{
			"many parts, some in backticks",
			"`abc`.`def`.ghi.`j,kl`,`mno`.pqr.`stu`",
			[]string{"abc.def.ghi.j,kl", "mno.pqr.stu"},
			`tables:\"abc.def.ghi.j,kl\" tables:\"mno.pqr.stu\"`,
			"`",
		},
		{
			"unterminated backtick",
			"foo, `bar, baz",
			[]string{"foo", "bar, baz"},
			`tables:\"foo\" tables:\"bar, baz\"`,
			"`",
		},
		{
			"ends with a comma",
			"foo,bar,",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			"`",
		},
		{
			"extra commas",
			"foo,,,bar,,",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			",",
		},
		{
			"only a comma",
			",",
			nil,
			"",
			"tables",
		},
		{
			"commas and backticks extravaganza",
			"`foo,bar`, baz, `blah`, `lorem`, ipsum, `abc, xyz`",
			[]string{"foo,bar", "baz", "blah", "lorem", "ipsum", "abc, xyz"},
			`tables:\"foo,bar\" tables:\"baz\" tables:\"blah\" tables:\"lorem\" tables:\"ipsum\" tables:\"abc, xyz\"`,
			"`",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := splitTables(tc.input)
			assert.Equal(t, tc.split, op)

			e1 := expect(queryTopic, "select ?", " total_duration:{seconds:5}")
			e2 := expect(queryStatsBundleTopic, "select ?", "query_count:1 sum_total_duration:{seconds:5} max_total_duration:{seconds:5}")
			if tc.message != "" {
				e1.patterns = append(e1.patterns, tc.message)
				e2.patterns = append(e2.patterns, tc.message)
			}
			if tc.avoid != "" {
				e1 = e1.butNot(tc.avoid)
				e2 = e2.butNot(tc.avoid)
			}
			insightsTestHelper(t, true, setupOptions{tableString: tc.input},
				[]insightsQuery{
					{sql: "select 1", responseTime: 5 * time.Second},
				},
				[]insightsKafkaExpectation{
					e1,
					e2,
				})
		})
	}
}

func TestNormalization(t *testing.T) {
	testCases := []struct {
		input, output string
	}{
		// nothing to change
		{"select * from users where id=:vtg1", "select * from users where id = ?"},

		// normalizer strips off comments
		{"/* with some leading comments */ select * from users where id=:vtg1 /* with some trailing comments */", "select * from users where id = ?"},

		// savepoints
		{"savepoint foo", "savepoint <id>"},
		{"release savepoint bar", "release savepoint <id>"},

		// booleans
		{"select * from users where staff=true", "select * from users where staff = ?"},
		{"select * from users where staff=false", "select * from users where staff = ?"},
		{"update users set staff=true where id=:vtg1", "update users set staff = ? where id = ?"},
		{"update users set staff=false where id=:vtg1", "update users set staff = ? where id = ?"},

		// nulls
		{"update users set email=null where id=:vtg1", "update users set email = ? where id = ?"},
		{"select * from users where email is null", "select * from users where email is null"},
		{"select * from users where email is not null", "select * from users where email is not null"},
		{"update users set email=null where email is not null", "update users set email = ? where email is not null"},

		//-- VALUES compaction
		// one tuple
		{"insert into xyz values (:vtg1, :vtg2)", "insert into xyz values <values>"},

		// case insensitive
		{"INSERT INTO xyz VALUES (:vtg1, :vtg2)", "insert into xyz values <values>"},

		// multiple tuples
		{"insert into xyz values (:vtg1, :vtg2), (:vtg3, null), (null, :vtg4)", "insert into xyz values <values>"},

		// multiple singles
		{"insert into xyz values (:vtg1), (null), (:vtg2)", "insert into xyz values <values>"},

		// bind variables are renumbered starting from 1
		{"select * from xyz where col1 = :vtg2 and col2 = :vtg4", "select * from xyz where col1 = ? and col2 = ?"},

		// bind variables are renumbered starting from 1 after removing the values
		{"insert into xyz(col1, col2) values (:vtg1, :vtg2), (:vtg3, :vtg4) on duplicate key update col1 = :vtg5, col2 = coalesce(col2, :vtg6)", "insert into xyz(col1, col2) values <values> on duplicate key update col1 = ?, col2 = coalesce(col2, ?)"},

		// bind variables renumbering keeps re-used binds (does not assign different bind nums to binds that were previously the same)
		{"insert into xyz(col1, col2) values (:vtg1, :vtg2), (:vtg3, :vtg4) on duplicate key update col1 = :vtg5, col2 = coalesce(col2, :vtg5), col1 = :vtg6, col2 = coalesce(col2, :vtg7) ", "insert into xyz(col1, col2) values <values> on duplicate key update col1 = ?, col2 = coalesce(col2, ?), col1 = ?, col2 = coalesce(col2, ?)"},

		// question marks instead
		{"insert into xyz values (?, ?)", "insert into xyz values <values>"},

		//-- SET compaction: IN
		// case insensitive
		{"SELECT 1 FROM x WHERE xyz IN (:vtg1, :vtg2) AND abc in (:vtg3, :vtg4)", "select ? from x where xyz in (<elements>) and abc in (<elements>)"},

		// question marks instead
		{"SELECT 1 FROM x WHERE xyz IN (?, ?) AND abc in (?, ?)", "select ? from x where xyz in (<elements>) and abc in (<elements>)"},

		// single element in list
		{"select 1 FROM x where xyz in (:vtg1)", "select ? from x where xyz in (<elements>)"},

		// very large :v sequence numbers
		{"select 1 from x where xyz in (:vtg8675309, :vtg8765000)", "select ? from x where xyz in (<elements>)"},

		// nested, single
		{"select 1 from x where (abc, xyz) in ((:vtg1, :vtg2))", "select ? from x where (abc, xyz) in (<elements>)"},

		// nested, multiple
		{"select 1 from x where (abc, xyz) in ((:vtg1, :vtg2), (:vtg3, :vtg4), (:vtg5, :vtg6))", "select ? from x where (abc, xyz) in (<elements>)"},

		// nested, multiple, question marks
		{"select 1 from x where (abc, xyz) in ((?, ?), (?, ?), (?, ?))", "select ? from x where (abc, xyz) in (<elements>)"},

		// mixed nested and simple
		{"select 1 from x where xyz in ((:vtg1, :vtg2), :vtg3)", "select ? from x where xyz in (<elements>)"},

		// subqueries should not be normalized
		{"select 1 from x where xyz in (select distinct foo from bar)", "select ? from x where xyz in (select distinct foo from bar)"},

		// stuff within a subquery should be normalized
		{"select 1 from x where xyz in (select distinct foo from bar where baz in (1,2,3))", "select ? from x where xyz in (select distinct foo from bar where baz in (<elements>))"},

		//-- SET compaction: NOT IN
		// case insensitive
		{"SELECT 1 FROM x WHERE xyz NOT IN (:vtg1, :vtg2) AND abc not in (:v3, :v4)", "select ? from x where xyz not in (<elements>) and abc not in (<elements>)"},

		// question marks instead
		{"SELECT 1 FROM x WHERE xyz NOT IN (?, ?) AND abc not in (?, ?)", "select ? from x where xyz not in (<elements>) and abc not in (<elements>)"},

		// single element in list
		{"select 1 FROM x where xyz not in (:bv1)", "select ? from x where xyz not in (<elements>)"},

		// very large :v sequence numbers
		{"select 1 from x where xyz not in (:v8675309, :v8765000)", "select ? from x where xyz not in (<elements>)"},

		// nested, single
		{"select 1 from x where (abc, xyz) not in ((:v1, :v2))", "select ? from x where (abc, xyz) not in (<elements>)"},

		// nested, multiple
		{"select 1 from x where (abc, xyz) not in ((:vtg1, :vtg2), (:vtg3, :vtg4), (:vtg5, :vtg6))", "select ? from x where (abc, xyz) not in (<elements>)"},

		// nested, multiple, question marks
		{"select 1 from x where (abc, xyz) not in ((?, ?), (?, ?), (?, ?))", "select ? from x where (abc, xyz) not in (<elements>)"},

		// mixed nested and simple
		{"select 1 from x where xyz not in ((:v1, :v2), :v3)", "select ? from x where xyz not in (<elements>)"},

		// subqueries should not be normalized
		{"select 1 from x where xyz not in (select distinct foo from bar)", "select ? from x where xyz not in (select distinct foo from bar)"},

		// stuff within a subquery should be normalized
		{"select 1 from x where xyz not in (select distinct foo from bar where baz not in (1,2,3))", "select ? from x where xyz not in (select distinct foo from bar where baz not in (<elements>))"},

		// Literals are removed
		{"insert into users(email, first_name) select * from (select 'e@mail.com' as email, 'fn' as first_name) as tmp", "insert into users(email, first_name) select * from (select ? as email, ? as first_name from dual) as tmp"},
		{"set @my_secret_variable = 'secret'", "set @my_secret_variable = ?"},
		{"select 1 from x where xyz not in (select distinct foo from bar where baz not in (1,2,3))", "select ? from x where xyz not in (select distinct foo from bar where baz not in (<elements>))"},

		// unary operators
		{"select * from users where id=-1", "select * from users where id = ?"},
		{"select * from users where id=-:vtg1", "select * from users where id = ?"},
		{"select * from users where id=+1", "select * from users where id = ?"},
		{"select * from users where happy=!true", "select * from users where happy = ?"},
		{"select * from users where bitfield=~1", "select * from users where bitfield = ?"},
		{"select * from users where email=N'wut'", "select * from users where email = ?"},
		{"select b'010101' from users", "select ? from users"},
		{"select x'1f' from users", "select ? from users"},
		{"select _utf8'hello' from users", "select ? from users"},

		// negatives that shouldn't get squashed
		{"select 5-4 from users", "select ? - ? from users"},
		{"select * from users where id=-refcount", "select * from users where id = -refcount"},
		{"select * from users where id=-(select id from users limit 1)", "select * from users where id = -(select id from users limit ?)"},

		// comments should get stripped out
		{"select /*+ SET_VAR(sql_mode = 'NO_ZERO_IN_DATE') */ sleep(:vtg1) from customer", "select sleep(?) from customer"},
		{"select /* freeform comment */ sleep(:vtg1) from customer", "select sleep(?) from customer"},
		{"select /*abc='xyz'*/ sleep(:vtg1) from customer", "select sleep(?) from customer"},
	}
	ii := Insights{}
	re := regexp.MustCompile(`<id>|<values>|<elements>`)
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tc.input)
			assert.NoError(t, err)

			out, _ := ii.normalizeSQL(stmt, true)
			assert.Equal(t, tc.output, out)

			// Most normalized queries should be legal to parse again.
			// Both Boost and the Insights EXPLAIN feature need this.
			// But skip normalized queries with <id>, <values>, or <elements>,
			// which we know can't be re-parsed.
			if !re.MatchString(tc.output) {
				stmt, err = sqlparser.Parse(tc.input)
				assert.NoError(t, err)
				out, _ = ii.normalizeSQL(stmt, true)
				assert.Equal(t, tc.output, out)
			}
		})
	}
}

func TestNilAST(t *testing.T) {
	// vtgate should give us an AST whenever ls.IsNormalized is true or ls.Error is nil.
	// This unit test covers the unexpected case where it doesn't, where we would want an "<error>"
	// as our query pattern, rather than a nil-dereference panic.
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select * from aaa where id = :vtg1", responseTime: 5 * time.Second, nilAST: false},
			{sql: "select * from bbb where id = :vtg1", responseTime: 5 * time.Second, nilAST: true},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic,
				`normalized_sql:{value:\"select * from aaa where id = ?\"}`),
			expect(queryTopic,
				`<error>`),
			expect(queryStatsBundleTopic).count(2),
		})
}

func TestStringTruncation(t *testing.T) {
	testCases := []struct {
		in, out string
	}{
		{"", ""},
		{"1234567890", "12345678"},
		{"1234567😂", "1234567"},
		{"123456😂", "123456"},
		{"12345😂", "12345"},
		{"1234😂", "1234😂"},
		{"123😂", "123😂"},
		{"12😂", "12😂"},
		{"1😂", "1😂"},
		{"😂", "😂"},
		{"1234567😂8", "1234567"},
		{"123456😂7", "123456"},
		{"12345😂6", "12345"},
		{"1234😂5", "1234😂"},
		{"123😂4", "123😂4"},
		{"12😂3", "12😂3"},
		{"1😂2", "1😂2"},
		{"😂1", "😂1"},
		{"123456\xcf\xcf\xcf\xcf", "123456\xcf\xcf"}, // invalid UTF-8
		{"123456\x8f\x8f\x8f\x8f", "123456\x8f\x8f"}, // invalid UTF-8
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			op := safelyTruncate(tc.in, 8)
			assert.Equal(t, tc.out, op)
			assert.LessOrEqual(t, len(op), 8)

			op = efficientlyTruncate(tc.in, 8)
			assert.Equal(t, tc.out, op)
			assert.LessOrEqual(t, len(op), 8)
		})
	}
}

func TestRawQueries(t *testing.T) {
	var maxRawQuerySize uint = 32
	insightsTestHelper(t, true, setupOptions{maxRawQuerySize: &maxRawQuerySize},
		[]insightsQuery{
			{sql: "select * from users where id = :vtg1", responseTime: 5 * time.Second,
				rawSQL: "select * from users where id=7"}, // no change
			{sql: "select * from users where id = :vtg1 and email = :vtg2", responseTime: 5 * time.Second,
				rawSQL: "select * from users where id=8 and email='alice@sample.com'"}, // truncates after `id=8 a`
			{sql: "insert into foo values (:v1,:v2),(:v3,:v4),(:v5,:v6)", responseTime: 5 * time.Second,
				rawSQL: "insert into foo values (1, 2), (3, 4), (5, 6)"}, // cleanly summarizes
			{sql: "insert into foo values (:v1,:v2)", responseTime: 5 * time.Second,
				rawSQL: "insert into foo values ('😂', 'bob')"}, // truncates after `😂', `
			{sql: "update foo set a = :vtg1 where id = :vtg2", responseTime: 5 * time.Second,
				rawSQL: "update foo set a=1 where id=7"}, // no change
			{sql: "update foo set a = :vtg1 where id = :vtg2", responseTime: 5 * time.Second,
				rawSQL: "update foo set a=1 where id='6😂7890'"}, // trucates after `id='6` without splitting the 😂
		},
		[]insightsKafkaExpectation{
			expect(queryTopic,
				`normalized_sql:{value:\"select * from users where id = ?\"}`,
				`raw_sql:{value:\"select * from users where id=7\"}`).butNot("raw_sql_abbreviation"),
			expect(queryTopic,
				`normalized_sql:{value:\"select * from users where id = ? and email = ?\"}`,
				`raw_sql:{value:\"select * from users where id=8 a\"}`,
				"raw_sql_abbreviation:TRUNCATED").butNot("alice"),
			expect(queryTopic,
				`normalized_sql:{value:\"insert into foo values <values>\"}`,
				`raw_sql:{value:\"insert into foo values (1, 2)\"}`,
				"raw_sql_abbreviation:SUMMARIZED").butNot("),", "(3, 4)", "(5, 6)"),
			expect(queryTopic,
				`normalized_sql:{value:\"insert into foo values <values>\"}`,
				`raw_sql:{value:\"insert into foo values ('😂', \"}`,
				"raw_sql_abbreviation:TRUNCATED").butNot("bob"),
			expect(queryTopic,
				`normalized_sql:{value:\"update foo set a = ? where id = ?\"}`,
				`raw_sql:{value:\"update foo set a=1 where id=7\"}`).butNot("raw_sql_abbreviation"),
			expect(queryTopic,
				`normalized_sql:{value:\"update foo set a = ? where id = ?\"}`,
				`raw_sql:{value:\"update foo set a=1 where id='6\"}`,
				"raw_sql_abbreviation:TRUNCATED").butNot("67890"),
			expect(queryStatsBundleTopic).count(4),
		})
}

func TestNotNormalizedNotError(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			// successful, normalized
			{sql: "select * from users where id = :vtg1", responseTime: 5 * time.Second, normalized: YES,
				rawSQL: "select * from users where id=7"},

			// successful, not normalized => statement is sent
			{sql: "begin", responseTime: 5 * time.Second, normalized: NO,
				rawSQL: "begin"},

			// successful, not normalized => statement is sent, downcased, tags are parsed
			{sql: "roLlBacK /*abc='xyz'*/", responseTime: 5 * time.Second, normalized: NO,
				rawSQL: "roLlBacK /*abc='xyz'*/"},

			// error, normalized => query is interesting, statement is sent
			{sql: "select * from orders", normalized: YES, error: "no such table",
				rawSQL: "select * from orders"},

			// error, not normalized => query is interesting, statement replaced with "<error>"
			{sql: "hello world", normalized: NO, error: "syntax error",
				rawSQL: "hello world"},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, `normalized_sql:{value:\"select * from users where id = ?\"}`,
				`raw_sql:{value:\"select * from users where id=7\"}`),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"select * from users where id = ?\"}`),
			expect(queryTopic, `normalized_sql:{value:\"begin\"}`,
				`raw_sql:{value:\"begin\"}`),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"begin\"}`),
			expect(queryTopic, `normalized_sql:{value:\"rollback\"}`, "xyz",
				`raw_sql:{value:\"roLlBacK /*abc='xyz'*/\"}`,
				`key:\"abc\" value:\"xyz\"`),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"rollback\"}`).butNot("xyz"),
			expect(queryTopic, `normalized_sql:{value:\"select * from orders\"}`, "no such table",
				`raw_sql:{value:\"select * from orders\"}`).butNot("<error>"),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"select * from orders\"}`).butNot("no such table", "<error>"),
			expect(queryTopic, `normalized_sql:{value:\"<error>\"}`, "syntax error",
				`raw_sql:{value:\"hello world\"}`),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"<error>\"}`).butNot("hello", "syntax error"),
		})
}

func TestRawQueryShortening(t *testing.T) {
	testCases := []struct {
		input, output, errStr string
		limit                 uint
	}{
		{
			input:  "select * from users",
			output: "select * from users",
			limit:  64,
		},
		{
			input:  "select * from users",
			errStr: "raw SQL string is still too long",
			limit:  8,
		},
		{
			input:  "insert into users values (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
			output: "insert into users values (1, 'Alice')",
			limit:  64,
		},
		{
			input:  "insert into users values (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
			errStr: "raw SQL string is still too long",
			limit:  8,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			op, err := shortenRawSQL(tc.input, tc.limit)
			assert.Equal(t, tc.output, op)
			if tc.errStr != "" {
				require.Error(t, err)
				assert.Equal(t, tc.errStr, err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestErrorNormalization(t *testing.T) {
	testCases := []struct {
		name, input, output string
	}{
		{
			name:   "Normalizes elapsed time and query id",
			input:  "vttablet: rpc error: code = Canceled desc = (errno 2013) due to context deadline exceeded, elapsed time: 20.000937445s, killing query ID 135243 (CallerID: planetscale-admin)",
			output: "vttablet: rpc error: code = Canceled desc = (errno 2013) due to context deadline exceeded, elapsed time: <time>, killing query ID <id>",
		},
		{
			name:   "Normalizes transaction id and ended at time",
			input:  "vttablet: rpc error: code = Aborted desc = transaction 1656362196194291371: ended at 2022-06-30 20:34:09.614 UTC (unlocked closed connection) (CallerID: planetscale-admin)",
			output: "vttablet: rpc error: code = Aborted desc = transaction <transaction>: ended at <time> (unlocked closed connection)",
		},
		{
			name:   "Normalizes connection id",
			input:  "target: taobench-8.20-40.primary: vttablet: rpc error: code = Unavailable desc = conn 299286: Write(packet) failed: write unix @->/vt/socket/mysql.sock: write: broken pipe (errno 2006) (sqlstate HY000) (CallerID: planetscale-admin)",
			output: "target: taobench-8.20-40.primary: vttablet: rpc error: code = Unavailable desc = conn <conn>: Write(packet) failed: write unix @->/vt/socket/mysql.sock: write: broken pipe (errno 2006) (sqlstate HY000)",
		},
		{
			name:   "Normalizes the the table path",
			input:  "target: gomy_backend_production.-.primary: vttablet: rpc error: code = ResourceExhausted desc = The table '/vt/vtdataroot/vt_0955681468/tmp/#sql351_1df_2' is full (errno 1114) (sqlstate HY000) (CallerID: planetscale-admin)",
			output: "target: gomy_backend_production.-.primary: vttablet: rpc error: code = ResourceExhausted desc = The table <table> is full (errno 1114) (sqlstate HY000)",
		},
		{
			name:   "Normalizes the row number",
			input:  "transaction rolled back to reverse changes of partial DML execution: target: targ.a8-b0.primary: vttablet: rpc error: code = InvalidArgument desc = Data too long for column 'media_url' at row 2 (errno 1406) (sqlstate 22001)",
			output: "transaction rolled back to reverse changes of partial DML execution: target: targ.a8-b0.primary: vttablet: rpc error: code = InvalidArgument desc = Data too long for column 'media_url' at row <row> (errno 1406) (sqlstate 22001)",
		},
		{
			name:   "Normalizes syntax error position",
			input:  "syntax error at position 29",
			output: "syntax error at position <position>",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			normalized := normalizeError(tc.input)
			assert.Equal(t, tc.output, normalized)
		})
	}
}

func TestTotalDurationSketches(t *testing.T) {
	alpha := 0.01
	units := -6

	insightsTestHelper(t, true, setupOptions{totalDurationSketchAlpha: &alpha, totalDurationSketchUnits: &units},
		[]insightsQuery{
			{sql: "select * from foo", responseTime: 0 * time.Microsecond},
			{sql: "select * from foo", responseTime: 1 * time.Microsecond},
			{sql: "select * from foo", responseTime: 10 * time.Microsecond},
			{sql: "select * from foo", responseTime: 100 * time.Microsecond},
			{sql: "select * from foo", responseTime: 100 * time.Microsecond},
			{sql: "select * from foo", responseTime: 100 * time.Millisecond},
			{sql: "select * from foo", responseTime: 1 * time.Second},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, "select * from foo",
				"total_duration_sketch:{gamma:1.020202 units:-6 sum:1.100211e+06 count:7",
				"buckets:{key:0 value:2}",
				"buckets:{key:116 value:1}",
				"buckets:{key:231 value:2}",
				"buckets:{key:576 value:1}",
				"buckets:{key:691 value:1}"),
		})

	alpha = 0.05
	units = -3

	insightsTestHelper(t, true, setupOptions{totalDurationSketchAlpha: &alpha, totalDurationSketchUnits: &units},
		[]insightsQuery{
			{sql: "select * from foo", responseTime: 0 * time.Microsecond},
			{sql: "select * from foo", responseTime: 1 * time.Microsecond},
			{sql: "select * from foo", responseTime: 10 * time.Microsecond},
			{sql: "select * from foo", responseTime: 100 * time.Microsecond},
			{sql: "select * from foo", responseTime: 100 * time.Microsecond},
			{sql: "select * from foo", responseTime: 100 * time.Millisecond},
			{sql: "select * from foo", responseTime: 1 * time.Second},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, "select * from foo",
				"total_duration_sketch:{gamma:1.1052631 units:-3 sum:1100.211 count:7",
				"buckets:{key:0 value:5}",
				"buckets:{key:47 value:1}",
				"buckets:{key:70 value:1}"),
		})
}

func TestTotalDurationSketchesDisabled(t *testing.T) {
	sketchesEnabled := true
	insightsTestHelper(t, true, setupOptions{queryTimeSketch: &sketchesEnabled},
		[]insightsQuery{
			{sql: "select * from foo", responseTime: 10 * time.Microsecond},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, "select * from foo", "total_duration_sketch"),
		})

	sketchesEnabled = false
	insightsTestHelper(t, true, setupOptions{queryTimeSketch: &sketchesEnabled},
		[]insightsQuery{
			{sql: "select * from foo", responseTime: 10 * time.Microsecond},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, "select * from foo").butNot("total_duration_sketch"),
		})
}

const (
	AUTO = iota
	NO
	YES
)

type insightsQuery struct {
	sql, error, rawSQL, keyspace, activeKeyspace string
	responseTime                                 time.Duration
	rowsRead                                     int

	// insightsTestHelper sets ls.IsNormalized to false for errors, true otherwise.
	// Set normalized=YES or normalized=NO to override this, e.g., to simulate an error
	// that occurs after query parsing has succeeded, or to simulate a successful
	// statement that did not need to be normalized.
	normalized int

	nilAST bool

	boostQueryID string
	method       string
}

type insightsKafkaExpectation struct {
	patterns     []string
	antipatterns []string
	topic        string
	want, found  int
}

func expect(topic string, patterns ...string) insightsKafkaExpectation {
	return insightsKafkaExpectation{
		patterns: patterns,
		topic:    topic,
		want:     1,
	}
}

func (ike insightsKafkaExpectation) butNot(anti ...string) insightsKafkaExpectation {
	ike.antipatterns = append(ike.antipatterns, anti...)
	return ike
}

func (ike insightsKafkaExpectation) count(n int) insightsKafkaExpectation {
	ike.want = n
	return ike
}

func insightsTestHelper(t *testing.T, mockTimer bool, options setupOptions, queries []insightsQuery, expect []insightsKafkaExpectation) {
	t.Helper()
	insights, err := setup(t, "localhost:1234", "mumblefoo", "", "", options)
	require.NoError(t, err)
	if options.maxRawQuerySize != nil {
		insights.MaxRawQueryLength = *options.maxRawQuerySize
	}
	insights.Sender = func(buf []byte, topic, key string) error {
		assert.Contains(t, string(buf), "mumblefoo", "database branch public ID not present in message body")
		assert.True(t, strings.HasPrefix(key, "mumblefoo"), "key has unexpected form %q", key)
		assert.Contains(t, string(buf), queryURLBase+"/"+topic, "expected key not present in message body")
		var found bool
		for i, ex := range expect {
			matchesAll := true
			if topic == ex.topic {
				for _, p := range ex.patterns {
					if !strings.Contains(string(buf), p) {
						matchesAll = false
						break
					}
				}

				for _, ap := range ex.antipatterns {
					if strings.Contains(string(buf), ap) {
						matchesAll = false
						break
					}
				}

				if matchesAll {
					expect[i].found++
					found = true
					break
				}
			}
		}
		assert.True(t, found, "no pattern expects topic=%q buf=%q", topic, string(buf))
		return nil
	}
	now := time.Now()

	for _, q := range queries {
		if q.method == "" {
			q.method = "Execute"
		}

		ls := &logstats.LogStats{
			Method:       q.method,
			SQL:          q.sql,
			RawSQL:       q.rawSQL,
			IsNormalized: q.normalized == YES || (q.error == "" && q.normalized == AUTO),
			StartTime:    now.Add(-q.responseTime),
			EndTime:      now,
			RowsRead:     uint64(q.rowsRead),
			Ctx: callerid.NewContext(context.Background(), &vtrpcpb.CallerID{
				// Principal must match the roles used for ACLs
				Principal:    "planetscale-reader",
				Component:    "127.0.0.1", // TODO
				Subcomponent: "PSDB API",
			}, nil),
			Table:          options.tableString,
			Keyspace:       q.keyspace,
			ActiveKeyspace: q.activeKeyspace,
			BoostQueryID:   q.boostQueryID,
		}
		if q.error != "" {
			ls.Error = errors.New(q.error)
		} else {
			ls.StmtType = sqlparser.Preview(q.sql).String()
		}

		if (ls.IsNormalized || ls.Error == nil) && !q.nilAST {
			ls.AST, err = sqlparser.Parse(q.sql)
			assert.NoError(t, err)
		}
		logger.Send(ls)
	}
	if mockTimer {
		insights.MockTimer()
	}
	require.True(t, insights.Drain(), "did not drain")
	for _, ex := range expect {
		assert.Equal(t, ex.want, ex.found, "count for %+v was wrong", ex)
	}
}

var (
	lsSlowQuery = &logstats.LogStats{
		SQL:          "select sleep(:vtg1)",
		IsNormalized: true,
		StartTime:    time.Now().Add(-5 * time.Second),
		EndTime:      time.Now(),
		Ctx: callerid.NewContext(context.Background(), &vtrpcpb.CallerID{
			// Principal must match the roles used for ACLs
			Principal:    "planetscale-reader",
			Component:    "127.0.0.1", // TODO
			Subcomponent: "PSDB API",
		}, nil),
	}
)

func realize(t *testing.T, ls *logstats.LogStats) *logstats.LogStats {
	if ls.RawSQL == "" {
		ls.RawSQL = ls.SQL
	}
	if ls.AST == nil {
		var err error
		ls.AST, err = sqlparser.Parse(ls.SQL)
		if err != nil {
			t.Log(err)
		}
	}
	return ls
}
