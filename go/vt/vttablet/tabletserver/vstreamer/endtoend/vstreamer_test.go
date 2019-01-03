/*
Copyright 2018 The Vitess Authors.

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

package vstreamer

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type testcase struct {
	input  interface{}
	output [][]string
}

func TestStatements(t *testing.T) {
	execStatements(t, []string{
		"create table stream1(id int, val varbinary(128), primary key(id))",
		"create table stream2(id int, val varbinary(128), primary key(id))",
	})
	framework.Server.ReloadSchema(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}

	testcases := []testcase{{
		input: []string{
			"insert into stream1 values (1, 'aaa')",
			"update stream1 set val='bbb' where id = 1",
		},
		output: [][]string{{
			`gtid`,
			`begin`,
			`type:ROW row_event:<table_name:"stream1" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`type:ROW row_event:<table_name:"stream1" row_changes:<before:<lengths:1 lengths:3 values:"1aaa" > after:<lengths:1 lengths:3 values:"1bbb" > > > `,
			`commit`,
		}},
	}, {
		// Normal DDL.
		input: "alter table stream1 change column val val varbinary(128)",
		output: [][]string{{
			`gtid`,
			`type:DDL ddl:"alter table stream1 change column val val varbinary(128)" `,
		}},
	}, {
		// DDL padded with comments.
		input: " /* prefix */ alter table stream1 change column val val varbinary(256) /* suffix */ ",
		output: [][]string{{
			`gtid`,
			`type:DDL ddl:"/* prefix */ alter table stream1 change column val val varbinary(256) /* suffix */" `,
		}},
	}, {
		// Multiple tables, and multiple rows changed per statement.
		input: []string{
			"insert into stream1 values (2, 'bbb')",
			"insert into stream2 values (1, 'aaa')",
			"update stream1 set val='ccc'",
			"delete from stream1",
		},
		output: [][]string{{
			`gtid`,
			`begin`,
			`type:ROW row_event:<table_name:"stream1" row_changes:<after:<lengths:1 lengths:3 values:"2bbb" > > > `,
			`type:ROW row_event:<table_name:"stream2" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`type:ROW row_event:<table_name:"stream1" ` +
				`row_changes:<before:<lengths:1 lengths:3 values:"1bbb" > after:<lengths:1 lengths:3 values:"1ccc" > > ` +
				`row_changes:<before:<lengths:1 lengths:3 values:"2bbb" > after:<lengths:1 lengths:3 values:"2ccc" > > > `,
			`type:ROW row_event:<table_name:"stream1" ` +
				`row_changes:<before:<lengths:1 lengths:3 values:"1ccc" > > ` +
				`row_changes:<before:<lengths:1 lengths:3 values:"2ccc" > > > `,
			`commit`,
		}},
	}, {
		// truncate is a DDL
		input: "truncate table stream2",
		output: [][]string{{
			`gtid`,
			`type:DDL ddl:"truncate table stream2" `,
		}},
	}, {
		// repair, optimize and analyze show up in binlog stream, but ignored by vitess.
		input: "repair table stream2",
	}, {
		input: "optimize table stream2",
	}, {
		input: "analyze table stream2",
	}, {
		// select, set, show, analyze and describe don't get logged.
		input: "select * from stream1",
	}, {
		input: "set @val=1",
	}, {
		input: "show tables",
	}, {
		input: "analyze table stream1",
	}, {
		input: "describe stream1",
	}}
	runCases(t, filter, testcases)
}

func TestDDLAddColumn(t *testing.T) {
	execStatement(t, "create table ddl_test(id int, val varbinary(128), primary key(id))")

	// Record position before the next few statements.
	pos, err := framework.Mysqld.MasterPosition()
	if err != nil {
		t.Fatal(err)
	}
	execStatements(t, []string{
		"insert into ddl_test values(1, 'aaa')",
		// Adding columns is allowed.
		"alter table ddl_test add column val2 varbinary(128)",
		"insert into ddl_test values(2, 'bbb', 'ccc')",
	})
	framework.Server.ReloadSchema(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}

	ch := make(chan []*binlogdatapb.VEvent)
	go func() {
		err := framework.Server.VStream(ctx, &framework.Target, pos, filter, func(evs []*binlogdatapb.VEvent) error {
			t.Logf("evs: %v\n", evs)
			ch <- evs
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		close(ch)
	}()
	expectLog(t, "ddls", ch, [][]string{{
		// Current schema has 3 columns, but they'll be truncated to match the two columns in the event.
		`gtid`,
		`begin`,
		`type:ROW row_event:<table_name:"ddl_test" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
		`commit`,
	}, {
		`gtid`,
		`type:DDL ddl:"alter table ddl_test add column val2 varbinary(128)" `,
	}, {
		// The plan will be updated to now include the third column
		// because the new table map will have three columns.
		`gtid`,
		`begin`,
		`type:ROW row_event:<table_name:"ddl_test" row_changes:<after:<lengths:1 lengths:3 lengths:3 values:"2bbbccc" > > > `,
		`commit`,
	}})
}

func runCases(t *testing.T, filter *binlogdatapb.Filter, testcases []testcase) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := startStream(ctx, t, filter)

	for _, tcase := range testcases {
		switch input := tcase.input.(type) {
		case []string:
			execTransaction(t, input)
		case string:
			execStatement(t, input)
		default:
			t.Fatalf("unexpected input: %#v", input)
		}
		expectLog(t, tcase.input, ch, tcase.output)
	}
	cancel()
	if evs, ok := <-ch; ok {
		t.Fatalf("unexpected evs: %v", evs)
	}
}

func expectLog(t *testing.T, input interface{}, ch <-chan []*binlogdatapb.VEvent, output [][]string) {
	t.Helper()

	for _, wantset := range output {
		evs := <-ch
		if len(wantset) != len(evs) {
			t.Fatalf("%v: evs\n%v, want\n%v", input, evs, wantset)
		}
		for i, want := range wantset {
			switch want {
			case "gtid":
				if evs[i].Type != binlogdatapb.VEventType_GTID {
					t.Fatalf("%v (%d): event: %v, want gtid", input, i, evs[i])
				}
			case "begin":
				if evs[i].Type != binlogdatapb.VEventType_BEGIN {
					t.Fatalf("%v (%d): event: %v, want begin", input, i, evs[i])
				}
			case "commit":
				if evs[i].Type != binlogdatapb.VEventType_COMMIT {
					t.Fatalf("%v (%d): event: %v, want commit", input, i, evs[i])
				}
			default:
				if got := fmt.Sprintf("%v", evs[i]); got != want {
					t.Fatalf("%v (%d): event:\n%q, want\n%q", input, i, got, want)
				}
			}
		}
	}
}

func startStream(ctx context.Context, t *testing.T, filter *binlogdatapb.Filter) <-chan []*binlogdatapb.VEvent {
	pos, err := framework.Mysqld.MasterPosition()
	if err != nil {
		t.Fatal(err)
	}

	ch := make(chan []*binlogdatapb.VEvent)
	go func() {
		err := framework.Server.VStream(ctx, &framework.Target, pos, filter, func(evs []*binlogdatapb.VEvent) error {
			t.Logf("evs: %v\n", evs)
			ch <- evs
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		close(ch)
	}()
	return ch
}

func execTransaction(t *testing.T, queries []string) {
	t.Helper()

	client := framework.NewClient()
	if err := client.Begin(false); err != nil {
		t.Fatal(err)
	}
	for _, query := range queries {
		if _, err := client.Execute(query, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Commit(); err != nil {
		t.Fatal(err)
	}
}

func execStatement(t *testing.T, query string) {
	t.Helper()
	if err := framework.Mysqld.ExecuteSuperQuery(context.Background(), query); err != nil {
		t.Fatal(err)
	}
}

func execStatements(t *testing.T, queries []string) {
	t.Helper()
	if err := framework.Mysqld.ExecuteSuperQueryList(context.Background(), queries); err != nil {
		t.Fatal(err)
	}
}
