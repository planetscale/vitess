package vtgate

import (
	"testing"

	pbvtgate "github.com/planetscale/psevents/go/vtgate/v1"

	"github.com/stretchr/testify/assert"
)

func TestCommentSplitting(t *testing.T) {
	testCases := []struct {
		input    string
		comments []string
	}{
		{
			"hello world /* comment */",
			[]string{"comment"},
		},
		{
			"hello world /* unterminated comment",
			[]string{"unterminated comment"},
		},
		{
			"before/* comment */after",
			[]string{"comment"},
		},
		{
			"/* now */ hello /* three */ world /* comments */",
			[]string{"now", "three", "comments"},
		},
		{
			"/*/*/*/*///***/*/***///**/",
			[]string{"/", "///**", "*", ""},
		},
		{
			" no\tcomments\t",
			nil,
		},
		{
			"",
			nil,
		},
		{
			// We don't split on `--` because that style of comments gets split off before the
			// string is copied into LogStats.SQL.  Only `/* ... */` comments show up in LogStats.SQL.
			"we don't -- split these",
			nil,
		},
		{
			"select /*+ SET_VAR(sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO') */ schema_snapshot.* from schema_snapshot where schema_snapshot.ready = true and schema_snapshot.migration_snapshot_applied_at is null and created_at > :vtg1 and schema_snapshot.migration_snapshot_public_id is not null and schema_snapshot.deleted_at is null order by schema_snapshot.id asc limit :vtg2",
			[]string{"+ SET_VAR(sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO')"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			c := extractComments(tc.input)
			assert.Equal(t, tc.comments, c)
		})
	}
}

func TestCommentTags(t *testing.T) {
	testCases := []struct {
		name, input string
		tags        []*pbvtgate.Query_Tag
	}{
		{
			"sqlcommenter",
			`INSERT INTO "polls_question" ("question_text", "pub_date") VALUES ('What is this?', '2019-05-28T18:54:50.767481+00:00'::timestamptz) RETURNING "polls_question"."id" /*controller='index',db_driver='django.db.backends.postgresql',framework='django%3A2.2.1',route='%5Epolls/%24',traceparent='00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01',tracestate='congo%3Dt61rcWkgMzE%2Crojo%3D00f067aa0ba902b7'*/`,
			[]*pbvtgate.Query_Tag{
				{Key: "controller", Value: "index"},
				{Key: "db_driver", Value: "django.db.backends.postgresql"},
				{Key: "framework", Value: "django:2.2.1"},
				{Key: "route", Value: "^polls/$"},
				{Key: "traceparent", Value: "00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01"},
				{Key: "tracestate", Value: "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7"},
			},
		},
		{
			"interior comments, not sqlcommenter",
			"select /*+ SET_VAR(sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO') */ schema_snapshot.* from schema_snapshot where schema_snapshot.ready = true and schema_snapshot.migration_snapshot_applied_at is null and created_at > :vtg1 and schema_snapshot.migration_snapshot_public_id is not null and schema_snapshot.deleted_at is null order by schema_snapshot.id asc limit :vtg2",
			nil,
		},
		{
			"ugly",
			` /*one='1' , two='2' */ SELECT /* th%2dree= ' 3 '*/* FROM hello/*	four='4\'s a great n\umber'*//* five ='5\\cinco' ,, six='%foo'  */`,
			[]*pbvtgate.Query_Tag{
				{Key: "one", Value: "1"},
				{Key: "two", Value: "2"},
				{Key: "th-ree", Value: " 3 "},
				{Key: "four", Value: "4's a great number"},
				{Key: "five", Value: `5\cinco`},
				{Key: "six", Value: "%foo"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comments := extractComments(tc.input)
			tags := parseCommentTags(comments)
			assert.Equal(t, tc.tags, tags)
		})
	}
}

func TestMoreCommentTags(t *testing.T) {
	testCases := []struct {
		comment string
		expect  []*pbvtgate.Query_Tag
	}{
		{
			"unterminated='", nil,
		},
		{
			"noequals", nil,
		},
		{
			"this is the sort of normal SQL comment that doesn't contain any tags: http://sample.com", nil,
		},
		{
			"almost=valid", nil, // single quotes are required
		},
		{
			"junk='after'the closing quotation mark is ignored",
			[]*pbvtgate.Query_Tag{
				{Key: "junk", Value: "after"},
			},
		},
		{
			"another normal SQL comment, with an = sign in it", nil,
		},
		{
			",internal-bonus='commas',,,are='skipped',", []*pbvtgate.Query_Tag{
				{Key: ",internal-bonus", Value: "commas"},
				{Key: "are", Value: "skipped"},
			},
		},
		{
			"commas='with', ,spaces='are-bad'", []*pbvtgate.Query_Tag{
				{Key: "commas", Value: "with"},
				{Key: ",spaces", Value: "are-bad"},
			},
		},
		{
			"space='before' ,comma='fine'", []*pbvtgate.Query_Tag{
				{Key: "space", Value: "before"},
				{Key: "comma", Value: "fine"},
			},
		},
		{
			"space='after', comma='fine'", []*pbvtgate.Query_Tag{
				{Key: "space", Value: "after"},
				{Key: "comma", Value: "fine"},
			},
		},
		{
			"the='first',error=,terminates='parsing'", []*pbvtgate.Query_Tag{
				{Key: "the", Value: "first"},
			},
		},
		{
			"empty='',values='',='and',='keys'", []*pbvtgate.Query_Tag{
				{Key: "empty", Value: ""},
				{Key: "values", Value: ""},
				{Key: "", Value: "and"},
				{Key: "", Value: "keys"},
			},
		},
		{
			`ends-with-backslash='foo\`, nil,
		},
		{
			`ends-with-escaped-backslash='foo\\`, nil,
		},
		{
			`ends-with-escaped-quote='foo\'`, nil,
		},
		{
			`escaped-unterminated-bare='\'`, nil,
		},
		{
			`escaped-unterminated-more='\'more`, nil,
		},
		{
			"", nil,
		},
		{
			`good-escape='foo\'bar\\baz',x='y'`, []*pbvtgate.Query_Tag{
				{Key: "good-escape", Value: `foo'bar\baz`},
				{Key: "x", Value: "y"},
			},
		},
		{
			`foo='\\\\\'\\\\\'\\'`, []*pbvtgate.Query_Tag{
				{Key: "foo", Value: `\\'\\'\`},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.comment, func(t *testing.T) {
			got := parseCommentTags([]string{tc.comment})
			assert.Equal(t, tc.expect, got)
		})
	}
}