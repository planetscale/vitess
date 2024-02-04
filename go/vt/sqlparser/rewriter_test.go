/*
Copyright 2021 The Vitess Authors.

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

package sqlparser

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"testing"
	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func BenchmarkVisitLargeExpression(b *testing.B) {
	gen := NewGenerator(rand.New(rand.NewSource(1)), 5)
	exp := gen.Expression(ExprGeneratorConfig{})

	depth := 0
	for i := 0; i < b.N; i++ {
		_ = Rewrite(exp, func(cursor *Cursor) bool {
			depth++
			return true
		}, func(cursor *Cursor) bool {
			depth--
			return true
		})
	}
}

func TestReplaceWorksInLaterCalls(t *testing.T) {
	q := "select * from tbl1"
	parser := NewTestParser()
	stmt, err := parser.Parse(q)
	require.NoError(t, err)
	count := 0
	Rewrite(stmt, func(cursor *Cursor) bool {
		switch node := cursor.Node().(type) {
		case *Select:
			node.SelectExprs[0] = &AliasedExpr{
				Expr: NewStrLiteral("apa"),
			}
			node.SelectExprs = append(node.SelectExprs, &AliasedExpr{
				Expr: NewStrLiteral("foobar"),
			})
		case *StarExpr:
			t.Errorf("should not have seen the star")
		case *Literal:
			count++
		}
		return true
	}, nil)
	assert.Equal(t, 2, count)
}

func TestReplaceAndRevisitWorksInLaterCalls(t *testing.T) {
	q := "select * from tbl1"
	parser := NewTestParser()
	stmt, err := parser.Parse(q)
	require.NoError(t, err)
	count := 0
	Rewrite(stmt, func(cursor *Cursor) bool {
		switch node := cursor.Node().(type) {
		case SelectExprs:
			if len(node) != 1 {
				return true
			}
			expr1 := &AliasedExpr{
				Expr: NewStrLiteral("apa"),
			}
			expr2 := &AliasedExpr{
				Expr: NewStrLiteral("foobar"),
			}
			cursor.ReplaceAndRevisit(SelectExprs{expr1, expr2})
		case *StarExpr:
			t.Errorf("should not have seen the star")
		case *Literal:
			count++
		}
		return true
	}, nil)
	assert.Equal(t, 2, count)
}

func TestChangeValueTypeGivesError(t *testing.T) {
	parser := NewTestParser()
	parse, err := parser.Parse("select * from a join b on a.id = b.id")
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			require.Equal(t, "[BUG] tried to replace 'On' on 'JoinCondition'", r)
		}
	}()
	_ = Rewrite(parse, func(cursor *Cursor) bool {
		_, ok := cursor.Node().(*ComparisonExpr)
		if ok {
			cursor.Replace(&NullVal{}) // this is not a valid replacement because the container is a value type
		}
		return true
	}, nil)

}

func TestTenantRouting(t *testing.T) {
	type testCase struct {
		query    string
		expected string
		err      bool
	}
	testCases := []testCase{
		{
			query:    "select * from t1 where id = 1 and tenant_id = 1",
			expected: "select * from target.t1 where id = 1 and tenant_id = 1",
		},
		{
			query:    "select * from t1 where id = 1 and tenant_id = 2",
			expected: "select * from source.t1 where id = 1 and tenant_id = 2",
		},
		{
			query:    "select * from t1 where id = 1 and tenant_id = 3",
			expected: "select * from unknown.t1 where id = 1 and tenant_id = 3",
		},
		{
			query:    "select * from t1, t2 where t1.id = t2.id and t1.tenant_id = 1 and t2.tenant_id = 1",
			expected: "select * from target.t1, target.t2 where target.t1.id = target.t2.id and target.t1.tenant_id = 1 and target.t2.tenant_id = 1",
		},
		{
			query:    "select * from t1 where tenant_id = 1 and id in (select id from t2 where tenant_id = 1)",
			expected: "select * from target.t1 where tenant_id = 1 and id in (select id from target.t2 where tenant_id = 1)",
		},
		//{
		//	query: "select * from t1, t2 where t1.id = t2.id and t1.tenant_id = 1 and t2.tenant_id = 2",
		//	err:   true,
		//},
	}
	for _, tc := range testCases {
		parser := NewTestParser()
		stmt, err := parser.Parse(tc.query)
		require.NoError(t, err)
		isTenant := false
		queryTenantId := 0
		tenantId := 0
		keyspace := ""
		var pre ApplyFunc = func(cursor *Cursor) bool {
			switch node := cursor.Node().(type) {
			case *ColName:
				if node.Name.EqualString("tenant_id") {
					isTenant = true
				}
			case *Literal:
				if isTenant {
					tenantId, err = strconv.Atoi(node.Val)
					require.NoError(t, err)
					if queryTenantId == 0 {
						queryTenantId = tenantId
					} else if queryTenantId != tenantId {
						// how do we signal an error?
						panic(vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "unsupported cross tenant query"))
					}
					switch tenantId {
					case 1:
						keyspace = "target"
					case 2:
						keyspace = "source"
					default:
						keyspace = "unknown"
					}
					return false
				}
			default:
			}
			return true
		}
		newStmt := Rewrite(stmt, pre, nil)
		var pre2 ApplyFunc = func(cursor *Cursor) bool {
			switch node := cursor.Node().(type) {
			case TableName:
				node.Qualifier = NewIdentifierCS(keyspace)
				cursor.Replace(node)
			default:
			}
			return true
		}
		newStmt = Rewrite(newStmt, pre2, nil)
		log.Infof("newStmt: %s", String(newStmt))
		assert.Equal(t, tc.expected, String(newStmt))
	}
}
