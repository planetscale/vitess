package watcher

import (
	"bytes"
	"errors"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/sqlparser"
)

var errMismatch = errors.New("not matched")

func GenerateBoundsForQuery(stmt sqlparser.Statement, keySchema []*querypb.Field) (bounds []*vtboostpb.Materialization_Bound, fullyMaterialized bool) {
	var arguments int
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName, sqlparser.TableName:
			// Common node types that never contain expressions but create a lot of object
			// allocations.
			return false, nil
		case sqlparser.Argument:
			pos := slices.IndexFunc(keySchema, func(f *querypb.Field) bool {
				return f.Name == string(node) && (f.Flags&uint32(querypb.MySqlFlag_MULTIPLE_KEY_FLAG)) == 0
			})
			if pos < 0 {
				panic("did not find placeholder in Key Schema")
			}
			bounds = append(bounds, &vtboostpb.Materialization_Bound{Name: string(node), Pos: int64(pos)})
			arguments++
		case sqlparser.ListArg:
			pos := slices.IndexFunc(keySchema, func(f *querypb.Field) bool {
				return f.Name == string(node) && (f.Flags&uint32(querypb.MySqlFlag_MULTIPLE_KEY_FLAG)) != 0
			})
			if pos < 0 {
				panic("did not find placeholder in Key Schema")
			}
			bounds = append(bounds, &vtboostpb.Materialization_Bound{Name: string(node), Pos: int64(pos), Multi: true})
			arguments++
		case *sqlparser.Literal:
			bindVar := sqlparser.SQLToBindvar(node)
			bounds = append(bounds, &vtboostpb.Materialization_Bound{Name: "@literal", Type: int64(bindVar.Type), BoundValue: bindVar.Value})
		}
		return true, nil
	}, stmt)

	if arguments == 0 {
		if len(keySchema) != 1 || keySchema[0].Name != "bogokey" {
			panic("fully materialized view without bogokey")
		}
		fullyMaterialized = true
	}
	return
}

// WHERE col1 = :foo AND col2 IN (a, b, c)
// => col1 = :foo AND col2 = a
// => col1 = :foo AND col2 = b

func matchParametrizedQuery(keyOut []*querypb.BindVariable, stmt sqlparser.Statement, bvars map[string]*querypb.BindVariable, bounds []*vtboostpb.Materialization_Bound) bool {
	var pos int

	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName, sqlparser.TableName:
			// Common node types that never contain expressions but create a lot of object
			// allocations.
			return false, nil
		case sqlparser.Argument:
			if pos == len(bounds) {
				return false, errMismatch
			}
			bound := bounds[pos]
			pos++

			bv2, ok := bvars[string(node)]
			if !ok {
				return false, errMismatch
			}

			if bound.BoundValue != nil {
				if sqltypes.Type(bound.Type) != bv2.Type || !bytes.Equal(bound.BoundValue, bv2.Value) {
					return false, errMismatch
				}
			} else {
				keyOut[bound.Pos] = bv2
			}
		case sqlparser.ListArg:
			if pos == len(bounds) {
				return false, errMismatch
			}
			bound := bounds[pos]
			pos++

			bv2, ok := bvars[string(node)]
			if !ok {
				return false, errMismatch
			}
			if bv2.Type != sqltypes.Tuple {
				return false, errMismatch
			}
			if bound.BoundValue != nil {
				panic("BoundValue in tuple?")
			}
			keyOut[bound.Pos] = bv2
		}
		return true, nil
	}, stmt)
	return err == nil && pos == len(bounds)
}

func ParametrizeQuery(q sqlparser.Statement) string {
	var buf = sqlparser.NewTrackedBuffer(func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node.(type) {
		case sqlparser.Argument, sqlparser.ListArg, *sqlparser.Literal:
			buf.WriteByte('?')
		case *sqlparser.ParsedComments:
		default:
			node.Format(buf)
		}
	})
	buf.WriteNode(q)
	return buf.String()
}
