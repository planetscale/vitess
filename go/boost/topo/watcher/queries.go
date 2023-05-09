package watcher

import (
	"bytes"
	"errors"
	"fmt"

	"vitess.io/vitess/go/boost/server/controller/boostplan/viewplan"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/sqlparser"
)

var errMismatch = errors.New("not matched")

func GenerateBoundsForQuery(stmt sqlparser.Statement, plan *viewplan.Plan) (bounds []*vtboostpb.Materialization_Bind, fullyMaterialized bool, err error) {
	var arguments int
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName, sqlparser.TableName:
			// Common node types that never contain expressions but create a lot of object
			// allocations.
			return false, nil

		case *sqlparser.Argument:
			b := &vtboostpb.Materialization_Bind{
				Name: node.Name,
				Pos:  int64(arguments),
			}
			bounds = append(bounds, b)
			arguments++

		case sqlparser.ListArg:
			b := &vtboostpb.Materialization_Bind{
				Name: string(node),
				Pos:  int64(arguments),
			}
			bounds = append(bounds, b)
			arguments++

		case *sqlparser.Literal:
			bindVar := sqlparser.SQLToBindvar(node)
			b := &vtboostpb.Materialization_Bind{
				Name:    "@literal",
				Literal: bindVar,
			}
			bounds = append(bounds, b)
		}
		return true, nil
	}, stmt)

	if err != nil {
		return nil, false, err
	}

	if arguments == 0 {
		if len(plan.Parameters) != 1 || plan.Parameters[0].Name != "bogokey" {
			return nil, false, fmt.Errorf("unexpected schema for fully materialized view %v", plan.Parameters)
		}
		fullyMaterialized = true
	}
	return
}

// WHERE col1 = :foo AND col2 IN (a, b, c)
// => col1 = :foo AND col2 = a
// => col1 = :foo AND col2 = b

func matchParametrizedQuery(key []*querypb.BindVariable, stmt sqlparser.Statement, bvars map[string]*querypb.BindVariable, bounds []*vtboostpb.Materialization_Bind) bool {
	var pos int

	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName, sqlparser.TableName:
			// Common node types that never contain expressions but create a lot of object
			// allocations.
			return false, nil
		case *sqlparser.Argument:
			if pos == len(bounds) {
				return false, errMismatch
			}
			bound := bounds[pos]
			pos++

			bv2, ok := bvars[node.Name]
			if !ok {
				return false, errMismatch
			}

			if bound.Literal != nil {
				if bound.Literal.Type != bv2.Type || !bytes.Equal(bound.Literal.Value, bv2.Value) {
					return false, errMismatch
				}
				return true, nil
			}
			key[bound.Pos] = bv2
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
			if bound.Literal != nil {
				return false, fmt.Errorf("bound value for node %v in tuple: %v", sqlparser.String(node), bound)
			}
			key[bound.Pos] = bv2
		}
		return true, nil
	}, stmt)
	return err == nil && pos == len(bounds)
}

func ParametrizeQuery(q sqlparser.Statement) string {
	var buf = sqlparser.NewTrackedBuffer(func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node.(type) {
		case *sqlparser.Argument, sqlparser.ListArg, *sqlparser.Literal:
			buf.WriteByte('?')
		case *sqlparser.ParsedComments:
		default:
			node.Format(buf)
		}
	})
	buf.WriteNode(q)
	return buf.String()
}
