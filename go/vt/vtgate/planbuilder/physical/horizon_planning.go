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

package physical

import (
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

var ErrNotReadyForHorizon = fmt.Errorf("oh noes")

func checkIfWeCanShortCut(ctx *plancontext.PlanningContext, op abstract.PhysicalOperator, stmt sqlparser.SelectStatement) (abstract.PhysicalOperator, error) {
	route, isRoute := op.(*Route)
	if !isRoute && ctx.SemTable.NotSingleRouteErr != nil {
		// If we got here, we don't have a single shard plan
		return nil, ctx.SemTable.NotSingleRouteErr
	}

	if isRoute && route.IsSingleShard() && stmt.GetLimit() == nil {
		return planSingleShardRoute(ctx, route, stmt)
	}
	return nil, nil
}

func planSingleShardRoute(ctx *plancontext.PlanningContext, route *Route, stmt sqlparser.SelectStatement) (abstract.PhysicalOperator, error) {
	tableNames, err := GetAllTableNames(route)
	if err != nil {
		return nil, err
	}
	route.TableNames = tableNames

	route.SourceAST = ToSQL(ctx, route.SourceOp)
	route.SourceOp = nil
	err = stripDownQuery(stmt, route.SourceAST)
	if err != nil {
		return nil, err
	}
	sqlparser.Rewrite(route.SourceAST, func(cursor *sqlparser.Cursor) bool {
		if aliasedExpr, ok := cursor.Node().(sqlparser.SelectExpr); ok {
			removeKeyspaceFromSelectExpr(aliasedExpr)
		}
		return true
	}, nil)
	return route, nil
}

func removeKeyspaceFromSelectExpr(expr sqlparser.SelectExpr) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedExpr:
		sqlparser.RemoveKeyspaceFromColName(expr.Expr)
	case *sqlparser.StarExpr:
		expr.TableName.Qualifier = sqlparser.NewTableIdent("")
	}
}

func stripDownQuery(from, to sqlparser.SelectStatement) error {
	var err error

	switch node := from.(type) {
	case *sqlparser.Select:
		toNode, ok := to.(*sqlparser.Select)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "AST did not match")
		}
		toNode.Distinct = node.Distinct
		toNode.GroupBy = node.GroupBy
		toNode.Having = node.Having
		toNode.OrderBy = node.OrderBy
		toNode.Comments = node.Comments
		toNode.SelectExprs = node.SelectExprs
	case *sqlparser.Union:
		toNode, ok := to.(*sqlparser.Union)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "AST did not match")
		}
		err = stripDownQuery(node.Left, toNode.Left)
		if err != nil {
			return err
		}
		err = stripDownQuery(node.Right, toNode.Right)
		if err != nil {
			return err
		}
		toNode.OrderBy = node.OrderBy
	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: this should not happen - we have covered all implementations of SelectStatement %T", from)
	}
	return nil
}
