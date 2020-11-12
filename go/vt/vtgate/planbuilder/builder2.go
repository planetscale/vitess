package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	planContext struct {
		symbolTable  *symbolTable
		vschema      ContextVSchema
		bindVarNeeds *sqlparser.BindVarNeeds
	}

	symbolTable struct{}

	LogicalPlan interface {
		ToString() string
		Sources() []LogicalPlan
	}

	routePlan struct {
		// Select is the AST for the query fragment that will be
		// executed by this route.
		Select *sqlparser.Select

		// the fields are described in the RouteOpcode values comments.
		// Opcode is the execution opcode.
		Opcode engine.RouteOpcode

		// Keyspace specifies the keyspace to send the query to.
		Keyspace *vindexes.Keyspace

		// TargetDestination specifies an explicit target destination to send the query to.
		// This bypases the core of the v3 engine.
		TargetDestination key.Destination

		// TargetTabletType specifies an explicit target destination tablet type
		// this is only used in conjunction with TargetDestination
		TargetTabletType topodatapb.TabletType
	}
)

func (r *routePlan) ToString() string {
	panic("implement me")
}

func (r *routePlan) Sources() []LogicalPlan {
	return []LogicalPlan{}
}

var _ LogicalPlan = (*routePlan)(nil)

func BuildFromStmt2(query string, stmt sqlparser.Statement, vschema ContextVSchema, bindVarNeeds *sqlparser.BindVarNeeds) (*engine.Plan, error) {
	pctx := &planContext{
		symbolTable:  &symbolTable{},
		vschema:      vschema,
		bindVarNeeds: bindVarNeeds,
	}

	logical, err := buildLogicalPlanFor(pctx, stmt)
	if err != nil {
		return nil, err
	}

	instruction, err := transformToEnginePrimitive(pctx, logical)
	if err != nil {
		return nil, err
	}

	plan := &engine.Plan{
		Type:         sqlparser.ASTToStatementType(stmt),
		Original:     query,
		Instructions: instruction,
		BindVarNeeds: bindVarNeeds,
	}

	return plan, err
}

func transformToEnginePrimitive(pctx *planContext, logical LogicalPlan) (engine.Primitive, error) {
	switch plan := logical.(type) {
	case *routePlan:
		prim := &engine.Route{
			Opcode:            plan.Opcode,
			Keyspace:          plan.Keyspace,
			TargetDestination: plan.TargetDestination,
			TargetTabletType:  plan.TargetTabletType,
			FieldQuery:        "sqlparser.FormatImpossibleQuery()",
			Query:             sqlparser.String(plan.Select),
		}
		return prim, nil
	}

	_, err := fail()
	return nil, err
}

func buildLogicalPlanFor(pctx *planContext, stmt sqlparser.Statement) (LogicalPlan, error) {
	switch node := stmt.(type) {
	case *sqlparser.Select:
		fromPlan, err := processTableExpr(pctx, node.From[0])
		if err != nil {
			return nil, err
		}
		selectPlan, err := addSelectExpressions(pctx, fromPlan, node.SelectExprs)
		if err != nil {
			return nil, err
		}

		return selectPlan, nil
	}
	return fail()
}

func addSelectExpressions(pctx *planContext, in LogicalPlan, exprs sqlparser.SelectExprs) (LogicalPlan, error) {
	switch plan := in.(type) {
	case *routePlan:
		plan.Select.SelectExprs = exprs
		return plan, nil
	}

	return fail()
}

func processTableExpr(pctx *planContext, tableExpr sqlparser.TableExpr) (LogicalPlan, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return processAliasedTable(pctx, tableExpr)
	}
	return fail()
}

func fail() (LogicalPlan, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "implement me")
}

func processAliasedTable(pctx *planContext, tableExpr *sqlparser.AliasedTableExpr) (LogicalPlan, error) {
	switch expr := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		return buildTablePlan(pctx, tableExpr, expr)
	}
	return fail()
}

func buildTablePlan(pctx *planContext, tableExpr *sqlparser.AliasedTableExpr, tableName sqlparser.TableName) (LogicalPlan, error) {
	sel := &sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})}
	vschemaTable, _, _, destTabletTyp, destTarget, err := pctx.vschema.FindTableOrVindex(tableName)
	if err != nil {
		return nil, err
	}
	var keyspace *vindexes.Keyspace
	if vschemaTable != nil {
		keyspace = vschemaTable.Keyspace
	}
	plan := routePlan{
		Select:            sel,
		Opcode:            engine.SelectScatter,
		Keyspace:          keyspace,
		TargetTabletType:  destTabletTyp,
		TargetDestination: destTarget,
	}
	return &plan, nil
}
