package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	staticRouting struct {
		opCode   engine.Opcode
		selected *VindexOption
	}
	nextRouting      struct{}
	referenceRouting struct{}
)

func (n *nextRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error {
	return vterrors.VT13001("did not expect any predicates on NEXT query")
}

func (n *nextRouting) OpCode() engine.Opcode {
	return engine.Next
}

func (n *nextRouting) Clone() routing {
	return &nextRouting{}
}

func (n *nextRouting) AddQueryTablePredicates(ctx *plancontext.PlanningContext, qt *QueryTable) error {
	return n.UpdateRoutingLogic(ctx, nil) // doing this so we always return the same error for the two methods
}

var _ routing = (*staticRouting)(nil)
var _ routing = (*nextRouting)(nil)

func (s *staticRouting) UpdateRoutingLogic(*plancontext.PlanningContext, sqlparser.Expr) error {
	return nil
}

func (s *staticRouting) OpCode() engine.Opcode {
	return s.opCode
}

func (s *staticRouting) Clone() routing {
	return &staticRouting{opCode: s.opCode}
}

func (s *staticRouting) AddQueryTablePredicates(*plancontext.PlanningContext, *QueryTable) error {
	return nil
}

func (rr *referenceRouting) UpdateRoutingLogic(*plancontext.PlanningContext, sqlparser.Expr) error {
	return nil
}

func (rr *referenceRouting) OpCode() engine.Opcode {
	return engine.Reference
}

func (rr *referenceRouting) Clone() routing {
	return &referenceRouting{}
}

func (rr *referenceRouting) AddQueryTablePredicates(*plancontext.PlanningContext, *QueryTable) error {
	return nil
}
