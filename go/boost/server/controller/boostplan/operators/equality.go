package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func (p *Project) Equals(st *semantics.SemTable, op Operator) bool {
	other, ok := op.(*Project)
	if !ok {
		return false
	}
	return p.Columns.Equals(st, other.Columns, false)
}

func (g *GroupBy) Equals(st *semantics.SemTable, op Operator) bool {
	other, ok := op.(*GroupBy)
	if !ok {
		return false
	}
	if !g.Grouping.Equals(st, other.Grouping, false) {
		return false
	}

	if len(g.Aggregations) != len(other.Aggregations) {
		return false
	}

	for i, aggregation := range g.Aggregations {
		o := other.Aggregations[i]
		if !sqlparser.EqualsAggrFunc(aggregation, o) {
			return false
		}
	}

	return true
}

func (v *View) Equals(st *semantics.SemTable, op Operator) bool {
	return false // we don't want to share views
}

func (f *Filter) Equals(st *semantics.SemTable, op Operator) bool {
	other, ok := op.(*Filter)
	if !ok {
		return false
	}

	// TODO: we could split the predicates into ANDed atoms and compare these. The order doesn't really matter
	return sqlparser.EqualsExpr(f.Predicates, other.Predicates)
}

func (n *NullFilter) Equals(st *semantics.SemTable, op Operator) bool {
	other, ok := op.(*NullFilter)
	if !ok {
		return false
	}

	return sqlparser.EqualsExpr(n.Predicates, other.Predicates)
}

func (j *Join) Equals(st *semantics.SemTable, op Operator) bool {
	other, ok := op.(*Join)
	if !ok {
		return false
	}
	if j.Inner != other.Inner {
		return false
	}
	if !sqlparser.EqualsExpr(j.Predicates, other.Predicates) {
		return false
	}
	return true
}

func (t *Table) Equals(st *semantics.SemTable, op Operator) bool {
	other, ok := op.(*Table)
	if !ok {
		return false
	}
	return t.Keyspace == other.Keyspace && t.TableName == other.TableName
}

func (n *NodeTableRef) Equals(st *semantics.SemTable, op Operator) bool {
	other, ok := op.(*NodeTableRef)
	if !ok {
		return false
	}

	return n.Node.Op.Equals(st, other.Node.Op)
}

func (u *Union) Equals(st *semantics.SemTable, op Operator) bool {
	other, ok := op.(*Union)
	if !ok {
		return false
	}

	return u.Columns.Equals(st, other.Columns, false) &&
		u.InputColumns[0].Equals(st, other.InputColumns[0], false) &&
		u.InputColumns[1].Equals(st, other.InputColumns[1], false)
}

func (t *TopK) Equals(st *semantics.SemTable, op Operator) bool {
	other, ok := op.(*TopK)
	if !ok {
		return false
	}

	return t.K == other.K && sqlparser.EqualsOrderBy(t.Order, other.Order)
}

func (d *Distinct) Equals(st *semantics.SemTable, op Operator) bool {
	_, ok := op.(*Distinct)
	return ok
}
