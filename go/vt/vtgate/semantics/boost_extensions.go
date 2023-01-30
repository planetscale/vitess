package semantics

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type noTable struct {
	id TableSet
}

func (n *noTable) Name() (sqlparser.TableName, error) {
	return sqlparser.TableName{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "should not be called")
}

func (n *noTable) GetVindexTable() *vindexes.Table {
	return nil
}

func (n *noTable) getExprFor(s string) (sqlparser.Expr, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "should not be called")
}

func (n *noTable) IsInfSchema() bool {
	return false
}

func (n *noTable) matches(name sqlparser.TableName) bool {
	return false
}

func (n *noTable) authoritative() bool {
	return false
}

func (n *noTable) getExpr() *sqlparser.AliasedTableExpr {
	return nil
}

func (n *noTable) getColumns() []ColumnInfo {
	return nil
}

func (n *noTable) dependencies(colName string, org originable) (dependencies, error) {
	return &nothing{}, nil
}

func (n *noTable) getTableSet(org originable) TableSet {
	return n.id
}

var _ TableInfo = (*noTable)(nil)

// GetNextTableSet reserves a place in the table list of the semantic state
func (st *SemTable) GetNextTableSet() TableSet {
	marker := &noTable{id: SingleTableSet(len(st.Tables))}
	st.Tables = append(st.Tables, marker)
	return marker.id
}
