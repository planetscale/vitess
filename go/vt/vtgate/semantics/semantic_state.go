/*
Copyright 2020 The Vitess Authors.

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

package semantics

import (
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	ITableInfo interface {
		HasAsExpr() bool
		AsExprString() string
		KeyspaceName() string
		Name() string
		IsAuthoritative() bool
		InVSchema() bool
		GetColumns() (sqlparser.SelectExprs, error)
		GetTableColumns() []vindexes.Column
		GetExpr() *sqlparser.AliasedTableExpr
		TableName() string
		DBName() string
	}

	// TableInfo contains the alias table expr and vindex table
	TableInfo struct {
		dbName, tableName string
		ASTNode           *sqlparser.AliasedTableExpr
		Table             *vindexes.Table
	}

	vTableInfo struct {
		cols []*sqlparser.AliasedExpr
	}

	// TableSet is how a set of tables is expressed.
	// Tables get unique bits assigned in the order that they are encountered during semantic analysis
	TableSet uint64 // we can only join 64 tables with this underlying data type
	// TODO : change uint64 to struct to support arbitrary number of tables.

	// SemTable contains semantic analysis information about the query.
	SemTable struct {
		Tables           []ITableInfo
		exprDependencies map[sqlparser.Expr]TableSet
		selectScope      map[*sqlparser.Select]*scope
	}

	scope struct {
		parent      *scope
		selectExprs sqlparser.SelectExprs
		tables      []ITableInfo
		vtables     []*vTableInfo
	}

	// SchemaInformation is used tp provide table information from Vschema.
	SchemaInformation interface {
		FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error)
	}
)

func (v vTableInfo) HasAsExpr() bool {
	panic("implement me")
}

func (v vTableInfo) AsExprString() string {
	panic("implement me")
}

func (v vTableInfo) KeyspaceName() string {
	panic("implement me")
}

func (v vTableInfo) Name() string {
	panic("implement me")
}

func (v vTableInfo) IsAuthoritative() bool {
	panic("implement me")
}

func (v vTableInfo) InVSchema() bool {
	panic("implement me")
}

func (v vTableInfo) GetColumns() (sqlparser.SelectExprs, error) {
	panic("implement me")
}

func (v vTableInfo) GetTableColumns() []vindexes.Column {
	panic("implement me")
}

func (v vTableInfo) GetExpr() *sqlparser.AliasedTableExpr {
	panic("implement me")
}

func (v vTableInfo) TableName() string {
	panic("implement me")
}

func (v vTableInfo) DBName() string {
	panic("implement me")
}

// GetTableColumns returns all the tables in the VSchema
func (t *TableInfo) GetTableColumns() []vindexes.Column {
	if !t.InVSchema() {
		return []vindexes.Column{}
	}
	return t.Table.Columns
}

// TableName returns the name of the table specified in the query
func (t *TableInfo) TableName() string {
	return t.tableName
}

// DBName returns the name of the database specified in the query
func (t *TableInfo) DBName() string {
	return t.dbName
}

// GetExpr returns the AST node
func (t *TableInfo) GetExpr() *sqlparser.AliasedTableExpr {
	return t.ASTNode
}

// GetColumns returns the column found in the AST node
func (t *TableInfo) GetColumns() (sqlparser.SelectExprs, error) {
	tblName, err := t.ASTNode.TableName()
	if err != nil {
		return nil, err
	}
	var colNames sqlparser.SelectExprs
	for _, col := range t.Table.Columns {
		colNames = append(colNames, &sqlparser.AliasedExpr{
			Expr: sqlparser.NewColNameWithQualifier(col.Name.String(), tblName),
			As:   sqlparser.NewColIdent(col.Name.String()),
		})
	}
	return colNames, nil
}

// HasAsExpr returns true if the AS expression is in the query
func (t *TableInfo) HasAsExpr() bool {
	return t.ASTNode.As.IsEmpty()
}

// AsExprString return the AS expression as a string
func (t *TableInfo) AsExprString() string {
	return t.ASTNode.As.String()
}

// KeyspaceName returns the name of the keyspace from the VSchema
func (t *TableInfo) KeyspaceName() string {
	if !t.InVSchema() {
		return ""
	}
	return t.Table.Keyspace.Name
}

// Name returns the name of the table
func (t *TableInfo) Name() string {
	expr, ok := t.ASTNode.Expr.(sqlparser.TableName)
	if !ok {
		return ""
	}
	return expr.Name.String()
}

// IsAuthoritative returns true if the table is marked as authoritative in the vschema
func (t *TableInfo) IsAuthoritative() bool {
	return t.InVSchema() && t.Table.ColumnListAuthoritative
}

// InVSchema returns true if the table is in the vschema
func (t *TableInfo) InVSchema() bool {
	return t.Table != nil
}

// NewSemTable creates a new empty SemTable
func NewSemTable() *SemTable {
	return &SemTable{exprDependencies: map[sqlparser.Expr]TableSet{}}
}

// TableSetFor returns the bitmask for this particular tableshoe
func (st *SemTable) TableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	for idx, t2 := range st.Tables {
		if t == t2.GetExpr() {
			return 1 << idx
		}
	}
	return 0
}

// TableInfoFor returns the table info for the table set. It should contains only single table.
func (st *SemTable) TableInfoFor(id TableSet) (ITableInfo, error) {
	if id.NumberOfTables() > 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should only be used for single tables")
	}
	return st.Tables[id.TableOffset()], nil
}

// Dependencies return the table dependencies of the expression.
func (st *SemTable) Dependencies(expr sqlparser.Expr) TableSet {
	deps, found := st.exprDependencies[expr]
	if found {
		return deps
	}

	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		colName, ok := node.(*sqlparser.ColName)
		if ok {
			set := st.exprDependencies[colName]
			deps |= set
		}
		return true, nil
	}, expr)

	st.exprDependencies[expr] = deps

	return deps
}

// GetSelectTables returns the table in the select.
func (st *SemTable) GetSelectTables(node *sqlparser.Select) []ITableInfo {
	scope := st.selectScope[node]
	return scope.tables
}

// AddExprs adds new select exprs to the SemTable.
func (st *SemTable) AddExprs(tbl *sqlparser.AliasedTableExpr, cols sqlparser.SelectExprs) {
	tableSet := st.TableSetFor(tbl)
	for _, col := range cols {
		st.exprDependencies[col.(*sqlparser.AliasedExpr).Expr] = tableSet
	}
}

func newScope(parent *scope) *scope {
	return &scope{parent: parent}
}

func (s *scope) addTable(table ITableInfo) error {
	for _, scopeTable := range s.tables {
		b := scopeTable.TableName() == table.TableName()
		b2 := scopeTable.DBName() == table.DBName()
		if b && b2 {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUniqTable, "Not unique table/alias: '%s'", table.TableName())
		}
	}

	s.tables = append(s.tables, table)
	return nil
}

// IsOverlapping returns true if at least one table exists in both sets
func (ts TableSet) IsOverlapping(b TableSet) bool { return ts&b != 0 }

// IsSolvedBy returns true if all of `ts` is contained in `b`
func (ts TableSet) IsSolvedBy(b TableSet) bool { return ts&b == ts }

// NumberOfTables returns the number of bits set
func (ts TableSet) NumberOfTables() int {
	// Brian Kernighan’s Algorithm
	count := 0
	for ts > 0 {
		ts &= ts - 1
		count++
	}
	return count
}

// TableOffset returns the offset in the Tables array from TableSet
func (ts TableSet) TableOffset() int {
	offset := 0
	for ts > 1 {
		ts = ts >> 1
		offset++
	}
	return offset
}

// Constituents returns an slice with all the
// individual tables in their own TableSet identifier
func (ts TableSet) Constituents() (result []TableSet) {
	mask := ts

	for mask > 0 {
		maskLeft := mask & (mask - 1)
		constituent := mask ^ maskLeft
		mask = maskLeft
		result = append(result, constituent)
	}
	return
}

// Merge creates a TableSet that contains both inputs
func (ts TableSet) Merge(other TableSet) TableSet {
	return ts | other
}
