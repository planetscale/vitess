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

package semantics

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// UnionTable is used to represent the projected results of a UNION.
type UnionTable struct {
	columnNames []string
	cols        []sqlparser.Expr
	tables      TableSet
}

var _ TableInfo = (*UnionTable)(nil)

// Dependencies implements the TableInfo interface
func (u *UnionTable) Dependencies(colName string, org originable) (dependencies, error) {
	var deps dependencies = &nothing{}
	var err error
	for i, name := range u.columnNames {
		if name != colName {
			continue
		}
		recursiveDeps, qt := org.depsForExpr(u.cols[i])

		var directDeps TableSet
		if recursiveDeps.NumberOfTables() > 0 {
			directDeps = recursiveDeps
		}

		newDeps := createCertain(directDeps, recursiveDeps, qt)
		deps, err = deps.Merge(newDeps)
		if err != nil {
			return nil, err
		}
	}
	return deps, nil
}

// IsInfSchema implements the TableInfo interface
func (u *UnionTable) IsInfSchema() bool {
	return false
}

// IsActualTable implements the TableInfo interface
func (u *UnionTable) IsActualTable() bool {
	return false
}

func (u *UnionTable) Matches(name sqlparser.TableName) bool {
	return name.Name.String() == "" && name.Qualifier.IsEmpty()
}

func (u *UnionTable) Authoritative() bool {
	return true
}

func (u *UnionTable) Name() (sqlparser.TableName, error) {
	return sqlparser.TableName{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "oh noes")
}

func (u *UnionTable) GetExpr() *sqlparser.AliasedTableExpr {
	return nil
}

// GetVindexTable implements the TableInfo interface
func (u *UnionTable) GetVindexTable() *vindexes.Table {
	return nil
}

func (u *UnionTable) GetColumns() []ColumnInfo {
	cols := make([]ColumnInfo, 0, len(u.columnNames))
	for _, col := range u.columnNames {
		cols = append(cols, ColumnInfo{
			Name: col,
		})
	}
	return cols
}

func (u *UnionTable) hasStar() bool {
	return u.tables > 0
}

// GetTables implements the TableInfo interface
func (u *UnionTable) GetTables(_ originable) TableSet {
	return u.tables
}

// GetExprFor implements the TableInfo interface
func (u *UnionTable) GetExprFor(s string) (sqlparser.Expr, error) {
	for i, colName := range u.columnNames {
		if colName == s {
			return u.cols[i], nil
		}
	}
	return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadFieldError, "Unknown column '%s' in 'field list'", s)
}

func createUnionTableForExpressions(expressions sqlparser.SelectExprs, tables []TableInfo, org originable) *UnionTable {
	cols, colNames, ts := selectExprsToInfo(expressions, tables, org)
	return &UnionTable{
		columnNames: colNames,
		cols:        cols,
		tables:      ts,
	}
}
