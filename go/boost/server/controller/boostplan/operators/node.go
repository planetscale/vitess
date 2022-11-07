package operators

import (
	"fmt"

	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	Node struct {
		Name    string
		Version int64

		// These are the columns being exposed to the outside
		// select id, name from users where apa = 23
		Columns Columns
		Op      Operator

		// Input is from the perspective of someone querying the graph, and getting the output from the view node.
		// The base tables don't have inputs from other operators, only from transactions on the underlying table
		Ancestors []*Node `json:",omitempty"` // this is the old Ancestors
		Children  []*Node `json:",omitempty"` // this is the old Children

		Flow FlowNode
	}

	Column struct {
		// The original AST node, if available
		// For join columns, two AST expressions can point to the same column
		AST []sqlparser.Expr

		// Name is the column name
		Name string
	}

	Columns []*Column
)

func (node *Node) Covers() semantics.TableSet {
	ts := semantics.EmptyTableSet()

	if tbl := node.Op.IntroducesTableID(); tbl != nil {
		ts.MergeInPlace(*tbl)
	}

	for _, input := range node.Ancestors {
		ts.MergeInPlace(input.Covers())
	}
	return ts
}

func (node *Node) AddColumns(ctx *PlanContext, col Columns) (Columns, error) {
	needs, err := node.Op.AddColumns(ctx, col)
	if err != nil {
		return Columns{}, err
	}

	ch, ok := node.Op.(ColumnHolder)
	if ok {
		node.Columns = ch.GetColumns()
		return needs, nil
	}

outer:
	for _, newCol := range col {
		for _, oldCol := range node.Columns {
			if newCol.Equals(ctx.SemTable, oldCol, true) {
				continue outer
			}
		}
		node.Columns = node.Columns.Add(ctx, newCol)
	}

	return needs, nil
}

func (node *Node) ExprLookup(st *semantics.SemTable, expr sqlparser.Expr) (int, error) {
	for i, column := range node.Columns {
		if column.EqualsAST(st, expr, true) {
			return i, nil
		}
	}

	tableInfo, err := st.TableInfoForExpr(expr)
	if err == nil {
		if _, isDerived := tableInfo.(*semantics.DerivedTable); isDerived {
			expr, err = semantics.RewriteDerivedExpression(expr, tableInfo)
			if err != nil {
				return 0, err
			}
		}
	}

	for i, column := range node.Columns {
		if column.EqualsAST(st, expr, true) {
			return i, nil
		}
	}

	return -1, NewBug(fmt.Sprintf("column not found: %s", sqlparser.String(expr)))
}

func (node *Node) Equals(st *semantics.SemTable, other *Node) bool {
	if !node.Op.Equals(st, other.Op) {
		return false
	}
	if len(node.Ancestors) != len(other.Ancestors) {
		return false
	}
	for i, ancestor := range node.Ancestors {
		o := other.Ancestors[i]
		if !ancestor.Equals(st, o) {
			return false
		}
	}
	return true
}

func (columns Columns) Add(ctx *PlanContext, newColumns ...*Column) Columns {
outer:
	for _, newCol := range newColumns {
		for _, oldCol := range columns {
			if newCol.Equals(ctx.SemTable, oldCol, true) {
				// if these are considered the same, we should still go over the AST collection and
				// collect any AST expressions that are _not_ the same as an already existing column.
				for _, expr := range newCol.AST {
					if !oldCol.EqualsAST(ctx.SemTable, expr, true) {
						oldCol.AST = append(oldCol.AST, expr)
					}
				}
				continue outer
			}
		}
		columns = append(columns, newCol)
	}
	return columns
}

func (col *Column) Equals(st *semantics.SemTable, other *Column, semanticEquality bool) bool {
	for _, rgt := range other.AST {
		if col.EqualsAST(st, rgt, semanticEquality) {
			return true
		}
	}

	return false
}

func (columns Columns) Equals(st *semantics.SemTable, other Columns, semanticEquality bool) bool {
	if len(columns) != len(other) {
		return false
	}
	for i, column := range columns {
		if !column.Equals(st, other[i], semanticEquality) {
			return false
		}
	}
	return true
}

func (col *Column) EqualsAST(semTable *semantics.SemTable, other sqlparser.Expr, semanticEquality bool) bool {
	switch other := other.(type) {
	case *sqlparser.ColName:
		otherID := semTable.DirectDeps(other)
		for _, thisExpr := range col.AST {
			if thisCol, isCol := thisExpr.(*sqlparser.ColName); isCol {
				if !thisCol.Name.Equal(other.Name) {
					continue
				}

				thisID := semTable.DirectDeps(thisCol)
				if thisID.Equals(otherID) {
					return true
				}

				if !semanticEquality && equalAccordingToSchemaTable(semTable, other, thisCol) {
					return true
				}
			}
		}
	default:
		for _, thisExpr := range col.AST {
			if sqlparser.EqualsExpr(thisExpr, other) {
				return true
			}
		}
	}

	return false
}

// equalAccordingToSchemaTable compares two ColName expressions based on which table they are fetching data from
// this is different than the semantic equality which is comparing the table in the query and not the real underlying table
// Example: SELECT * FROM tbl as t1, tbl as t2
// According to the semantic comparison, t1 and t2 are different tables here,
// but when comparing with equalAccordingToSchemaTable they are considered the same table
func equalAccordingToSchemaTable(semTable *semantics.SemTable, a, b *sqlparser.ColName) bool {
	tableInfo1, err := semTable.TableInfoForExpr(a)
	if err != nil {
		return false
	}
	tableInfo2, err := semTable.TableInfoForExpr(b)
	if err != nil {
		return false
	}
	switch tableInfo1.(type) {
	case *semantics.DerivedTable:
		_, ok := tableInfo2.(*semantics.DerivedTable)
		if !ok {
			return false
		}
		name1, err := tableInfo1.Name()
		if err != nil {
			return false
		}
		name2, err := tableInfo2.Name()
		if err != nil {
			return false
		}
		if sqlparser.EqualsTableName(name1, name2) {
			return true
		}
	case *semantics.RealTable:
		_, ok := tableInfo2.(*semantics.RealTable)
		if !ok {
			return false
		}
		vtable1 := tableInfo1.GetVindexTable()
		vtable2 := tableInfo2.GetVindexTable()
		if vtable1 == vtable2 {
			return true
		}
	default:
		return false
	}
	return false
}

func (col *Column) SingleAST() (sqlparser.Expr, error) {
	if len(col.AST) != 1 {
		return nil, NewBug("assumed we would get a single AST here")
	}
	return col.AST[0], nil
}
func (col *Column) Explain() (string, string) {
	details := ""
	if len(col.AST) > 0 {
		details += sqlparser.String(sqlparser.Exprs(col.AST))
	}
	return col.Name, details
}

func (col *Column) IsLiteral() bool {
	if len(col.AST) == 0 {
		return false
	}
	_, isLit := col.AST[0].(*sqlparser.Literal)
	return isLit
}

func (col *Column) IsAggregation() bool {
	if len(col.AST) == 0 {
		return false
	}
	_, isAggr := col.AST[0].(sqlparser.AggrFunc)
	return isAggr
}

func (node *Node) Signature() QuerySignature {
	var sig QuerySignature
	for _, input := range node.Ancestors {
		sig = sig.Merge(input.Signature())
	}
	return sig.Merge(node.Op.Signature())
}

func (node *Node) VersionedName() string {
	return fmt.Sprintf("%s_v%d", node.Name, node.Version)
}

func (node *Node) Roots() (roots []*Node) {
	if len(node.Ancestors) == 0 {
		return []*Node{node}
	}

	for _, input := range node.Ancestors {
		roots = append(roots, input.Roots()...)
	}

	return
}

func (node *Node) ConnectOutputs() {
	for _, in := range node.Ancestors {
		in.Children = append(in.Children, node)
		in.ConnectOutputs()
	}
}

func (node *Node) FlowNodeAddr() (graph.NodeIdx, error) {
	if !node.Flow.Valid() {
		return graph.NodeIdx(0), NewBug("MIR node does not have an associated FlowNode")
	}
	return node.Flow.Address, nil
}
