package operators

import (
	"fmt"
	"slices"
	"strings"

	"vitess.io/vitess/go/maps2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	DDLSchema interface {
		LoadTableSpec(keyspace, table string) (string, *sqlparser.TableSpec, error)
	}

	ColumnReport struct {
		Name     string
		Expanded bool
	}

	TableReport struct {
		Node    *Node
		Name    sqlparser.TableName
		Columns []ColumnReport
	}
)

func (cr *ColumnReport) String() string {
	if cr.Expanded {
		return cr.Name + " (expanded)"
	}
	return cr.Name
}

func (tr *TableReport) String() string {
	var columns []string
	for _, col := range tr.Columns {
		columns = append(columns, col.String())
	}
	slices.Sort(columns)
	return fmt.Sprintf("%s: %s", sqlparser.CanonicalString(tr.Name), strings.Join(columns, ", "))
}

func (conv *Converter) Plan(ddl DDLSchema, si semantics.SchemaInformation, stmt sqlparser.Statement, keyspace, publicID string) (
	view *Node,
	usage []*TableReport,
	err error,
) {
	semTable, err := conv.semanticAnalyze(stmt, keyspace, si)
	if err != nil {
		return nil, nil, err
	}

	tr := make(tableUsageMap)
	if err = tr.compute(semTable); err != nil {
		return nil, nil, err
	}

	ctx := &PlanContext{
		SemTable: semTable,
		DDL:      ddl,
	}

	// First step is to build an operator tree from the AST
	// We already verified earlier that this is a select statement.
	node, err := conv.toOperator(ctx, stmt.(sqlparser.SelectStatement), publicID)
	if err != nil {
		return
	}

	// Next we to push predicates as close to the underlying tables as possible
	node, err = rewrite(node, conv.pushDownPredicate(ctx))
	if err != nil {
		return
	}

	// Now we can make sure that all the needed columns are available where they are needed
	needs, err := node.AddColumns(ctx, Columns{})
	if err != nil {
		return
	}

	err = pushColumnsToAncestors(ctx, node, needs)
	if err != nil {
		return
	}

	node, err = cleanUpTree(ctx, node, true)
	if err != nil {
		return
	}

	// Finally, we go through the operator tree and figure out the column offsets for all the column accesses
	err = conv.bindOffsets(node, semTable)
	if err != nil {
		return
	}

	reuser := NodeReuser{cache: map[Hash][]*Node{}}
	reuser.Visit(semTable, node)

	node.ConnectOutputs()

	err = node.generateUpqueries(ctx)
	if err != nil {
		return
	}

	err = node.Op.(*View).plan(ctx, node)
	if err != nil {
		return
	}

	return node, tr.resolve(node), nil
}

type tableUsageMap map[semantics.TableSet]*TableReport

func (report tableUsageMap) compute(semTable *semantics.SemTable) (err error) {
	for idx, tableInfo := range semTable.Tables {
		vtbl := tableInfo.GetVindexTable()
		if vtbl == nil {
			continue
		}

		report[semantics.SingleTableSet(idx)] = &TableReport{
			Name: sqlparser.NewTableNameWithQualifier(vtbl.Name.String(), vtbl.Keyspace.Name),
		}
	}

	// Add expanded columns first to ensure they are in the proper order.
	for _, tr := range report {
		expandedCols := semTable.ExpandedColumns[tr.Name]
		for _, col := range expandedCols {
			tr.Columns = append(tr.Columns, ColumnReport{
				Name:     col.Name.String(),
				Expanded: true,
			})
		}
	}

	for expr, tblID := range semTable.Direct {
		col, ok := expr.(*sqlparser.ColName)
		if !ok {
			continue
		}

		tr, found := report[tblID]
		if !found {
			continue
		}

		// only add this column if it's not already there
		if slices.ContainsFunc(tr.Columns, func(cr ColumnReport) bool { return col.Name.EqualString(cr.Name) }) {
			continue
		}

		tr.Columns = append(tr.Columns, ColumnReport{
			Name: col.Name.String(),
		})
	}
	return nil
}

func (report tableUsageMap) resolve(node *Node) []*TableReport {
	switch tableRef := node.Op.(type) {
	case *NodeTableRef:
		if tr, ok := report[tableRef.TableID]; ok {
			tr.Node = node
		}
	default:
		for _, n := range node.Ancestors {
			report.resolve(n)
		}
	}
	return maps2.Values(report)
}

func (conv *Converter) semanticAnalyze(stmt sqlparser.Statement, keyspace string, si semantics.SchemaInformation) (*semantics.SemTable, error) {
	semTable, err := semantics.AnalyzeStrict(stmt, keyspace, si)
	if err != nil {
		return nil, err
	}

	return semTable, nil
}

type PlanContext struct {
	SemTable  *semantics.SemTable
	Signature QuerySignature
	Query     *sqlparser.Select
	NodeCount int
	DDL       DDLSchema
	Grouping  bool
}

func (p *PlanContext) IncreaseNodeCount(n int) {
	p.NodeCount += n
}
