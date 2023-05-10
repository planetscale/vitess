package operators

import (
	"fmt"
	"strings"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

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
	err = bindOffsets(node, semTable)
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
	for expr, tblID := range semTable.Direct {
		colName, ok := expr.(*sqlparser.ColName)
		if !ok {
			continue
		}

		tr, found := report[tblID]
		if !found {
			infoFor, err := semTable.TableInfoFor(tblID)
			if err != nil {
				return err
			}

			vtbl := infoFor.GetVindexTable()
			if vtbl == nil {
				continue
			}

			tr = &TableReport{
				Name: sqlparser.TableName{
					Name:      sqlparser.NewIdentifierCS(vtbl.Name.String()),
					Qualifier: sqlparser.NewIdentifierCS(vtbl.Keyspace.Name),
				},
			}
			report[tblID] = tr
		}

		// only add this column if it's not already there
		if slices.ContainsFunc(tr.Columns, func(col ColumnReport) bool { return colName.Name.EqualString(col.Name) }) {
			continue
		}

		tr.Columns = append(tr.Columns, ColumnReport{
			Name: strings.ToLower(colName.Name.String()),
		})
	}

	for _, tr := range report {
		expandedCols := semTable.ExpandedColumns[tr.Name]
		for _, colExp := range expandedCols {
			for i, col := range tr.Columns {
				if colExp.Name.EqualString(col.Name) {
					tr.Columns[i].Expanded = true
				}
			}
		}
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
	return maps.Values(report)
}

func (conv *Converter) semanticAnalyze(stmt sqlparser.Statement, keyspace string, si semantics.SchemaInformation) (*semantics.SemTable, error) {
	semTable, err := semantics.Analyze(stmt, keyspace, si)
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
}

func (p *PlanContext) IncreaseNodeCount(n int) {
	p.NodeCount += n
}
