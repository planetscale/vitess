package operators

import (
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	DDLSchema interface {
		LoadTableSpec(keyspace, table string) (*sqlparser.TableSpec, error)
	}

	TableReport struct {
		TableUsage      map[sqlparser.TableName][]string
		ExpandedColumns map[sqlparser.TableName][]*sqlparser.ColName
	}
)

func (conv *Converter) Plan(ddl DDLSchema, si semantics.SchemaInformation, stmt sqlparser.Statement, keyspace, publicID string) (
	view *Node,
	usage *TableReport,
	err error,
) {
	semTable, err := conv.semanticAnalyze(stmt, keyspace, si)
	if err != nil {
		return nil, nil, err
	}

	tableReport := computeColumnUsage(semTable)

	sel, isSel := stmt.(sqlparser.SelectStatement)
	if !isSel {
		panic("not a SelectStatement")
	}

	ctx := &PlanContext{
		SemTable: semTable,
		DDL:      ddl,
	}

	// First step is to build an operator tree from the AST
	node, err := conv.toOperator(ctx, sel, publicID)
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

	err = generateUpqueries(ctx, node)
	if err != nil {
		return nil, nil, err
	}

	return node, tableReport, nil
}

func addStringButKeepItUnique(in []string, n string) []string {
	in = append(in, n)
	slices.Sort(in)
	return slices.Compact(in)
}

func computeColumnUsage(semTable *semantics.SemTable) *TableReport {
	tblUsage := make(map[sqlparser.TableName][]string)
	for expr, set := range semTable.Direct {
		col, isCol := expr.(*sqlparser.ColName)
		if !isCol {
			continue
		}
		infoFor, err := semTable.TableInfoFor(set)
		if err != nil {
			continue
		}
		vt := infoFor.GetVindexTable()
		if vt == nil {
			continue
		}
		tableName := sqlparser.TableName{
			Name:      vt.Name,
			Qualifier: sqlparser.NewIdentifierCS(vt.Keyspace.Name),
		}
		for _, column := range vt.Columns {
			if column.Name.Equal(col.Name) {
				tblUsage[tableName] = addStringButKeepItUnique(tblUsage[tableName], column.Name.String())
				break
			}
		}
	}
	return &TableReport{
		TableUsage:      tblUsage,
		ExpandedColumns: semTable.ExpandedColumns,
	}
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
