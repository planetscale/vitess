package operators

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/boost/common/dbg"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func pushColumnsToAncestors(ctx *PlanContext, this *Node, needsColumns Columns) error {
	switch len(this.Ancestors) {
	case 0:
		// nothing to do
		return nil
	case 1:
		return pushToSingleAncestor(ctx, this, needsColumns)
	default:
		switch op := this.Op.(type) {
		case *Union:
			return pushToUnion(ctx, this, op)
		case *Join:
			return pushToJoin(ctx, this, needsColumns)
		}
	}

	panic(fmt.Sprintf("should not happen: %T", this.Op))
}

func pushToUnion(ctx *PlanContext, this *Node, op *Union) error {
	toAncestorsLeft, err := this.Ancestors[0].AddColumns(ctx, op.InputColumns[0])
	if err != nil {
		return err
	}
	err = pushColumnsToAncestors(ctx, this.Ancestors[0], toAncestorsLeft)
	if err != nil {
		return err
	}

	toAncestorsRight, err := this.Ancestors[1].AddColumns(ctx, op.InputColumns[1])
	if err != nil {
		return err
	}
	return pushColumnsToAncestors(ctx, this.Ancestors[1], toAncestorsRight)
}

func pushToJoin(ctx *PlanContext, this *Node, needsColumns Columns) error {
	count := 0
	for _, ancestor := range this.Ancestors {
		colsForThisAncestor := Columns{}
		for _, column := range needsColumns {
			expr, err := column.SingleAST()
			if err != nil {
				return err
			}

			deps := ctx.SemTable.DirectDeps(expr)
			if deps.IsSolvedBy(ancestor.Covers()) {
				colsForThisAncestor = colsForThisAncestor.Add(ctx, column)
				count++
			}
		}
		toAncestors, err := ancestor.AddColumns(ctx, colsForThisAncestor)
		if err != nil {
			return err
		}
		err = pushColumnsToAncestors(ctx, ancestor, toAncestors)
		if err != nil {
			return err
		}
	}
	dbg.Assert(count == len(needsColumns), "incorrect number of pushed columns: got [%d], want [%d]", count, len(needsColumns))
	return nil
}

func pushToSingleAncestor(ctx *PlanContext, this *Node, needsColumns Columns) error {
	// If we have a single ancestor, we can push everything to it
	ancestor := this.Ancestors[0]
	toAncestors, err := ancestor.AddColumns(ctx, needsColumns)
	if err != nil {
		return err
	}
	if err = pushColumnsToAncestors(ctx, ancestor, toAncestors); err != nil {
		return err
	}
	if this.Op.KeepAncestorColumns() {
		this.Columns = ancestor.Columns
	}
	return nil
}

func (p *Project) AddColumns(ctx *PlanContext, col Columns) (needs Columns, err error) {

	// First we add the columns that the planner assigned to this operator
	for _, col := range p.Columns {
		switch {
		case col.IsAggregation():
			needs = needs.Add(ctx, col)
		case col.IsLiteral():
		// do nothing
		default:
			ast, err := col.SingleAST()
			if err != nil {
				return nil, err
			}
			needs = needs.Add(ctx, getColNamesInTree(ctx, ast)...)
		}
	}

	// Next we go over the needs of the operators above this one (closer to the view)
	// They will have pushed down their needs
	for _, newCol := range col {
		if p.isDerivedTable() {
			expr := newCol.AST[0]
			deps := ctx.SemTable.DirectDeps(expr)

			// sometimes we need to push in expressions from outside the derived table,
			// thanks to parameters inside the derived table.
			// here we check if the expression comes projection of the derived table and should get rewritten,
			// or if we have an expression that really lives on the inside of the derived table and can be used as is
			if deps == (*p.TableID) {
				newCol, err = p.rewriteDerivedColumn(ctx.SemTable, newCol)
				if err != nil {
					return Columns{}, err
				}
			}
		}
		p.Columns = p.Columns.Add(ctx, newCol)

		if newCol.IsAggregation() || !newCol.ShouldEvaluate() {
			needs = needs.Add(ctx, newCol)
			continue
		}

		ast, err := newCol.SingleAST()
		if err != nil {
			return nil, err
		}

		switch ast.(type) {
		case *sqlparser.Offset:
			needs = needs.Add(ctx, newCol)
		default:
			needs = needs.Add(ctx, getColNamesInTree(ctx, ast)...)
		}
	}
	return
}

func (p *Project) rewriteDerivedColumn(semTable *semantics.SemTable, in *Column) (*Column, error) {
	expr := in.AST[0]
	infoFor, err := semTable.TableInfoFor(*p.TableID)
	if err != nil {
		dbg.Bug("got an unknown table info")
	}
	dt, ok := infoFor.(*semantics.DerivedTable)
	if !ok {
		dbg.Bug("this should be a derived table")
	}

	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		dbg.Bug("this should be a sqlparser.ColName")
	}
	for i, colName := range dt.ColumnNames {
		if col.Name.EqualString(colName) {
			return p.DerivedColumns[i], nil
		}
	}

	s1, s2 := in.Explain()
	dbg.Bug("did not find the derived column: %s - %s", s1, s2)
	return nil, nil
}

func (p *Project) rewriteDerivedExpr(semTable *semantics.SemTable, expr sqlparser.Expr) (sqlparser.Expr, error) {
	tblID := semTable.DirectDeps(expr)
	if tblID.IsEmpty() {
		return expr, nil
	}
	infoFor, err := semTable.TableInfoFor(tblID)
	if err != nil {
		return nil, err
	}

	dt, ok := infoFor.(*semantics.DerivedTable)
	if !ok {
		dbg.Bug("this should be a derived table")
	}

	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		dbg.Bug("this should be a sqlparser.ColName")
	}
	for i, colName := range dt.ColumnNames {
		if col.Name.EqualString(colName) {
			expr, err := p.DerivedColumns[i].SingleAST()
			if err != nil {
				return nil, err
			}
			return expr, nil
		}
	}

	dbg.Bug("did not find the derived column")
	return nil, nil
}

func (p *Project) isDerivedTable() bool {
	return p.TableID != nil
}

func (g *GroupBy) AddColumns(ctx *PlanContext, columns Columns) (Columns, error) {
outer:
	for _, col := range columns {
		for _, aggrFunc := range g.Aggregations {
			if aggrFunc.Equals(ctx.SemTable, col, true) {
				continue outer
			}
		}

		for _, g := range g.Grouping {
			if g.Equals(ctx.SemTable, col, true) {
				continue outer
			}
		}

		// If we get here, we are seeing a new expression being requested here.
		// This is usually a predicate with a parameter,
		// and these are handled by adding the expression as a grouping expression.
		//
		// We set DoNotEvaluate as false to let the ancestor-projection (or other operators)
		// the GroupBy that they need to project this column
		newCol := *col
		newCol.AST = sqlparser.CloneExprs(col.AST)
		newCol.DoNotEvaluate = false
		g.Grouping = g.Grouping.Add(ctx, &newCol)
		g.ImplicitGrouping = g.ImplicitGrouping.Add(ctx, &newCol)
	}

	needs := Columns{}.Add(ctx, g.Grouping...)
	for _, aggrFunc := range g.Aggregations {
		ast, err := aggrFunc.SingleAST()
		if err != nil {
			return nil, err
		}
		switch ast := ast.(type) {
		case *sqlparser.CountStar:
			// empty by design
		case sqlparser.AggrFunc:
			needs = needs.Add(ctx, ColumnFromAST(ast.GetArg()))
		case sqlparser.Expr:
			needs = needs.Add(ctx, ColumnFromAST(ast))
		}
	}

	if len(g.Grouping) == 0 {
		grpCol := ColumnFromAST(sqlparser.NewIntLiteral("0"))
		grpCol.Name = "bogo_group"
		needs = needs.Add(ctx, grpCol)
		g.Grouping = g.Grouping.Add(ctx, grpCol)
		g.ImplicitGrouping = g.ImplicitGrouping.Add(ctx, grpCol)
	}

	return needs, nil
}

func (v *View) AddColumns(ctx *PlanContext, col Columns) (Columns, error) {
	col = col.Add(ctx, v.Columns...)

	for _, parameter := range v.Dependencies {
		// if this is a grouping query, we need to handle parameters a bit differently and not
		// evaluate expressions in the Projection which is the ancestor of the View,
		// and instead evaluate these under the GroupBy operator
		parameter.Column.DoNotEvaluate = ctx.Grouping
		col = col.Add(ctx, parameter.Column)
	}
	for _, pf := range v.PostFilter {
		col = col.Add(ctx, pf)
	}
	return col, nil
}

func (f *Filter) AddColumns(ctx *PlanContext, col Columns) (Columns, error) {
	col = col.Add(ctx, getColumnsAndAggregationsInTree(ctx, f.Predicates)...)
	return col, nil
}

func (n *NullFilter) AddColumns(ctx *PlanContext, col Columns) (Columns, error) {
	var needs Columns
	needs.Add(ctx)
	needs = needs.Add(ctx, getColumnsAndAggregationsInTree(ctx, n.Predicates)...)
	for _, col := range col {
		ast, err := col.SingleAST()
		if err != nil {
			return nil, err
		}
		needs = needs.Add(ctx, getColumnsAndAggregationsInTree(ctx, ast)...)
	}
	return needs, nil
}

func (j *Join) AddColumns(ctx *PlanContext, col Columns) (Columns, error) {
	comparisons, err := getComparisons(j.Predicates)
	if err != nil {
		return Columns{}, err
	}

	for _, cmp := range comparisons {
		lftCol, lftOK := cmp.Left.(*sqlparser.ColName)
		rgtCol, rgtOK := cmp.Right.(*sqlparser.ColName)
		if !lftOK || !rgtOK {
			return Columns{}, &UnsupportedError{
				AST:  cmp,
				Type: JoinPredicates,
			}
		}

		var add []*Column
		if j.Inner {
			add = append(add, &Column{
				AST:  []sqlparser.Expr{lftCol, rgtCol},
				Name: fmt.Sprintf("%s/%s", sqlparser.String(lftCol), sqlparser.String(rgtCol)),
			})
		} else {
			add = append(add, ColumnFromAST(lftCol), ColumnFromAST(rgtCol))
		}

		j.EmitColumns = j.EmitColumns.Add(ctx, add...)
		j.JoinColumns = append(j.JoinColumns, add...)
	}
	j.EmitColumns = j.EmitColumns.Add(ctx, col...)
	col = col.Add(ctx, getColNamesInTree(ctx, j.Predicates)...)
	return col, nil
}

func (t *Table) AddColumns(_ *PlanContext, col Columns) (Columns, error) {
	var missing []string
	for _, newCol := range col {
		var hasCol bool
		for _, column := range t.ColumnSpecs {
			if column.Column.Name.EqualString(newCol.Name) {
				hasCol = true
				break
			}
		}
		if !hasCol {
			missing = append(missing, newCol.Name)
		}
	}
	if len(missing) > 0 {
		return Columns{}, &UnknownColumnsError{
			Keyspace: t.Keyspace,
			Table:    t.TableName,
			Columns:  missing,
		}
	}
	return Columns{}, nil
}

func (n *NodeTableRef) AddColumns(ctx *PlanContext, col Columns) (Columns, error) {
	// HACK - change this when we want to share more than underlying tables
	tbl := n.Node.Op.(*Table)

	var missing []string
outer:
	for _, column := range col {
		expr, err := column.SingleAST()
		if err != nil {
			return Columns{}, err
		}
		colName, ok := expr.(*sqlparser.ColName)
		dbg.Assert(ok, "can't push non-columns to a table %s %T", sqlparser.String(expr), expr)

		tblID := ctx.SemTable.DirectDeps(colName)
		if tblID == n.TableID {
			for _, colSpec := range tbl.ColumnSpecs {
				if colSpec.Column.Name.Equal(colName.Name) {
					continue outer
				}
			}
		}

		missing = append(missing, sqlparser.String(expr))
	}

	if len(missing) > 0 {
		return Columns{}, &UnknownColumnsError{
			Keyspace: tbl.Keyspace,
			Table:    tbl.TableName,
			Columns:  missing,
		}
	}
	return Columns{}, nil
}

func (u *Union) AddColumns(ctx *PlanContext, col Columns) (Columns, error) {
	for _, c1 := range col {
		ast, err := c1.SingleAST()
		if err != nil {
			return nil, err
		}

		if col, ok := ast.(*sqlparser.ColName); ok {
			if !strings.HasPrefix(col.Name.String(), "union") {
				dbg.Bug("got columns we did not expect for UNION `%s`", col.Name.String())
			}
			continue
		}

		dbg.Bug("got columns we did not expect for UNION [%s]", sqlparser.String(ast))
	}
	return col, nil
}

func (t *TopK) AddColumns(ctx *PlanContext, col Columns) (Columns, error) {
	for _, order := range t.Order {
		col = col.Add(ctx, ColumnFromAST(order.Expr))
	}
	for _, parameter := range t.Parameters {
		col = col.Add(ctx, parameter)
	}
	return col, nil
}

func (d *Distinct) AddColumns(_ *PlanContext, col Columns) (Columns, error) {
	return col, nil
}

func getColNamesInTree(ctx *PlanContext, expr ...sqlparser.Expr) (col Columns) {
	var nodes []sqlparser.SQLNode
	for _, s := range expr {
		nodes = append(nodes, s)
	}
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if c, isCol := node.(*sqlparser.ColName); isCol {
			newCol := ColumnFromAST(c)
			col = col.Add(ctx, newCol)
		}
		return true, nil
	}, nodes...)
	if err != nil {
		return Columns{}
	}
	return
}

func getColumnsAndAggregationsInTree(ctx *PlanContext, expr ...sqlparser.Expr) (col Columns) {
	var nodes []sqlparser.SQLNode
	for _, s := range expr {
		nodes = append(nodes, s)
	}
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			newCol := ColumnFromAST(node)
			col = col.Add(ctx, newCol)
		case sqlparser.AggrFunc:
			newCol := ColumnFromAST(node)
			col = col.Add(ctx, newCol)
			return false, nil
		}
		return true, nil
	}, nodes...)
	if err != nil {
		return Columns{}
	}
	return
}

var _ ColumnHolder = (*NodeTableRef)(nil)
var _ ColumnHolder = (*Table)(nil)
var _ ColumnHolder = (*View)(nil)
var _ ColumnHolder = (*GroupBy)(nil)
var _ ColumnHolder = (*Join)(nil)
var _ ColumnHolder = (*Union)(nil)

func (n *NodeTableRef) GetColumns() Columns {
	return n.Columns
}

func (t *Table) GetColumns() (result Columns) {
	for _, columnSpec := range t.ColumnSpecs {
		name := columnSpec.Column.Name.String()
		result = append(result, ColumnFromAST(sqlparser.NewColName(name)))
	}
	return
}

func (p *Project) GetColumns() Columns {
	return p.Columns
}

func (v *View) GetColumns() Columns {
	return v.Columns
}

func (g *GroupBy) GetColumns() Columns {
	var cols Columns
	cols = append(cols, g.Grouping...)
	cols = append(cols, g.Aggregations...)

	return cols
}

func (j *Join) GetColumns() Columns {
	return j.EmitColumns
}

func (u *Union) GetColumns() Columns {
	return u.Columns
}