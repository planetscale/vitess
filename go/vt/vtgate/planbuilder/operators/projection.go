/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"fmt"
	"slices"
	"strings"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// Projection is used when we need to evaluate expressions on the vtgate
	// It uses the evalengine to accomplish its goal
	Projection struct {
		Source ops.Operator

		// TODO: we should replace these two slices with a single slice that contains both items. Keeping these two slices in sync leads to fragile code (systay 2023-07-25)
		// Columns contain the expressions as viewed from the outside of this operator
		Columns ProjCols

		// Projections will contain the actual evaluations we need to
		// do if this operator is still above a route after optimisation
		Projections []ProjExpr

		// TableID will be non-nil for derived tables
		TableID *semantics.TableSet
		Alias   string

		FromAggr bool
	}

	ProjCols interface {
		GetColumns() ([]*sqlparser.AliasedExpr, error)
		AddColumn(*sqlparser.AliasedExpr) (ProjCols, error)
	}

	// Used when there are stars in the expressions that we were unable to expand
	StarProjections sqlparser.SelectExprs

	// Used when we know all the columns
	AliasedProjections []*sqlparser.AliasedExpr

	ProjExpr interface {
		GetExpr() sqlparser.Expr
	}

	// Offset is used when we are only passing through data from an incoming column
	Offset struct {
		Expr   sqlparser.Expr
		Offset int
	}

	// Eval is used for expressions that have to be evaluated in the vtgate using the evalengine
	Eval struct {
		Expr  sqlparser.Expr
		EExpr evalengine.Expr
	}

	// UnexploredExpression is used before we have planned - one of two end results are possible for it
	// - we are able to push this projection under a route, and then this is not used at all - we'll just
	//   use the ColumnNames field of the Projection struct
	// - we have to evaluate this on the vtgate, and either it's just a copy from the input,
	//   or it's an evalengine expression that we have to evaluate
	UnexploredExpression struct {
		E sqlparser.Expr
	}
)

func (sp StarProjections) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return nil, vterrors.VT09015()
}

func (sp StarProjections) AddColumn(*sqlparser.AliasedExpr) (ProjCols, error) {
	return nil, vterrors.VT09015()
}

func (ap AliasedProjections) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return ap, nil
}

func (ap AliasedProjections) AddColumn(col *sqlparser.AliasedExpr) (ProjCols, error) {
	return append(ap, col), nil
}

var _ selectExpressions = (*Projection)(nil)

// createSimpleProjection returns a projection where all columns are offsets.
// used to change the name and order of the columns in the final output
func createSimpleProjection(ctx *plancontext.PlanningContext, qp *QueryProjection, src ops.Operator) (*Projection, error) {
	p := &Projection{
		Source: src,
	}

	var groupby []bool
	exprs, err := slice.MapWithError(qp.SelectExprs, func(from SelectExpr) (*sqlparser.AliasedExpr, error) {
		groupby = append(groupby, false)
		return from.GetAliasedExpr()
	})
	if err != nil {
		return nil, err
	}

	cols := AliasedProjections{}
	offsets, err := p.Source.AddColumns(ctx, true, groupby, exprs)
	if err != nil {
		return nil, err
	}
	for i := range exprs {
		offset, ae := offsets[i], exprs[i]
		p.Projections = append(p.Projections, Offset{Expr: ae.Expr, Offset: offset})
		cols = append(cols, ae)
	}
	p.Columns = cols
	return p, nil
}

func (p *Projection) addUnexploredExpr(ae *sqlparser.AliasedExpr, e sqlparser.Expr) (int, error) {
	var err error
	p.Columns, err = p.Columns.AddColumn(ae)
	if err != nil {
		return 0, err
	}
	p.Projections = append(p.Projections, UnexploredExpression{E: e})
	return len(p.Projections) - 1, nil
}

func (p *Projection) addColumnWithoutPushing(expr *sqlparser.AliasedExpr, _ bool) (int, error) {
	return p.addUnexploredExpr(expr, expr.Expr)
}

func (p *Projection) addColumnsWithoutPushing(ctx *plancontext.PlanningContext, reuse bool, _ []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	offsets := make([]int, len(exprs))
	for idx, expr := range exprs {
		if reuse {
			offset, err := p.FindCol(ctx, expr.Expr, true)
			if err != nil {
				return nil, err
			}
			if offset != -1 {
				offsets[idx] = offset
				continue
			}
		}
		offsets[idx], _ = p.addUnexploredExpr(expr, expr.Expr)
	}
	return offsets, nil
}

func (p *Projection) isDerived() bool {
	return p.TableID != nil
}

func (p *Projection) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	cols, err := p.Columns.GetColumns()
	if err != nil {
		return 0, err
	}
	if !(underRoute && p.isDerived()) {
		if offset, found := canReuseColumn(ctx, cols, expr, extractExpr); found {
			return offset, nil
		}
	}

	return -1, nil
}

// fetchExpr is used to accumulate all expressions we'll need from the input,
// and store in which column on the projection we want to store the offset returned
type fetchExpr struct {
	expr    sqlparser.Expr
	colIdx  []int
	groupBy bool
}

func (p *Projection) AddColumns(ctx *plancontext.PlanningContext, reuse bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	offsets := make([]int, len(exprs))
	var fetch []fetchExpr
	cols, err := p.Columns.GetColumns()
	if err != nil {
		return nil, err
	}
	startOffset := len(cols)
	for i, ae := range exprs {
		colIdx := i + startOffset
		expr := ae.Expr

		if p.TableID != nil {
			vt, err := ctx.SemTable.TableInfoFor(*p.TableID)
			if err != nil {
				return nil, err
			}
			expr = semantics.RewriteDerivedTableExpression(expr, vt)
		}

		if reuse {
			offset, err := p.FindCol(ctx, expr, false)
			if err != nil {
				return nil, err
			}
			if offset >= 0 {
				offsets[i] = offset
				continue
			}
		}

		// we add the column here, so we can find the expression in the next iteration of this loop,
		// but we wait with the actual projection until we have fetched it from the input
		offsets[i] = len(cols)
		cols = append(cols, aeWrap(expr))
		p.Projections = append(p.Projections, nil)

		// even if the receiver of the Projection output does not want to reuse column,
		// we can reuse columns from this input
		fIdx := slices.IndexFunc(fetch, func(f fetchExpr) bool {
			return ctx.SemTable.EqualsExprWithDeps(expr, f.expr)
		})

		if fIdx == -1 {
			// if we are not already asking for this expression, we add it to the list of expressions we'll ask for
			fIdx = len(fetch)
			fetch = append(fetch, fetchExpr{
				expr: expr,
			})
		}

		fetch[fIdx].colIdx = append(fetch[fIdx].colIdx, colIdx)
		fetch[fIdx].groupBy = fetch[fIdx].groupBy || addToGroupBy[i]
	}
	p.Columns = AliasedProjections(cols)

	askForExprs := make([]*sqlparser.AliasedExpr, len(fetch))
	askForGB := make([]bool, len(fetch))
	for i, f := range fetch {
		askForExprs[i] = aeWrap(f.expr)
		askForGB[i] = f.groupBy
	}

	inputOffsets, err := p.Source.AddColumns(ctx, true, askForGB, askForExprs)
	if err != nil {
		return nil, err
	}

	for fIdx, fetched := range fetch {
		for _, colIdx := range fetched.colIdx {
			p.Projections[colIdx] = Offset{Offset: inputOffsets[fIdx], Expr: fetched.expr}
		}
	}

	return offsets, nil
}

func (po Offset) GetExpr() sqlparser.Expr               { return po.Expr }
func (po Eval) GetExpr() sqlparser.Expr                 { return po.Expr }
func (po UnexploredExpression) GetExpr() sqlparser.Expr { return po.E }

func (p *Projection) Clone(inputs []ops.Operator) ops.Operator {
	return &Projection{
		Source:      inputs[0],
		Columns:     p.Columns,
		Projections: slices.Clone(p.Projections),
		TableID:     p.TableID,
		Alias:       p.Alias,
		FromAggr:    p.FromAggr,
	}
}

func (p *Projection) Inputs() []ops.Operator {
	return []ops.Operator{p.Source}
}

func (p *Projection) SetInputs(operators []ops.Operator) {
	p.Source = operators[0]
}

func (p *Projection) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	// we just pass through the predicate to our source
	src, err := p.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	p.Source = src
	return p, nil
}

func (p *Projection) GetColumns(*plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return p.Columns.GetColumns()
}

func (p *Projection) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return transformColumnsToSelectExprs(ctx, p)
}

func (p *Projection) GetOrdering() ([]ops.OrderBy, error) {
	return p.Source.GetOrdering()
}

// AllOffsets returns a slice of integer offsets for all columns in the Projection
// if all columns are of type Offset. If any column is not of type Offset, it returns nil.
func (p *Projection) AllOffsets() (cols []int) {
	for _, c := range p.Projections {
		offset, ok := c.(Offset)
		if !ok {
			return nil
		}

		cols = append(cols, offset.Offset)
	}
	return
}

func (p *Projection) ShortDescription() string {
	var columns []string
	if p.Alias != "" {
		columns = append(columns, "derived["+p.Alias+"]")
	}

	cols, _ := p.Columns.GetColumns()
	if len(cols) == 0 {
		// this means we are looking at a projection with at least one star in it

	}

	for i, col := range p.Projections {
		var aliasExpr *sqlparser.AliasedExpr
		if cols != nil {
			aliasExpr = cols[i]
			if aliasExpr.Expr == col.GetExpr() {
				columns = append(columns, sqlparser.String(aliasExpr))
				continue
			}
		}

		columns = append(columns, fmt.Sprintf("%s AS %s", sqlparser.String(col.GetExpr()), aliasExpr.ColumnName()))
	}
	return strings.Join(columns, ", ")
}

func (p *Projection) Compact(ctx *plancontext.PlanningContext) (ops.Operator, *rewrite.ApplyResult, error) {
	if p.isDerived() {
		return p, rewrite.SameTree, nil
	}

	// for projections that are not derived tables, we can check if it is safe to remove or not
	needed := false
	for i, projection := range p.Projections {
		e, ok := projection.(Offset)
		if !ok || e.Offset != i {
			needed = true
			break
		}
	}

	if !needed {
		return p.Source, rewrite.NewTree("removed projection only passing through the input", p), nil
	}

	switch src := p.Source.(type) {
	case *Route:
		return p.compactWithRoute(ctx, src)
	case *ApplyJoin:
		return p.compactWithJoin(ctx, src)
	}
	return p, rewrite.SameTree, nil
}

func (p *Projection) compactWithJoin(ctx *plancontext.PlanningContext, src *ApplyJoin) (ops.Operator, *rewrite.ApplyResult, error) {
	var newColumns []int
	var newColumnsAST []JoinColumn
	cols, err := p.Columns.GetColumns()
	if err != nil {
		return p, rewrite.SameTree, nil
	}
	for idx, col := range p.Projections {
		switch col := col.(type) {
		case Offset:
			newColumns = append(newColumns, src.Columns[col.Offset])
			newColumnsAST = append(newColumnsAST, src.JoinColumns[col.Offset])
		case UnexploredExpression:
			if !ctx.SemTable.EqualsExprWithDeps(col.E, cols[idx].Expr) {
				// the inner expression is different from what we are presenting to the outside - this means we need to evaluate
				return p, rewrite.SameTree, nil
			}
			offset := slices.IndexFunc(src.JoinColumns, func(jc JoinColumn) bool {
				return ctx.SemTable.EqualsExprWithDeps(jc.Original.Expr, col.E)
			})
			if offset < 0 {
				return p, rewrite.SameTree, nil
			}
			if len(src.Columns) > 0 {
				newColumns = append(newColumns, src.Columns[offset])
			}
			newColumnsAST = append(newColumnsAST, src.JoinColumns[offset])
		default:
			return p, rewrite.SameTree, nil
		}
	}
	src.Columns = newColumns
	src.JoinColumns = newColumnsAST
	return src, rewrite.NewTree("remove projection from before join", src), nil
}

func (p *Projection) compactWithRoute(ctx *plancontext.PlanningContext, rb *Route) (ops.Operator, *rewrite.ApplyResult, error) {
	for i, col := range p.Projections {
		offset, ok := col.(Offset)
		if !ok || offset.Offset != i {
			return p, rewrite.SameTree, nil
		}
	}
	columns, err := rb.GetColumns(ctx)
	if err != nil {
		return nil, nil, err
	}

	if len(columns) == len(p.Projections) {
		return rb, rewrite.NewTree("remove projection from before route", rb), nil
	}
	rb.ResultColumns = len(columns)
	return rb, rewrite.SameTree, nil
}

func (p *Projection) needsEvaluation(ctx *plancontext.PlanningContext, e sqlparser.Expr) bool {
	columns, err := p.Columns.GetColumns()
	if err != nil {
		return true
	}
	offset := slices.IndexFunc(columns, func(expr *sqlparser.AliasedExpr) bool {
		return ctx.SemTable.EqualsExprWithDeps(expr.Expr, e)
	})

	if offset < 0 {
		return false
	}

	inside := p.Projections[offset].GetExpr()
	outside := columns[offset].Expr
	return inside != outside
}

func (p *Projection) planOffsets(ctx *plancontext.PlanningContext) error {
	for i, col := range p.Projections {
		_, unexplored := col.(UnexploredExpression)
		if !unexplored {
			continue
		}

		// first step is to replace the expressions we expect to get from our input with the offsets for these
		expr := col.GetExpr()
		rewritten, err := useOffsets(ctx, expr, p)
		if err != nil {
			return err
		}

		offset, ok := rewritten.(*sqlparser.Offset)
		if ok {
			// we got a pure offset back. No need to do anything else
			p.Projections[i] = Offset{
				Expr:   expr,
				Offset: offset.V,
			}
			continue
		}

		// for everything else, we'll turn to the evalengine
		eexpr, err := evalengine.Translate(rewritten, nil)
		if err != nil {
			return err
		}

		p.Projections[i] = Eval{
			Expr:  rewritten,
			EExpr: eexpr,
		}
	}

	p.TableID = nil
	p.Alias = ""

	return nil
}

func (p *Projection) introducesTableID() semantics.TableSet {
	if p.TableID == nil {
		return semantics.EmptyTableSet()
	}
	return *p.TableID
}
