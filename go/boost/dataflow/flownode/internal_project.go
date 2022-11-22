package flownode

import (
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Internal = (*Project)(nil)
var _ ingredientQueryThrough = (*Project)(nil)

type (
	Project struct {
		src  boostpb.IndexPair
		cols int

		projections []Projection
	}

	Projection interface {
		describe() string
		build(env *evalengine.ExpressionEnv, row boostpb.Row, output *boostpb.RowBuilder)
		ToProto() string
		Type(g *graph.Graph[*Node], src boostpb.IndexPair) boostpb.Type
	}

	ProjectedExpr struct {
		AST      sqlparser.Expr
		EvalExpr evalengine.Expr
	}

	ProjectedCol int

	ProjectedLiteral struct {
		V   boostpb.Value
		AST *sqlparser.Literal
	}
)

func (p *Project) QueryThrough(columns []int, key boostpb.Row, nodes *Map, states *state.Map) (RowIterator, bool, MaterializedState) {
	inCols := columns
	proj := p.projections

	if len(proj) > 0 {
		inCols = make([]int, 0, len(columns))
		for _, c := range columns {
			switch col := p.projections[c].(type) {
			case ProjectedCol:
				inCols = append(inCols, int(col))
			default:
				panic("should never be queried for generated columns")
			}
		}
	}

	rows, found, materialized := nodeLookup(p.src.Local, inCols, key, nodes, states)
	if !materialized {
		return nil, false, NotMaterialized
	}
	if !found {
		return nil, false, IsMaterialized
	}
	if len(proj) == 0 {
		return rows, true, IsMaterialized
	}

	result := make(RowSlice, 0, rows.Len())

	var env evalengine.ExpressionEnv
	env.DefaultCollation = collations.Default()

	rows.ForEach(func(row boostpb.Row) {
		builder := boostpb.NewRowBuilder(len(proj))
		for _, col := range proj {
			col.build(&env, row, &builder)
		}
		result = append(result, builder.Finish())
		env.Row = nil
	})

	return result, true, IsMaterialized
}

var _ Projection = (*ProjectedExpr)(nil)
var _ Projection = ProjectedCol(1)
var _ Projection = (*ProjectedLiteral)(nil)

func (p *Project) internal() {}

func (p *Project) dataflow() {}

func (p *Project) DataflowNode() {}

func (p *Project) Ancestors() []graph.NodeIdx {
	return []graph.NodeIdx{p.src.AsGlobal()}
}

func (p *Project) SuggestIndexes(graph.NodeIdx) map[graph.NodeIdx][]int {
	return nil
}

func (p *Project) resolveColumn(col int) int {
	if proj := p.projections; len(proj) > 0 {
		switch col := proj[col].(type) {
		case ProjectedCol:
			return int(col)
		default:
			panic("can't resolve literal column")
		}
	}
	return col
}

func (p *Project) Resolve(col int) []NodeColumn {
	return []NodeColumn{
		{
			Node:   p.src.AsGlobal(),
			Column: p.resolveColumn(col),
		},
	}
}

func (p *Project) ParentColumns(col int) []NodeColumn {
	var nc = NodeColumn{
		Node:   p.src.AsGlobal(),
		Column: col,
	}

	if len(p.projections) != 0 {
		offset, ok := p.projections[col].(ProjectedCol)
		if ok {
			nc.Column = int(offset)
		} else {
			nc.Column = -1
		}
	}
	return []NodeColumn{nc}
}

func (p *Project) ColumnType(g *graph.Graph[*Node], col int) boostpb.Type {
	if len(p.projections) == 0 {
		return g.Value(p.src.AsGlobal()).ColumnType(g, col)
	}
	expr := p.projections[col]
	return expr.Type(g, p.src)
}

func (p *Project) Description(detailed bool) string {
	if !detailed {
		return "π"
	}
	if len(p.projections) == 0 {
		return "π[*]"
	}
	var cols []string
	for _, p := range p.projections {
		cols = append(cols, p.describe())
	}

	return "π[" + strings.Join(cols, ", ") + "]"
}

func (p *Project) OnConnected(graph *graph.Graph[*Node]) {
	p.cols = len(graph.Value(p.src.AsGlobal()).Fields())
}

func (p *Project) OnCommit(_ graph.NodeIdx, remap map[graph.NodeIdx]boostpb.IndexPair) {
	p.src.Remap(remap)

	// Eliminate emit specifications which require no permutation of
	// the inputs, so we don't needlessly perform extra work on each
	// update.
	for i, col := range p.projections {
		switch col := col.(type) {
		case ProjectedCol:
			if i != int(col) {
				return
			}
		default:
			return
		}
	}

	p.projections = []Projection{}
}

func (p *Project) OnInput(
	_ *Node,
	_ processing.Executor,
	_ boostpb.LocalNodeIndex,
	rs []boostpb.Record,
	_ []int,
	_ *Map,
	_ *state.Map,
) (processing.Result, error) {
	if emit := p.projections; len(emit) > 0 {
		var env evalengine.ExpressionEnv
		env.DefaultCollation = collations.Default()

		for i, record := range rs {
			builder := boostpb.NewRowBuilder(len(emit))
			for _, col := range emit {
				col.build(&env, record.Row, &builder)
			}
			rs[i] = builder.Finish().ToRecord(record.Positive)
			env.Row = nil
		}
	}
	return processing.Result{Records: rs}, nil
}

func (p *Project) Emits() []Projection {
	return p.projections
}

func NewProject(src graph.NodeIdx, projections []Projection) *Project {
	return &Project{
		src:         boostpb.NewIndexPair(src),
		cols:        0,
		projections: projections,
	}
}

func NewProjectFromProto(proj *boostpb.Node_InternalProject) (*Project, error) {
	expressions := make([]Projection, len(proj.Projections))
	for i, expression := range proj.Projections {
		expr, err := sqlparser.ParseExpr(expression)
		if err != nil {
			return nil, err
		}

		switch expr := expr.(type) {
		case *sqlparser.Offset:
			expressions[i] = ProjectedCol(expr.V)
		case *sqlparser.Literal:
			lit, err := ProjectedLiteralFromAST(expr)
			if err != nil {
				return nil, err
			}
			expressions[i] = lit
		default:
			ee, err := evalengine.Translate(expr, nil)
			if err != nil {
				return nil, err
			}
			expressions[i] = &ProjectedExpr{
				AST:      expr,
				EvalExpr: ee,
			}
		}
	}
	return &Project{
		src:         *proj.Src,
		cols:        proj.Cols,
		projections: expressions,
	}, nil
}

func (p *Project) ToProto() *boostpb.Node_InternalProject {
	expressions := make([]string, len(p.projections))
	for i, e := range p.projections {
		expressions[i] = e.ToProto()
	}
	return &boostpb.Node_InternalProject{
		Src:         &p.src,
		Cols:        p.cols,
		Projections: expressions,
	}
}

func (col ProjectedCol) Type(g *graph.Graph[*Node], src boostpb.IndexPair) boostpb.Type {
	return g.Value(src.AsGlobal()).ColumnType(g, int(col))
}

func (col ProjectedCol) describe() string {
	return strconv.Itoa(int(col))
}

func (col ProjectedCol) ToProto() string {
	return sqlparser.String(&sqlparser.Offset{V: int(col)})
}

func (col ProjectedCol) build(env *evalengine.ExpressionEnv, row boostpb.Row, output *boostpb.RowBuilder) {
	output.Add(row.ValueAt(int(col)))
}

func defaultCollationForType(t sqltypes.Type) collations.ID {
	switch {
	case sqltypes.IsText(t):
		return collations.ID(collations.Local().DefaultConnectionCharset())
	default:
		return collations.CollationBinaryID
	}
}

func (expr *ProjectedExpr) Type(g *graph.Graph[*Node], src boostpb.IndexPair) boostpb.Type {
	var env = evalengine.EmptyExpressionEnv()

	var parent = g.Value(src.AsGlobal())
	parent.ResolveSchema(g)

	for _, tt := range parent.Schema() {
		env.Row = append(env.Row, sqltypes.MakeTrusted(tt.T, nil))
	}

	tt, err := env.TypeOf(expr.EvalExpr)
	if err != nil {
		panic(err)
	}
	// FIXME: there are some expressions for which the evalengine can resolve
	// a non-default collation (e.g. an explicitly COLLATE('str'), but we have
	// no way of accessing this data right now
	return boostpb.Type{T: tt, Collation: defaultCollationForType(tt)}
}

func (expr *ProjectedExpr) describe() string {
	return sqlparser.String(expr.AST)
}

func (expr *ProjectedExpr) ToProto() string {
	return sqlparser.String(expr.AST)
}

func (expr *ProjectedExpr) build(env *evalengine.ExpressionEnv, row boostpb.Row, output *boostpb.RowBuilder) {
	if env.Row == nil {
		env.Row = row.ToVitess()
	}
	er, err := env.Evaluate(expr.EvalExpr)
	if err != nil {
		panic(err)
	}
	output.AddVitess(er.Value())
}

func (lit *ProjectedLiteral) Type(*graph.Graph[*Node], boostpb.IndexPair) boostpb.Type {
	tt := lit.V.Type()
	return boostpb.Type{T: tt, Collation: defaultCollationForType(tt)}
}

func (lit *ProjectedLiteral) describe() string {
	return fmt.Sprintf("lit: %s", lit.V.ToVitessUnsafe().String())
}

func (lit *ProjectedLiteral) ToProto() string {
	return sqlparser.String(lit.AST)
}

func (lit *ProjectedLiteral) build(env *evalengine.ExpressionEnv, row boostpb.Row, output *boostpb.RowBuilder) {
	output.Add(lit.V)
}

func ProjectedLiteralFromAST(expr *sqlparser.Literal) (*ProjectedLiteral, error) {
	lit, err := evalengine.LiteralToValue(expr)
	if err != nil {
		return nil, err
	}
	return &ProjectedLiteral{
		V:   boostpb.ValueFromVitess(lit),
		AST: expr,
	}, nil
}
