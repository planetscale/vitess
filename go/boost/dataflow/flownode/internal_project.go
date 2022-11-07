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

type (
	Project struct {
		src  boostpb.IndexPair
		cols int

		projections []Projection
	}

	Projection interface {
		describe() string
		ToProto() string
		Type(g *graph.Graph[*Node], src boostpb.IndexPair) boostpb.Type
	}

	Expr struct {
		AST      sqlparser.Expr
		EvalExpr evalengine.Expr
	}

	Emit int

	Literal struct {
		V   boostpb.Value
		AST *sqlparser.Literal
	}
)

func (e Emit) proj()    {}
func (e Expr) proj()    {}
func (e Literal) proj() {}

var _ Projection = Expr{}
var _ Projection = Emit(1)

func (p *Project) internal() {}

func (p *Project) dataflow() {}

func (p *Project) DataflowNode() {}

func (p *Project) Ancestors() []graph.NodeIdx {
	return []graph.NodeIdx{p.src.AsGlobal()}
}

func (p *Project) SuggestIndexes(graph.NodeIdx) map[graph.NodeIdx][]int {
	return nil
}

func (p *Project) Resolve(int) []NodeColumn {
	// TODO implement me
	panic("implement me")
}

func (p *Project) ParentColumns(col int) []NodeColumn {
	var nc = NodeColumn{
		Node:   p.src.AsGlobal(),
		Column: col,
	}

	if len(p.projections) != 0 {
		offset, ok := p.projections[col].(Emit)
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

func (e Emit) Type(g *graph.Graph[*Node], src boostpb.IndexPair) boostpb.Type {
	return g.Value(src.AsGlobal()).ColumnType(g, int(e))
}
func (e Expr) Type(g *graph.Graph[*Node], src boostpb.IndexPair) boostpb.Type {
	var env = evalengine.EmptyExpressionEnv()

	var parent = g.Value(src.AsGlobal())
	parent.ResolveSchema(g)

	for _, tt := range parent.Schema() {
		env.Row = append(env.Row, sqltypes.MakeTrusted(tt.T, nil))
	}

	tt, err := env.TypeOf(e.EvalExpr)
	if err != nil {
		panic(err)
	}
	return boostpb.Type{T: tt}
}
func (e Literal) Type(*graph.Graph[*Node], boostpb.IndexPair) boostpb.Type {
	return boostpb.Type{T: e.V.Type()}
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

func (e Emit) describe() string {
	return strconv.Itoa(int(e))
}
func (e Expr) describe() string {
	return sqlparser.String(e.AST)
}
func (e Literal) describe() string {
	return fmt.Sprintf("lit: %s", e.V.ToVitessUnsafe().String())
}

func (e Emit) ToProto() string {
	return sqlparser.String(&sqlparser.Offset{V: int(e)})
}

func (e Expr) ToProto() string {
	return sqlparser.String(e.AST)
}

func (e Literal) ToProto() string {
	return sqlparser.String(e.AST)
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
		offset, ok := col.(Emit)
		if !ok {
			return
		}
		if i != int(offset) {
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
			var row = record.Row
			var sign = record.Positive
			var rb = boostpb.NewRowBuilder(len(emit))
			for _, col := range emit {
				switch col := col.(type) {
				case Emit:
					rb.Add(row.ValueAt(int(col)))
				case Expr:
					if env.Row == nil {
						env.Row = row.ToVitess()
					}
					er, err := env.Evaluate(col.EvalExpr)
					if err != nil {
						panic(err)
					}
					rb.AddVitess(er.Value())
				case Literal:
					rb.Add(col.V)
				}

			}
			rs[i] = rb.Finish().ToRecord(sign)
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
			expressions[i] = Emit(expr.V)
		case *sqlparser.Literal:
			lit, err := ProjectionLiteralFromAST(expr)
			if err != nil {
				return nil, err
			}
			expressions[i] = lit
		default:
			ee, err := evalengine.Translate(expr, nil)
			if err != nil {
				return nil, err
			}
			expressions[i] = Expr{
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

func ProjectionLiteralFromAST(expr *sqlparser.Literal) (Literal, error) {
	lit, err := evalengine.LiteralToValue(expr)
	if err != nil {
		return Literal{}, err
	}
	v := Literal{
		V:   boostpb.ValueFromVitess(lit),
		AST: expr,
	}
	return v, nil
}
