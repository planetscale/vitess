package flownode

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Internal = (*Project)(nil)
var _ ingredientQueryThrough = (*Project)(nil)

type (
	Project struct {
		src  dataflow.IndexPair
		cols int

		projections []Projection
	}

	Projection interface {
		describe() string
		build(env *evalengine.ExpressionEnv, row sql.Row, output *sql.RowBuilder)
		ToProto() string
		Type(g *graph.Graph[*Node], src dataflow.IndexPair) (sql.Type, error)
	}

	ProjectedExpr struct {
		AST      sqlparser.Expr
		EvalExpr evalengine.Expr
	}

	ProjectedCol int

	ProjectedLiteral struct {
		V   sql.Value
		AST *sqlparser.Literal
	}
)

func (p *Project) QueryThrough(columns []int, key sql.Row, nodes *Map, states *state.Map) (RowIterator, bool, MaterializedState) {
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

	rows.ForEach(func(row sql.Row) {
		builder := sql.NewRowBuilder(len(proj))
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
	if len(p.projections) == 0 {
		return []NodeColumn{{
			Node:   p.src.AsGlobal(),
			Column: col,
		}}
	}

	var cols []NodeColumn
	switch offset := p.projections[col].(type) {
	case ProjectedCol:
		cols = append(cols, NodeColumn{
			Node:   p.src.AsGlobal(),
			Column: int(offset),
		})
	case *ProjectedExpr:
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.Offset:
				cols = append(cols, NodeColumn{
					Node:       p.src.AsGlobal(),
					Column:     node.V,
					Functional: true,
				})
			}
			return true, nil
		}, offset.AST)
	default:
		cols = append(cols, NodeColumn{
			Node:   p.src.AsGlobal(),
			Column: -1,
		})
	}
	return cols
}

func (p *Project) ColumnType(g *graph.Graph[*Node], col int) (sql.Type, error) {
	if len(p.projections) == 0 {
		return g.Value(p.src.AsGlobal()).ColumnType(g, col)
	}
	expr := p.projections[col]
	return expr.Type(g, p.src)
}

func (p *Project) Description() string {
	if len(p.projections) == 0 {
		return "π[*]"
	}
	var cols []string
	for _, p := range p.projections {
		cols = append(cols, p.describe())
	}
	return "π[" + strings.Join(cols, ", ") + "]"
}

func (p *Project) OnConnected(graph *graph.Graph[*Node]) error {
	p.cols = len(graph.Value(p.src.AsGlobal()).Fields())

	// Eliminate emit specifications which require no permutation of
	// the inputs, so we don't needlessly perform extra work on each
	// update.
	for i, col := range p.projections {
		switch col := col.(type) {
		case ProjectedCol:
			if i != int(col) {
				return nil
			}
		default:
			return nil
		}
	}

	p.projections = []Projection{}
	return nil
}

func (p *Project) OnCommit(_ graph.NodeIdx, remap map[graph.NodeIdx]dataflow.IndexPair) {
	p.src.Remap(remap)
}

func (p *Project) OnInput(you *Node, ex processing.Executor, from dataflow.LocalNodeIdx, rs []sql.Record, repl replay.Context, domain *Map, states *state.Map) (processing.Result, error) {
	if emit := p.projections; len(emit) > 0 {
		var env evalengine.ExpressionEnv

		for i, record := range rs {
			builder := sql.NewRowBuilder(len(emit))
			for _, col := range emit {
				col.build(&env, record.Row, &builder)
			}
			rs[i] = builder.Finish().ToOffsetRecord(record.Offset, record.Positive)
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
		src:         dataflow.NewIndexPair(src),
		cols:        0,
		projections: projections,
	}
}

func NewProjectFromProto(proj *flownodepb.Node_InternalProject) (*Project, error) {
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

func (p *Project) ToProto() *flownodepb.Node_InternalProject {
	expressions := make([]string, len(p.projections))
	for i, e := range p.projections {
		expressions[i] = e.ToProto()
	}
	return &flownodepb.Node_InternalProject{
		Src:         &p.src,
		Cols:        p.cols,
		Projections: expressions,
	}
}

func (p *Project) projectOrder(order Order) Order {
	if order == nil {
		return nil
	}
	if len(p.projections) == 0 {
		return order
	}
	var newOrder Order
	for _, ord := range order {
		col := slices.IndexFunc(p.projections, func(proj Projection) bool {
			switch proj := proj.(type) {
			case ProjectedCol:
				return int(proj) == ord.Col
			default:
				return false
			}
		})
		if col >= 0 {
			newOrder = append(newOrder, OrderedColumn{Col: col, Desc: ord.Desc})
		}
	}
	return newOrder
}

func (col ProjectedCol) Type(g *graph.Graph[*Node], src dataflow.IndexPair) (sql.Type, error) {
	return g.Value(src.AsGlobal()).ColumnType(g, int(col))
}

func (col ProjectedCol) describe() string {
	return strconv.Itoa(int(col))
}

func (col ProjectedCol) ToProto() string {
	return sqlparser.String(&sqlparser.Offset{V: int(col)})
}

func (col ProjectedCol) build(env *evalengine.ExpressionEnv, row sql.Row, output *sql.RowBuilder) {
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

func (expr *ProjectedExpr) Type(g *graph.Graph[*Node], src dataflow.IndexPair) (sql.Type, error) {
	var env = evalengine.EmptyExpressionEnv()

	var parent = g.Value(src.AsGlobal())
	_, err := parent.ResolveSchema(g)
	if err != nil {
		return sql.Type{}, err
	}

	for _, tt := range parent.Schema() {
		env.Row = append(env.Row, sqltypes.MakeTrusted(tt.T, nil))
	}

	tt, _, err := env.TypeOf(expr.EvalExpr, nil)
	if err != nil {
		return sql.Type{}, err
	}
	// FIXME: there are some expressions for which the evalengine can resolve
	// a non-default collation (e.g. an explicitly COLLATE('str'), but we have
	// no way of accessing this data right now
	return sql.Type{T: tt, Collation: defaultCollationForType(tt)}, nil
}

func (expr *ProjectedExpr) describe() string {
	return sqlparser.String(expr.AST)
}

func (expr *ProjectedExpr) ToProto() string {
	return sqlparser.String(expr.AST)
}

func (expr *ProjectedExpr) build(env *evalengine.ExpressionEnv, row sql.Row, output *sql.RowBuilder) {
	if env.Row == nil {
		env.Row = row.ToVitess()
	}
	er, err := env.Evaluate(expr.EvalExpr)
	if err != nil {
		panic(err)
	}
	output.AddVitess(er.Value(collations.Default()))
}

func (lit *ProjectedLiteral) Type(*graph.Graph[*Node], dataflow.IndexPair) (sql.Type, error) {
	tt := lit.V.Type()
	return sql.Type{T: tt, Collation: defaultCollationForType(tt)}, nil
}

func (lit *ProjectedLiteral) describe() string {
	return fmt.Sprintf("lit: %s", lit.V.ToVitessUnsafe().String())
}

func (lit *ProjectedLiteral) ToProto() string {
	return sqlparser.String(lit.AST)
}

func (lit *ProjectedLiteral) build(env *evalengine.ExpressionEnv, row sql.Row, output *sql.RowBuilder) {
	output.Add(lit.V)
}

func ProjectedLiteralFromAST(expr *sqlparser.Literal) (*ProjectedLiteral, error) {
	lit, err := sqlparser.LiteralToValue(expr)
	if err != nil {
		return nil, err
	}
	return &ProjectedLiteral{
		V:   sql.ValueFromVitess(lit),
		AST: expr,
	}, nil
}
