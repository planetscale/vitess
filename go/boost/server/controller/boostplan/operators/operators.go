package operators

import (
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/tools/graphviz"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	ID = int

	Operator interface {
		Signature() QuerySignature
		addToGraph(g *graphviz.Graph) (*graphviz.Node, error)

		// AddColumns will add columns to this operator, and return a slice of Columns that this operator
		// needs to receive from its ancestors in order to do it's work
		AddColumns(ctx *PlanContext, col Columns) (Columns, error)

		// IntroducesTableID will return a non-nil value for operators that introduces new columns, like aggregation and tables do.
		IntroducesTableID() *semantics.TableSet

		// KeepAncestorColumns should return true if the operator passes rows through, like filtering and ordering do.
		KeepAncestorColumns() bool

		PlanOffsets(node *Node, st *semantics.SemTable) error

		Equals(st *semantics.SemTable, op Operator) bool
	}

	// ColumnHolder is used by operators that already at creation time know which columns they will have
	ColumnHolder interface {
		GetColumns() Columns
	}

	doesNotIntroduceColumn   struct{}
	dontKeepsAncestorColumns struct{}
	keepsAncestorColumns     struct{}

	Table struct {
		Keyspace    string
		TableName   string
		VColumns    []vindexes.Column
		ColumnSpecs []ColumnSpec
		Keys        []Column
		Spec        *sqlparser.TableSpec

		doesNotIntroduceColumn
		dontKeepsAncestorColumns
	}

	ColumnSpec struct {
		Column   *sqlparser.ColumnDefinition
		ColumnID *int
	}

	Join struct {
		Predicates sqlparser.Expr
		Inner      bool

		EmitColumns Columns

		// These are the columns that will be compared.
		// The size of these two slices must be the same
		Emit []flownode.JoinSource

		doesNotIntroduceColumn
		dontKeepsAncestorColumns
	}

	Filter struct {
		// The original AST expression
		Predicates sqlparser.Expr

		// The evalengine expression that will actually run.
		// It will do the comparisons using offsets
		EvalExpr []evalengine.Expr
		ExprStr  []string

		doesNotIntroduceColumn
		keepsAncestorColumns
	}

	GroupBy struct {
		Grouping     Columns
		Aggregations []sqlparser.AggrFunc
		TableID      semantics.TableSet

		GroupingIdx       []int
		AggregationsIdx   []int
		AggregationsTypes []flownode.AggregationKind

		dontKeepsAncestorColumns
	}

	Project struct {
		// TableID is set only we project a derived table, in which case the TableID equal
		// the TableID of the derived table.
		TableID *semantics.TableSet
		Columns Columns

		// These are not filled in at creation,
		// but rather after the operator tree has stopped iterating
		Projections []flownode.Projection

		dontKeepsAncestorColumns
	}

	View struct {
		Parameters []Parameter
		Columns    Columns

		ParametersIdx []int

		doesNotIntroduceColumn
		dontKeepsAncestorColumns
	}

	Union struct {
		InputColumns [2]Columns // These are the columns we need to read from the ancestors
		Columns      Columns    // These are the columns being exposed through this operator

		ColumnsIdx [2][]int

		doesNotIntroduceColumn
		keepsAncestorColumns
	}

	// TopK is used to implement a query with ORDER BY & LIMIT
	TopK struct {
		Order      sqlparser.OrderBy
		K          uint
		Parameters []*Column

		OrderOffsets, ParamOffsets []int

		doesNotIntroduceColumn
		keepsAncestorColumns
	}

	Distinct struct {
		doesNotIntroduceColumn
		keepsAncestorColumns
	}

	Parameter struct {
		Name string
		Op   sqlparser.ComparisonExprOperator

		key *Column
	}

	NodeTableRef struct {
		TableID semantics.TableSet
		Node    *Node
		Columns Columns
		Version int

		dontKeepsAncestorColumns
	}

	OrderedColumn struct {
		Offset int
		Dir    sqlparser.OrderDirection
	}

	RewriteOpFunc func(op *Node) (*Node, error)
)

var _ Operator = (*Table)(nil)
var _ Operator = (*Join)(nil)
var _ Operator = (*Filter)(nil)
var _ Operator = (*GroupBy)(nil)
var _ Operator = (*Project)(nil)
var _ Operator = (*View)(nil)
var _ Operator = (*NodeTableRef)(nil)
var _ Operator = (*Union)(nil)
var _ Operator = (*TopK)(nil)
var _ Operator = (*Distinct)(nil)

func rewrite(op *Node, f RewriteOpFunc) (*Node, error) {
	op, err := f(op)
	if err != nil {
		return nil, err
	}
	ops := op.Ancestors
	var newOps []*Node
	for _, operator := range ops {
		operator, err = rewrite(operator, f)
		if err != nil {
			return nil, err
		}
		newOps = append(newOps, operator)
	}
	op.Ancestors = newOps
	return op, nil
}

func (n *NodeTableRef) Adds() semantics.TableSet {
	return n.TableID
}

func (g *GroupBy) Adds() semantics.TableSet {
	return g.TableID
}

func (doesNotIntroduceColumn) IntroducesTableID() *semantics.TableSet {
	return nil
}

func (keepsAncestorColumns) KeepAncestorColumns() bool     { return true }
func (dontKeepsAncestorColumns) KeepAncestorColumns() bool { return false }

func (n *NodeTableRef) IntroducesTableID() *semantics.TableSet {
	return &n.TableID
}

func (g *GroupBy) IntroducesTableID() *semantics.TableSet {
	id := g.TableID
	return &id
}

func (p *Project) IntroducesTableID() *semantics.TableSet {
	return p.TableID
}
