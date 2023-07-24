package operators

import (
	"vitess.io/vitess/go/boost/server/controller/boostplan/viewplan"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	ID = int

	Operator interface {
		Signature() QuerySignature

		// AddColumns will add columns to this operator, and return a slice of Columns that this operator
		// needs to receive from its ancestors in order to do it's work
		AddColumns(ctx *PlanContext, col Columns) (Columns, error)

		// IntroducesTableID will return a non-nil value for operators that introduces new columns, like aggregation and tables do.
		IntroducesTableID() *semantics.TableSet

		// KeepAncestorColumns should return true if the operator passes rows through, like filtering and ordering do.
		KeepAncestorColumns() bool

		PlanOffsets(node *Node, st *semantics.SemTable, conn *Converter) error

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
		JoinColumns Columns

		// These are the columns that will be compared.
		// The size of these two slices must be the same
		On   [2]int
		Emit [][2]int

		doesNotIntroduceColumn
		dontKeepsAncestorColumns
	}

	Filter struct {
		// The original AST expression
		Predicates sqlparser.Expr

		// The evalengine expression that will actually run.
		// It will do the comparisons using offsets
		EvalExpr []sqlparser.Expr

		doesNotIntroduceColumn
		keepsAncestorColumns
	}

	// NullFilter is used when we have an outer join with additional predicates that can't be evaluated with a hash join
	// In these cases, we want to evaluate a filter on top of the join, and make the columns coming from the outer side
	// into nulls instead of filtering out the rows
	// After planning, this operator will be represented by a projection flownode
	NullFilter struct {
		Join *Join

		// The original AST expression
		Predicates sqlparser.Expr

		// These are not filled in at creation,
		// but rather after the operator tree has stopped iterating
		Projections []Projection

		// This is the table(s) that are on the outer side of the join
		OuterSide semantics.TableSet

		doesNotIntroduceColumn
		dontKeepsAncestorColumns
	}

	GroupBy struct {
		// Aggregations will contain AggrFuncs, plus any columns that are returned but not in the grouping clause
		Aggregations     Columns
		TableID          semantics.TableSet
		Grouping         Columns
		ImplicitGrouping Columns

		GroupingIdx     []int
		AggregationsIdx []int

		dontKeepsAncestorColumns
	}

	ProjectionKind int

	Projection struct {
		Kind     ProjectionKind
		AST      sqlparser.Expr
		Eval     evalengine.Expr
		Original sqlparser.Expr
		Column   int
	}

	Project struct {
		// These fields are only set if this is on top of a derived table
		TableID        *semantics.TableSet
		Alias          string
		DerivedColumns Columns

		Columns Columns

		// These are not filled in at creation,
		// but rather after the operator tree has stopped iterating
		Projections []Projection

		dontKeepsAncestorColumns
	}

	View struct {
		PublicID     string
		Dependencies []*Dependency
		PostFilter   Columns
		Columns      Columns
		Plan         *viewplan.Plan

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
		K          int
		Parameters []*Column

		OrderOffsets, ParamOffsets []int

		doesNotIntroduceColumn
		keepsAncestorColumns
	}

	Distinct struct {
		doesNotIntroduceColumn
		keepsAncestorColumns
	}

	Dependency struct {
		Name string

		// Op will be nil if it's not a parameter (implicit grouping that is needed for post-filter)
		Op           *sqlparser.ComparisonExprOperator
		ColumnOffset int
		Column       *Column
		Type         sqltypes.Type
	}

	NodeTableRef struct {
		TableID semantics.TableSet
		Node    *Node
		Columns Columns
		Version int
		Hints   sqlparser.IndexHints

		dontKeepsAncestorColumns
	}

	OrderedColumn struct {
		Offset int
		Dir    sqlparser.OrderDirection
	}

	TreeState bool

	RewriteOpFunc func(op *Node) (*Node, TreeState, error)
)

const (
	SameTree TreeState = false
	NewTree  TreeState = true
)

const (
	ProjectionColumn ProjectionKind = iota
	ProjectionLiteral
	ProjectionEval
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
var _ Operator = (*NullFilter)(nil)

func rewriteActually(op *Node, f RewriteOpFunc) (*Node, TreeState, error) {
	op, state, err := f(op)
	if err != nil {
		return nil, false, err
	}
	ops := op.Ancestors
	var newOps []*Node
	for _, operator := range ops {
		var childState TreeState
		operator, childState, err = rewriteActually(operator, f)
		if err != nil {
			return nil, false, err
		}
		if childState == NewTree {
			state = NewTree
		}
		newOps = append(newOps, operator)
	}
	op.Ancestors = newOps
	return op, state, nil

}
func rewrite(op *Node, f RewriteOpFunc) (*Node, error) {
	var state = NewTree
	var err error
	for state == NewTree {
		op, state, err = rewriteActually(op, f)
		if err != nil {
			return nil, err
		}
	}
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

func (g *GroupBy) ScalarAggregation() bool {
	return len(g.Grouping) == len(g.ImplicitGrouping)
}

func newDependency(name string, col *Column, op *sqlparser.ComparisonExprOperator, typ sqltypes.Type) *Dependency {
	return &Dependency{
		Name:         name,
		Op:           op,
		ColumnOffset: -1,
		Column:       col,
		Type:         typ,
	}
}
