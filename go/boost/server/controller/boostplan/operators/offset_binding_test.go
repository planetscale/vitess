package operators

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestCalculateOffsets(t *testing.T) {
	colName := &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("colName")}

	// colName = 12
	expr := &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     colName,
		Right:    sqlparser.NewIntLiteral("12"),
	}
	t1 := semantics.SingleTableSet(0)
	semTable := semantics.EmptySemTable()
	semTable.Direct[colName] = t1
	opF := &Filter{
		Predicates: expr,
	}

	opT := &Table{
		TableName: "tbl",
	}

	conv := NewConverter()
	tNode := conv.NewNode("table", opT, nil)
	fNode := conv.NewNode("filter", opF, []*Node{tNode})
	tNode.Columns = Columns{
		{
			AST:  []sqlparser.Expr{colName},
			Name: "colName",
		},
	}
	err := bindOffsets(fNode, semTable)
	require.NoError(t, err)
	assert.NotNil(t, opF.EvalExpr)
	assert.Equal(t, ":0 = 12", sqlparser.String(opF.EvalExpr[0]))
}
