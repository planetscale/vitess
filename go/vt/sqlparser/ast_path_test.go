package sqlparser

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPathWalk(t *testing.T) {
	ast, err := NewTestParser().ParseExpr("tbl.foo + 12 = tbl.bar")
	require.NoError(t, err)
	path := ASTPath{
		ComparisonExprLeft,
		BinaryExprLeft,
	}

	byPath := GetNodeByPath(ast, path)
	fmt.Println(String(byPath))
}
