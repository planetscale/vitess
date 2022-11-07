package operators

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	t1 = &Table{
		Keyspace:  "ks",
		TableName: "t1",
	}

	t2 = &Table{
		Keyspace:  "ks",
		TableName: "t2",
	}
)

func TestSignature(t *testing.T) {
	tests := []struct {
		name string
		node Node
		want QuerySignature
	}{
		{
			name: "Simple table",
			node: Node{
				Columns: nil,
				Op:      t1,
			},
			want: QuerySignature{
				Tables:     []string{"ks.t1"},
				Predicates: nil,
			},
		}, {
			name: "Simple Join",
			node: Node{
				Op: &Join{
					Predicates: convertToExpr("id > col"),
				},
				Ancestors: []*Node{
					{
						Op: t1,
					},
					{
						Op: t1,
					},
				},
			},
			want: QuerySignature{
				Tables:     []string{"ks.t1"},
				Predicates: []string{"id > col"},
			},
		}, {
			name: "Simple Join - different tables",
			node: Node{
				Op: &Join{
					Predicates: convertToExpr("id > col"),
				},
				Ancestors: []*Node{
					{
						Op: t1,
					},
					{
						Op: t2,
					},
				},
			},
			want: QuerySignature{
				Tables:     []string{"ks.t1", "ks.t2"},
				Predicates: []string{"id > col"},
			},
		}, {
			name: "Simple Filter",
			node: Node{
				Op: &Filter{
					Predicates: convertToExpr("id > col"),
				},
				Ancestors: []*Node{
					{
						Op: t1,
					},
				},
			},
			want: QuerySignature{
				Tables:     []string{"ks.t1"},
				Predicates: []string{"id > col"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.node.Signature())
		})
	}
}

func convertToExpr(expr string) sqlparser.Expr {
	expression, err := sqlparser.ParseExpr(expr)
	if err != nil {
		panic(err)
	}
	return expression
}

func TestEqualSignature(t *testing.T) {
	tests := []struct {
		name          string
		nodes         []Node
		shouldBeEqual bool
	}{
		{
			name: "Simple tables",
			nodes: []Node{
				{
					Op: t1,
				},
				{
					Op: t1,
				},
			},
			shouldBeEqual: true,
		}, {
			name: "Simple Join",
			nodes: []Node{
				{
					Op: &Join{
						Predicates: convertToExpr("id > col"),
					},
					Ancestors: []*Node{
						{
							Op: t1,
						},
						{
							Op: t2,
						},
					},
				},
				{
					Op: &Join{
						Predicates: convertToExpr("id > col"),
					},
					Ancestors: []*Node{
						{
							Op: t2,
						},
						{
							Op: t1,
						},
					},
				},
			},
			shouldBeEqual: true,
		}, {
			name: "Simple Filter",
			nodes: []Node{
				{
					Op: &Filter{
						Predicates: convertToExpr("id > col and id2 > col2"),
					},
					Ancestors: []*Node{
						{
							Op: t1,
						},
					},
				},
				{
					Op: &Filter{
						Predicates: convertToExpr("id2 > col2 and id > col"),
					},
					Ancestors: []*Node{
						{
							Op: t1,
						},
					},
				},
			},
			shouldBeEqual: true,
		}, {
			name: "Simple unequal tables",
			nodes: []Node{
				{
					Op: t1,
				},
				{
					Op: t2,
				},
			},
			shouldBeEqual: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var hash Hash
			for idx, node := range tt.nodes {
				if idx == 0 {
					hash = node.Signature().Hash()
					continue
				}
				if tt.shouldBeEqual {
					require.Equal(t, hash, node.Signature().Hash())
					continue
				}
				require.NotEqual(t, hash, node.Signature().Hash())
			}
		})
	}
}
