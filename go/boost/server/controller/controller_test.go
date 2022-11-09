package controller

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	"vitess.io/vitess/go/boost/server/controller/boostplan"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
)

func Test_parseErrors(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		system []string
		query  map[string]string
	}{
		{
			name:  "nil",
			query: make(map[string]string),
		},
		{
			name:   "opaque",
			err:    io.EOF,
			system: []string{io.EOF.Error()},
			query:  make(map[string]string),
		},
		{
			name: "query error",
			err: multierr.Append(
				errors.New("failed to materialize recipe"),
				&boostplan.QueryScopedError{
					Err: errors.New("query unsupported"),
					Query: &boostplan.CachedQuery{
						CachedQuery: &vtboostpb.CachedQuery{
							PublicId: "123",
						},
					},
				},
			),
			system: []string{"failed to materialize recipe"},
			query: map[string]string{
				"123": `query unsupported (in query "123")`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			system, query := parseErrors(tt.err)
			require.Equal(t, tt.system, system)
			require.Equal(t, tt.query, query)
		})
	}
}
