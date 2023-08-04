package sql

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql/sqlerror"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLErrorCast(t *testing.T) {

	tests := []struct {
		err  error
		code sqlerror.ErrorCode
	}{
		{ErrTableNotFound.New("table not found err"), sqlerror.ERNoSuchTable},
		{ErrInvalidType.New("unhandled mysql error"), sqlerror.ERUnknownError},
		{fmt.Errorf("generic error"), sqlerror.ERUnknownError},
		{nil, sqlerror.ERUnknownError},
	}

	for _, test := range tests {
		var nilErr *sqlerror.SQLError = nil
		t.Run(fmt.Sprintf("%v %v", test.err, test.code), func(t *testing.T) {
			err := CastSQLError(test.err)
			if err != nil {
				require.Error(t, err)
				assert.Equal(t, err.Number(), test.code)
			} else {
				assert.Equal(t, err, nilErr)
			}
		})
	}
}
