// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"io"
	"testing"
	"vitess.io/vitess/go/mysql"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/go-mysql-server/sql"
)

func TestShowWarnings(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	ctx.Session.Warn(&sql.Warning{Level: "l1", Message: "w1", Code: mysql.ErrorCode(1)})
	ctx.Session.Warn(&sql.Warning{Level: "l2", Message: "w2", Code: mysql.ErrorCode(2)})
	ctx.Session.Warn(&sql.Warning{Level: "l4", Message: "w3", Code: mysql.ErrorCode(3)})

	sw := ShowWarnings(ctx.Session.Warnings())
	require.True(sw.Resolved())

	it, err := sw.RowIter(ctx, nil)
	require.NoError(err)

	n := mysql.ErrorCode(3)
	for row, err := it.Next(ctx); err == nil; row, err = it.Next(ctx) {
		level := row[0].(string)
		code := row[1].(mysql.ErrorCode)
		message := row[2].(string)

		t.Logf("level: %s\tcode: %v\tmessage: %s\n", level, code, message)

		require.Equal(n, code)
		n--
	}
	if err != io.EOF {
		require.NoError(err)
	}
	require.NoError(it.Close(ctx))
}