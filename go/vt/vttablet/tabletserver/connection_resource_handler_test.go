/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletserver

import (
	"testing"

	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestConnectionStartsClosed(t *testing.T) {
	resourceHandler := newConnectionResourceHandler()
	_, err := resourceHandler.ClaimExclusiveConnection(context.Background(), &querypb.ExecuteOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "conn pool is closed")
	resourceHandler.Close()
}

func TestCanOpenResourceHandler(t *testing.T) {
	db := fakesqldb.New(t)
	resourceHandler := newConnectionResourceHandler()
	resourceHandler.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	_, err := resourceHandler.ClaimExclusiveConnection(context.Background(), &querypb.ExecuteOptions{})
	require.NoError(t, err)
}

func TestCanGetConnectionById(t *testing.T) {
	db := fakesqldb.New(t)
	resourceHandler := newConnectionResourceHandler()
	resourceHandler.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	conn, err := resourceHandler.ClaimExclusiveConnection(context.Background(), &querypb.ExecuteOptions{})
	require.NoError(t, err)

	gotConn, err := resourceHandler.Get(conn.ConnectionID, "test")
	require.NoError(t, err)
	require.Equal(t, conn, gotConn)
}

func TestStrayConnectionsGetTerminated(t *testing.T) {
	db := fakesqldb.New(t)
	resourceHandler := newConnectionResourceHandler()
	resourceHandler.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	conn, err := resourceHandler.ClaimExclusiveConnection(context.Background(), &querypb.ExecuteOptions{})
	require.NoError(t, err)

	require.False(t, conn.dbConn.IsClosed(), "connection should not be closed")
	resourceHandler.Close()

	require.Nil(t, conn.dbConn, "connection should have been closed")
}

func TestTxPoolClientRowsFound(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("begin", &sqltypes.Result{})
	txPool := newConnectionResourceHandler()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	ctx := context.Background()

	startNormalSize := txPool.conns.Available()
	startFoundRowsSize := txPool.foundRowsPool.Available()

	// Start a 'normal' transaction. It should take a connection
	// for the normal 'conns' pool.
	conn1, err := txPool.ClaimExclusiveConnection(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	assert.Equal(t, txPool.conns.Available(), startNormalSize-1)
	assert.Equal(t, txPool.foundRowsPool.Available(), startFoundRowsSize)

	// Start a 'foundRows' transaction. It should take a connection
	// from the foundRows pool.
	conn2, err := txPool.ClaimExclusiveConnection(ctx, &querypb.ExecuteOptions{ClientFoundRows: true})
	require.NoError(t, err)
	require.Equal(t, txPool.conns.Available(), startNormalSize-1)
	require.Equal(t, txPool.foundRowsPool.Available(), startFoundRowsSize-1)

	// Recycle the first transaction. The conn should be returned to
	// the conns pool.
	conn1.Recycle()
	require.Equal(t, txPool.conns.Available(), startNormalSize)
	require.Equal(t, txPool.foundRowsPool.Available(), startFoundRowsSize-1)

	// Recycle the second transaction. The conn should be returned to
	// the foundRows pool.
	conn2.Recycle()
	require.Equal(t, txPool.conns.Available(), startNormalSize)
	require.Equal(t, txPool.foundRowsPool.Available(), startFoundRowsSize)
}
