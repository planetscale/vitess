package binlog

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
)

/*
Environment: setup mysql server?

For each test send event by inserting row and check that event is correctly received and also validate pos using MasterPosition if applicable
or sequentially the next one.
Use metrics for testing from the start

* first time, no producer. first consumer creates producer
	* start with pos
	* start with "current"

* join previous with "current", should not increment producer count

* join previous with valid future pos (test skipped events)

* start with valid past pos: should start new producer and join existing consumers on it

* join way in the past AND way in the future and should start own streams (test with small allowed lag)


Mimic scale
* start several (>100?!) consumers on same producer, kill randomly until 0 is reached. Then start a couple more


e2e
* keep parallel running movetables, reshard, vstreamapi



 */

func TestPool1(t *testing.T) {
	ctx := context.Background()
	bc, err := NewFakeBinlogConnection(dbconfigs.Connector{})

	pos := mysql.Position{}
	startPos, eventCh, teardown, err := theStreamerPool.get(ctx, bc, pos)
	require.NoError(t, err)
	require.NotNil(t, startPos)

}
