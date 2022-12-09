/*
Copyright 2022 The Vitess Authors.

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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TabletReshuffle test if a vttablet can be pointed at an existing mysql
func TestTabletReshuffle(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	var x = 0
	for i := 0; i < 10000; i++ {
		_, err := queryService.Execute(ctx, primaryTarget, fmt.Sprintf("insert into t1(id,value) values (%d, 'a'), (%d+1, 'a'), (%d+2, 'a'), (%d+3, 'a'), (%d+4, 'a'), (%d+5, 'a'), (%d+6, 'a'), (%d+7, 'a'), (%d+8, 'a'), (%d+9, 'a')", x, x, x, x, x, x, x, x, x, x), nil, 0, 0, &querypb.ExecuteOptions{})
		x += 10
		require.NoError(t, err)
	}

	log.Errorf("started stream execute")
	finished := make(chan bool)
	go func() {
		callbackCount := 0
		err := queryService.StreamExecute(ctx, primaryTarget, "select * from t1", nil, 0, 0, &querypb.ExecuteOptions{}, func(result *sqltypes.Result) error {
			callbackCount++
			time.Sleep(20 * time.Second)
			return nil
		})
		log.Errorf("Error from streamExecute - %v", err)
		assert.Equal(t, -1, callbackCount)
		finished <- true
	}()

	err := primaryTablet.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		_ = primaryTablet.MysqlctlProcess.Start()
	}()

	waitForCheckMySQLRunning(t, 1)
	select {
	case <-finished:
		log.Errorf("test finished")
		return
	case <-time.After(30 * time.Second):
		time.Sleep(120 * time.Second)
		t.Fatalf("StreamExecute didn't finish execution")
	}
}
