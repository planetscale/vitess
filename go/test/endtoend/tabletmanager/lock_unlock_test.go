/*
Copyright 2019 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
)

func TestLockAndUnlock(t *testing.T) {
	ctx := context.Background()

	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	if err != nil {
		t.Fatal(err)
	}
	defer masterConn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	if err != nil {
		t.Fatal(err)
	}
	defer replicaConn.Close()

	// first make sure that our writes to the master make it to the replica
	exec(t, masterConn, "insert into t1(id, value) values(1,'a'), (2,'b')")

	time.Sleep(200 * time.Millisecond)
	// TODO a loop re-try
	qr := exec(t, replicaConn, "select value from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("a")] [VARCHAR("b")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// now lock the replica
	err = tmcLockTables(ctx, replicaTabletGrpcPort)
	if err != nil {
		t.Fatal(err)
	}
	// make sure that writing to the master does not show up on the replica while locked
	exec(t, masterConn, "insert into t1(id, value) values(3,'c')")

	// TODO a loop re-try
	time.Sleep(500 * time.Millisecond)
	qr = exec(t, replicaConn, "select value from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("a")] [VARCHAR("b")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// finally, make sure that unlocking the replica leads to the previous write showing up
	err = tmcUnlockTables(ctx, replicaTabletGrpcPort)
	if err != nil {
		t.Fatal(err)
	}

	// TODO a loop re-try
	time.Sleep(500 * time.Millisecond)
	qr = exec(t, replicaConn, "select value from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("a")] [VARCHAR("b")] [VARCHAR("c")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}
