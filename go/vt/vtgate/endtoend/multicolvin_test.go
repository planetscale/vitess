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

package endtoend

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql"
)

func TestMulticolvin(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	//Multicolvin tests a table with a multi column vindex
	exec(t, conn, "insert into vt_multicolvin (cola, colb, colc, kid) values ('cola_value', 'colb_value', 'colc_value',5)")
	qr := exec(t, conn, "select cola,colb,colc,kid from vt_multicolvin where kid = 5")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("cola_value") VARCHAR("colb_value") VARCHAR("colc_value") INT64(5)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Updating both vindexes
	exec(t, conn, "update vt_multicolvin set cola = 'cola_newvalue', colb = 'colb_newvalue', colc = 'colc_newvalue' where kid = 5")
	qr = exec(t, conn, "select cola,colb,colc,kid from vt_multicolvin where kid = 5")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("cola_newvalue") VARCHAR("colb_newvalue") VARCHAR("colc_newvalue") INT64(5)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	qr = exec(t, conn, "select cola from cola_map")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("cola_newvalue")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	qr = exec(t, conn, "select colb,colc from colb_colc_map")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("colb_newvalue") VARCHAR("colc_newvalue")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Updating only one vindex
	exec(t, conn, "update vt_multicolvin set colb = 'colb_newvalue2', colc = 'colc_newvalue2' where kid = 5")
	qr = exec(t, conn, "select cola,colb,colc,kid from vt_multicolvin where kid = 5")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("cola_newvalue") VARCHAR("colb_newvalue2") VARCHAR("colc_newvalue2") INT64(5)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	qr = exec(t, conn, "select cola from cola_map")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("cola_newvalue")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	qr = exec(t, conn, "select colb,colc from colb_colc_map")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("colb_newvalue2") VARCHAR("colc_newvalue2")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Works when inserting multiple rows
	exec(t, conn, "insert into vt_multicolvin (cola, colb, colc, kid) values ('cola0_value', 'colb0_value', 'colc0_value', 6),('cola1_value', 'colb1_value', 'colc1_value', 7)")
	qr = exec(t, conn, "select cola,colb,colc,kid from vt_multicolvin order by kid")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("cola_newvalue") VARCHAR("colb_newvalue2") VARCHAR("colc_newvalue2") INT64(5)] [VARCHAR("cola0_value") VARCHAR("colb0_value") VARCHAR("colc0_value") INT64(6)] [VARCHAR("cola1_value") VARCHAR("colb1_value") VARCHAR("colc1_value") INT64(7)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}
