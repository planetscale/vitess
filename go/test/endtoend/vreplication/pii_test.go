/*
Copyright 2021 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPiiFilter(t *testing.T) {
	defaultCellName := "zone1"
	allCellNames = "zone1"
	vc = InitCluster(t, []string{defaultCellName})
	require.NotNil(t, vc)
	defaultReplicas = 0 // because of CI resource constraints we can only run this test with master tablets
	defer func() { defaultReplicas = 1 }()

	defer vc.TearDown()
	piiVSchema := `{"tables": {"pii_test": {}}}`
	piiSchema := "create table pii_test(id int, name varchar(50) comment 'pii-name', email varchar(100) comment 'pii-email', "
	piiSchema += "phone varchar(20) comment 'pii-phone', gender char(1) comment 'pii-gender', ssn binary(10) comment 'pii-ssn', salary int comment 'pii-salary', "
	piiSchema += "address varbinary(100) comment 'pii-address', dob date comment 'pii-date', val1 varchar(100), val2 varbinary(100), "
	piiSchema += "primary key (id));"

	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, "pii", "0", piiVSchema, piiSchema, 0, 0, 100)
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "pii", "0"), 1)

	vtgateConn = getConnection(t, globalConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t)
	id := 1
	name := "John Smith"
	email := "john@smith.com"
	phone := "(123) 456 7890"
	gender := "M"
	ssn := "1234567890"
	address := "8 Private Drive"
	dob := "2000-01-01"
	val1 := "notprivate"
	val2 := "not a secret"
	salary := 100000
	insertTpl := "insert into pii_test(id, name, email, phone, gender, ssn, salary, address, dob, val1, val2) "
	insertTpl += "values (%d,'%s', '%s', '%s', '%s', '%s', '%d', '%s', '%s', '%s', '%s')"
	insert := fmt.Sprintf(insertTpl, id, name, email, phone, gender, ssn, salary, address, dob, val1, val2)
	execVtgateQuery(t, vtgateConn, "pii:0", insert)
	require.Empty(t, validateCount(t, vtgateConn, "pii:0", "pii_test", 1))

	vc.AddKeyspace(t, []*Cell{defaultCell}, "pii2", "0", piiVSchema, "", 0, 0, 200)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "pii2", "0"), 1)

	t.Run("Pii Redact Strategy", func(t *testing.T) {
		if err := vc.VtctlClient.ExecuteCommand("MoveTables", "-workflow=pii", "-pii_strategy=redact",
			"-tablet_types="+"master", "pii", "pii2", "pii_test"); err != nil {
			t.Fatalf("MoveTables command failed with %+v\n", err)
		}

		waitForCount(t, vtgateConn, "pii2:0", "pii_test", 1, 10)
		require.Empty(t, validateQuery(t, vtgateConn, "pii2:0",
			"select id, name, email, phone, gender, ssn, salary, address, dob, val1, val2 from pii_test",
			fmt.Sprintf(`[[INT32(%d) VARCHAR("%s") VARCHAR("%s") VARCHAR("%s") CHAR("%s") BINARY("%s") INT32(%d) VARBINARY("%s") DATE("%s") VARCHAR("%s") VARBINARY("%s")]]`,
				1, "", "", "", "", "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00", 0, "", "1970-01-01", val1, val2)))

		insert = fmt.Sprintf(insertTpl, 2, name, email, phone, gender, ssn, salary, address, dob, val1, val2)
		execVtgateQuery(t, vtgateConn, "pii:0", insert)

		waitForCount(t, vtgateConn, "pii2:0", "pii_test", 2, 10)
		require.Empty(t, validateQuery(t, vtgateConn, "pii2:0",
			"select id, name, email, phone, gender, ssn, salary, address, dob, val1, val2 from pii_test where id = 2",
			fmt.Sprintf(`[[INT32(%d) VARCHAR("%s") VARCHAR("%s") VARCHAR("%s") CHAR("%s") BINARY("%s") INT32(%d) VARBINARY("%s") DATE("%s") VARCHAR("%s") VARBINARY("%s")]]`,
				2, "", "", "", "", "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00", 0, "", "1970-01-01", val1, val2)))
		qr := execVtgateQuery(t, vtgateConn, "pii2:0", "select id, name, email, phone, gender, ssn, salary, address, dob, val1, val2 from pii_test")
		require.NotNil(t, qr)
		require.Equal(t, 2, len(qr.Rows))

	})
	t.Run("Pii Fake Strategy", func(t *testing.T) {
		vc.AddKeyspace(t, []*Cell{defaultCell}, "pii3", "0", piiVSchema, "", 0, 0, 300)
		vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "pii3", "0"), 1)

		if err := vc.VtctlClient.ExecuteCommand("MoveTables", "-workflow=pii", "-pii_strategy=fake",
			"-tablet_types="+"master", "pii", "pii3", "pii_test"); err != nil {
			t.Fatalf("MoveTables command failed with %+v\n", err)
		}

		waitForCount(t, vtgateConn, "pii3:0", "pii_test", 2, 10)

		qr := execVtgateQuery(t, vtgateConn, "pii:0", "select id, name, email, phone, gender, ssn, salary, address, dob, val1, val2 from pii_test")
		require.NotNil(t, qr)
		require.Equal(t, 2, len(qr.Rows))

		qr = execVtgateQuery(t, vtgateConn, "pii3:0", "select id, name, email, phone, gender, ssn, salary, address, dob, val1, val2 from pii_test")
		require.NotNil(t, qr)
		require.Equal(t, 2, len(qr.Rows))
		row := qr.Rows[0]

		id2, _ := row[0].ToInt64()
		name2 := row[1].ToString()
		email2 := row[2].ToString()
		phone2 := row[3].ToString()
		gender2 := row[4].ToString()
		ssn2 := row[5].ToString()
		salary2, _ := row[6].ToInt64()
		address2 := row[7].ToString()
		dob2 := row[8].ToString()
		val12 := row[9].ToString()
		val22 := row[10].ToString()
		require.Equal(t, int64(1), id2)
		require.NotEqual(t, name, name2)
		require.NotEmpty(t, name2)
		require.NotEqual(t, email, email2)
		require.NotEmpty(t, email2)
		require.NotEqual(t, phone, phone2)
		require.NotEmpty(t, phone2)
		require.NotEqual(t, gender, gender2)
		require.NotEmpty(t, gender2)
		require.NotEqual(t, ssn, ssn2)
		require.NotEmpty(t, ssn2)
		require.NotEqual(t, salary, salary2)
		require.NotEmpty(t, salary2)
		require.NotEqual(t, address, address2)
		require.NotEmpty(t, address2)
		require.NotEqual(t, dob, dob2)
		require.NotEmpty(t, dob2)
		require.Equal(t, val1, val12)
		require.Equal(t, val2, val22)
	})
}
