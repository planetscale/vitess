package vreplication

import (
	"fmt"
	"testing"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/wrangler"
)

var lastId int64

func insertSomeData(t *testing.T, keyspace string, numRows int64) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	idx := lastId
	for i := idx + 1; i <= idx+numRows; i++ {
		execQueryWithRetry(t, vtgateConn,
			fmt.Sprintf("insert into %s.customer(cid, name) values(%d, 'name%d')", keyspace, i, i), queryTimeout)
	}
	lastId += numRows
}

func TestMoveTablesAll(t *testing.T) {
	defaultRdonly = 1
	vc = setupMinimalCluster(t)
	defer vc.TearDown()
	sourceKeyspace := "product"
	targetKeyspace := "customer"
	workflowName := "p2c"
	createFlags := []string{"--all-tables", "--exclude-tables", "customer_seq,customer_seq2, order_seq"}
	tables := "customer"
	currentWorkflowType = wrangler.MoveTablesWorkflow
	setupMinimalCustomerKeyspace(t)
	mt := createMoveTables(t, sourceKeyspace, targetKeyspace, workflowName, tables, nil, nil, nil)
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
	insertSomeData(t, sourceKeyspace, 10)
	vdiff(t, targetKeyspace, workflowName, "zone1", false, true, nil)
	mt.SwitchReadsAndWrites()
	mt.Complete()

	sourceKeyspace = "customer"
	targetKeyspace = "all"
	workflowName = "c2a"
	createFlags = []string{"--all-tables"}
	if _, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, "all", "-80,80-",
		customerVSchema, customerSchema, 0, 0, 400, nil); err != nil {
		t.Fatal(err)
	}
	mt = createMoveTables(t, sourceKeyspace, targetKeyspace, workflowName, "", createFlags, nil, nil)
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
	insertSomeData(t, sourceKeyspace, 10)
	vdiff(t, targetKeyspace, workflowName, "zone1", false, true, nil)
	mt.SwitchReadsAndWrites()
	mt.Complete()

}
