package vreplication

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

func TestFailedTrafficSwitches(t *testing.T) {
	allCellNames = "zone1"
	defaultCellName := "zone1"
	workflow := "wwf"
	reverse_workflow := "wwf_reverse"
	sourceKs := "product"
	targetKs := "customer"
	shard := "0"
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
	ksReverseWorkflow := fmt.Sprintf("%s.%s_reverse", sourceKs, workflow)
	_ = ksReverseWorkflow
	vc = NewVitessCluster(t, t.Name(), []string{"zone1"}, mainClusterConfig)
	require.NotNil(t, vc)
	defaultCell = vc.Cells[defaultCellName]
	cells := []*Cell{defaultCell}

	defer vc.TearDown(t)

	cell1 := vc.Cells["zone1"]
	vc.AddKeyspace(t, []*Cell{cell1}, sourceKs, "0", initialProductVSchema, initialProductSchema, 0, 0, 100, sourceKsOpts)

	vtgate = cell1.Vtgates[0]
	require.NotNil(t, vtgate)
	err := cluster.WaitForHealthyShard(vc.VtctldClient, sourceKs, shard)
	require.NoError(t, err)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	productTab := vc.Cells[defaultCell.Name].Keyspaces[sourceKs].Shards["0"].Tablets["zone1-100"].Vttablet
	_ = productTab
	insertInitialData(t)

	if _, err := vc.AddKeyspace(t, cells, targetKs, "0", customerVSchema, customerSchema, 0, 0, 200, targetKsOpts); err != nil {
		t.Fatal(err)
	}
	err = cluster.WaitForHealthyShard(vc.VtctldClient, targetKs, shard)
	require.NoError(t, err)

	defaultCell := vc.Cells["zone1"]
	custKs := vc.Cells[defaultCell.Name].Keyspaces[targetKs]
	customerTab := custKs.Shards["0"].Tablets["zone1-200"].Vttablet

	tables := "customer"
	var output string

	output, err = vc.VtctlClient.ExecuteCommandWithOutput("MoveTables", "--", "--source", sourceKs, "--tables", tables, "Create", ksWorkflow)
	require.NoError(t, err, output)

	catchup(t, customerTab, workflow, "MoveTables")
	runVDiffsSideBySide = false
	vdiffSideBySide(t, ksWorkflow, "")

	execVtgateQuery(t, vtgateConn, "customer", "select * from customer")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// successfully switch traffic
	//go lockTable(t, ctx, cancel, productTab, sourceKs, "customer", 35)
	//time.Sleep(1 * time.Second)
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("MoveTables", "--", "SwitchTraffic", ksWorkflow)
	log.Infof("Completed MoveTables SwitchTraffic, with err and output: %v, %s", err, output)
	//require.NoError(t, err, output)

	printStatus(t, "After SwitchTraffic")
	return
	execVtgateQuery(t, vtgateConn, "customer", "update customer set name = 'new name' where cid = 1")
	catchup(t, productTab, reverse_workflow, "MoveTables")

	// failed reverse traffic

	go lockTable(t, ctx, cancel, customerTab, targetKs, "customer", 35)
	time.Sleep(1 * time.Second)
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("MoveTables", "--action_timeout", "10s", "--", "ReverseTraffic", ksWorkflow)
	log.Infof("Completed MoveTables ReverseTraffic, with err and output: %v, %s", err, output)
	require.Error(t, err, output)
	printStatus(t, "After First Failed ReverseTraffic")
	//execVtgateQuery(t, vtgateConn, "customer", "select * from customer")

	done := false
	for !done {
		select {
		case <-ctx.Done():
			log.Infof("Context Done")
			done = true
		}
	}
	return

	// second failed reverse traffic
	go lockTable(t, ctx, cancel, customerTab, targetKs, "customer", 35)
	time.Sleep(1 * time.Second)
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("MoveTables", "--action_timeout", "10s", "--", "ReverseTraffic", ksWorkflow)
	log.Infof("Completed MoveTables ReverseTraffic, with err and output: %v, %s", err, output)
	require.Error(t, err, output)
	printStatus(t, "After Second Failed ReverseTraffic")
	//execVtgateQuery(t, vtgateConn, "customer", "select * from customer")

}

func printStatus(t *testing.T, scope string) {
	ksw := "customer.wwf"
	ksrw := "product.wwf_reverse"
	scope = fmt.Sprintf("%s: ", scope)
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("MoveTables", "--", "GetState", ksw)
	log.Infof(scope+"Completed MoveTables GetState for %s, with err and output: %v, %s", ksw, err, output)

	output, err = vc.VtctlClient.ExecuteCommandWithOutput("Workflow", ksw, "show")
	log.Infof(scope+"After SwitchTraffic Workflow show for %s, with err and output: %v, %s", ksw, err, output)

	output, err = vc.VtctlClient.ExecuteCommandWithOutput("Workflow", ksrw, "show")
	log.Infof(scope+"After SwitchTraffic Workflow show for %s, with err and output: %v, %s", ksrw, err, output)
}

func lockTable(t *testing.T, ctx context.Context, cancel context.CancelFunc, tab *cluster.VttabletProcess, dbname, tableName string, timeoutSeconds int) {
	conn, err := tab.GetConnection(fmt.Sprintf("vt_%s", dbname))
	if err != nil {
		t.Fatalf("failed to get connection for tablet %d: %v", tab.TabletUID, err)
	}
	defer conn.Close()
	query := fmt.Sprintf("LOCK TABLES %s WRITE", tableName)
	log.Infof("running query %s", query)
	_, err = conn.ExecuteFetch(query, -1, false)
	if err != nil {
		t.Fatalf("failed to run query %s: %v", query, err)
	}
	log.Infof("sleeping for %d seconds", timeoutSeconds)
	time.Sleep(time.Duration(timeoutSeconds) * time.Second)
	query = "UNLOCK TABLES"
	log.Infof("running query %s", query)
	_, err = conn.ExecuteFetch(query, -1, false)
	if err != nil {
		t.Fatalf("failed to run query %s: %v", query, err)
	}
	cancel()
}
