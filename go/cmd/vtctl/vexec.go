package main

import (
	"fmt"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtctl"
)

func getWorkflowFlags(cmd *cobra.Command) (string, string, string, string) {
	workflow, _ := cmd.Parent().PersistentFlags().GetString("workflow")
	keyspace, _ := cmd.Parent().PersistentFlags().GetString("keyspace")
	shard, _ := cmd.Parent().PersistentFlags().GetString("shard")
	tablet, _ := cmd.Parent().PersistentFlags().GetString("tablet")

	return workflow, keyspace, shard, tablet
}

func runVexecCommand(cmd *cobra.Command, args []string) {
	args = servenv.ParseFlagsWithArgs("vtctl")
	query := args[1]
	workflow, keyspace, shard, tablet := getWorkflowFlags(cmd)

}

func confirmTabletOperation(cr *vtctl.CommandRunner, msg string, listTablets bool) bool {
	msg := fmt.Sprintf("You are about to run '%s' on %d tablets, are you sure?", action, len(vx.Masters))

}

func runWorkflowCommand(cmd *cobra.Command, args []string) { //TODO: args is empty, check why

	args = servenv.ParseFlagsWithArgs("vtctl") //fixme, should not hardcode vtctl ...
	action := args[1]
	workflow, keyspace, shard, tablet := getWorkflowFlags(cmd)

	cr, err := vtctl.NewCommandRunner(workflow, keyspace, shard, tablet)
	if err != nil {
		fmt.Println(err.Error())
		return
	}


	if confirmTabletOperation(cr, msg, true) {

	}
	if !confirm(msg) {
		fmt.Println("Aborting ...")
		return
	}
	switch action {
	case "StopStreams":
		if err := cr.StopStreams(ctx); err != nil {
			fmt.Printf(err.Error())
		}
	case "StartStreams":
		if err := cr.StartStreams(ctx); err != nil {
			fmt.Printf(err.Error())
		}
	case "ListStreams":
		if err := cr.ListStreams(ctx); err != nil {
			fmt.Printf(err.Error())
		}
	default:
		fmt.Printf("Invalid action found: %s", action)

	}
}

func confirm(msg string) bool {
	prompt := promptui.Prompt{
		Label:     msg,
		Default:   "n",
		IsConfirm: true,
	}
	result, err := prompt.Run()
	if err != nil {
		fmt.Errorf(err.Error())
		return false
	}
	if result == "y" {
		return true
	}
	return false
}

func addWorkflowFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringP("workflow", "", "", "name of vreplication workflow")
	cmd.MarkPersistentFlagRequired("workflow")
	cmd.PersistentFlags().StringP("keyspace", "", "", "name of target keyspace")
	cmd.MarkPersistentFlagRequired("keyspace")
	cmd.PersistentFlags().StringP("shard", "", "", "name of shard in specified keyspace")
	cmd.PersistentFlags().StringP("tablet", "", "", "master tablet alias")
}

func addCommands() {
	vexecCmd := &cobra.Command{
		Use:   "VExec",
		Short: "execute a sql query on tables in the _vt database on all matching tablets",
		Run:   runVexecCommand
	}
	addWorkflowFlags(vexecCmd)
	rootCmd.AddCommand(vexecCmd)

	workflowCmd := &cobra.Command{
		Use:   "Workflow",
		Short: "commands to operate on a workflow",
	}
	addWorkflowFlags(workflowCmd)
	rootCmd.AddCommand(workflowCmd)

	workflowStopStreamsCmd := &cobra.Command{
		Use:   "StopStreams",
		Short: "Set status of all streams in given workflow in matching tablets to Stopped",
		Run:   runWorkflowCommand,
	}
	workflowCmd.AddCommand(workflowStopStreamsCmd)

	workflowStartStreamsCmd := &cobra.Command{
		Use:   "StartStreams",
		Short: "Set status of all streams in a workflow in matching tablets to Running",
		Run:   runWorkflowCommand,
	}
	workflowCmd.AddCommand(workflowStartStreamsCmd)

	workflowListStreamsCmd := &cobra.Command{
		Use:   "start",
		Short: "List all streams in a workflow in matching tablets",
		Run:   runWorkflowCommand,
	}
	workflowCmd.AddCommand(workflowListStreamsCmd)

}
