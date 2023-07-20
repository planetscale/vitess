package command

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/proto/vtboost"
)

var (
	// BoostListClusters lists all of the Boost clusters in topo.
	BoostListClusters = &cobra.Command{
		Use:   "BoostListClusters",
		Short: "List all of the Boost clusters in topo.",
		Long: `List all of the Boost clusters in topo.
A raw JSON dump of the state for each cluster is returned.
`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(0),
		RunE:                  commandBoostListClusters,
	}

	// BoostRemoveCluster removes one or more Boost clusters from topo.
	BoostRemoveCluster = &cobra.Command{
		Use:   "BoostRemoveCluster --clusters <c1,c2,...>",
		Short: "Remove one or more Boost clusters from topo.",
		Long: `Remove one or more Boost clusters from topo.
Normally, the lifecycle of a Boost cluster is managed entirely by psdb-operator. However, in the event of a failure or unexpected
behavior, it can be useful to manually remove a Boost cluster from topo.
`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(0),
		RunE:                  commandBoostRemoveCluster,
	}

	// BoostSetScience sets the science settings in the boost cluster.
	BoostSetScience = &cobra.Command{
		Use:   "BoostSetScience --sample-rate <rate> --failure-mode <mode> [--clusters <c1,c2,...>]",
		Short: "Sets the science for Boost.",
		Long: `You can change the science of Boost using this command.
Science allows you to compare Boost results with Gen4 results.
The sample rate defines the % of tested queries. The failure mode dictates what to do in the event of a mismatch.
You can also specify the list of clusters on which you want to set the Science. An empty list of clusters means all clusters.
`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(0),
		RunE:                  commandBoostSetScience,
	}
)

func commandBoostListClusters(cmd *cobra.Command, _ []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.BoostListClusters(commandCtx, &vtboost.ListClustersRequest{})
	if err != nil {
		return err
	}

	p, err := cli.MarshalJSON(resp.Clusters)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", p)

	return nil
}

var boostRemoveClusterOptions = struct {
	clusters []string
}{}

func commandBoostRemoveCluster(cmd *cobra.Command, _ []string) error {
	cli.FinishedParsing(cmd)

	req := &vtboost.RemoveClusterRequest{
		Uuids: boostRemoveClusterOptions.clusters,
	}

	if _, err := client.BoostRemoveCluster(commandCtx, req); err != nil {
		return err
	}

	return nil
}

var boostSetScienceOptions = struct {
	vtboost.Science
	failureMode cli.FailureModeFlag
	clusters    []string
}{
	failureMode: cli.FailureModeFlag(vtboost.Science_LOG),
}

func commandBoostSetScience(cmd *cobra.Command, _ []string) error {
	switch vtboost.Science_FailureMode(boostSetScienceOptions.failureMode) {
	case vtboost.Science_ERROR, vtboost.Science_LOG:
		boostSetScienceOptions.FailureMode = vtboost.Science_FailureMode(boostSetScienceOptions.failureMode)
	default:
		return fmt.Errorf("invalid failure mode passed to --failure-mode: %v", boostSetScienceOptions.failureMode)
	}

	if boostSetScienceOptions.ComparisonSampleRate < 0.00 || boostSetScienceOptions.ComparisonSampleRate > 1.00 {
		return fmt.Errorf("invalid sample rate passed to --sample-rate, the value should be between >= 0.00 and <= 1.00, got: %f", boostSetScienceOptions.ComparisonSampleRate)
	}

	cli.FinishedParsing(cmd)

	req := &vtboost.SetScienceRequest{
		Science:  &boostSetScienceOptions.Science,
		Clusters: boostSetScienceOptions.clusters,
	}

	_, err := client.BoostSetScience(commandCtx, req)
	if err != nil {
		return err
	}

	return nil
}

func init() {
	Root.AddCommand(BoostListClusters)

	BoostRemoveCluster.Flags().StringSliceVarP(&boostRemoveClusterOptions.clusters, "clusters", "c", []string{}, "List of clusters to be removed from topo.")
	BoostRemoveCluster.MarkFlagRequired("clusters")
	Root.AddCommand(BoostRemoveCluster)

	BoostSetScience.Flags().Float64VarP(&boostSetScienceOptions.ComparisonSampleRate, "sample-rate", "s", 0.00, "The sample rate used to tell if a query will be tested or not, should be between 0.00 and 1.00.")
	BoostSetScience.Flags().VarP(&boostSetScienceOptions.failureMode, "failure-mode", "m", "The failure mode used by VTGate when there is a mismatch between Gen4 and Boost.")
	BoostSetScience.Flags().StringSliceVarP(&boostSetScienceOptions.clusters, "clusters", "c", []string{}, "List of clusters on which we want to set the science. An empty value means all clusters.")
	BoostSetScience.MarkFlagRequired("sample-rate")
	BoostSetScience.MarkFlagRequired("failure-mode")
	Root.AddCommand(BoostSetScience)
}
