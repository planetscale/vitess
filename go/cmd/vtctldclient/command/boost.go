package command

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/proto/vtboost"
)

var (
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

var scienceOptions = struct {
	vtboost.Science
	failureMode cli.FailureModeFlag
	clusters    []string
}{
	failureMode: cli.FailureModeFlag(vtboost.Science_LOG),
}

func commandBoostSetScience(cmd *cobra.Command, args []string) error {
	switch vtboost.Science_FailureMode(scienceOptions.failureMode) {
	case vtboost.Science_ERROR, vtboost.Science_LOG:
		scienceOptions.FailureMode = vtboost.Science_FailureMode(scienceOptions.failureMode)
	default:
		return fmt.Errorf("invalid failure mode passed to --failure-mode: %v", scienceOptions.failureMode)
	}

	if scienceOptions.ComparisonSampleRate < 0.00 || scienceOptions.ComparisonSampleRate > 1.00 {
		return fmt.Errorf("invalid sample rate passed to --sample-rate, the value should be between >= 0.00 and <= 1.00, got: %f", scienceOptions.ComparisonSampleRate)
	}

	cli.FinishedParsing(cmd)

	req := &vtboost.SetScienceRequest{
		Science:  &scienceOptions.Science,
		Clusters: scienceOptions.clusters,
	}

	_, err := client.BoostSetScience(commandCtx, req)
	if err != nil {
		return err
	}

	return nil
}

func init() {
	BoostSetScience.Flags().Float64VarP(&scienceOptions.ComparisonSampleRate, "sample-rate", "s", 0.00, "The sample rate used to tell if a query will be tested or not, should be between 0.00 and 1.00.")
	BoostSetScience.Flags().VarP(&scienceOptions.failureMode, "failure-mode", "m", "The failure mode used by VTGate when there is a mismatch between Gen4 and Boost.")
	BoostSetScience.Flags().StringSliceVarP(&scienceOptions.clusters, "clusters", "c", []string{}, "List of clusters on which we want to set the science. An empty value means all clusters.")
	BoostSetScience.MarkFlagRequired("sample-rate")
	BoostSetScience.MarkFlagRequired("failure-mode")
	Root.AddCommand(BoostSetScience)
}
