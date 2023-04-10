package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parseVttabletVersion(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		version *VitessImageVersion
		err     string
	}{
		{
			name: "empty version output",
			err:  "vttablet --version output is empty",
		},

		{
			name:   "partial output",
			output: `Version: 15.0.1-SNAPSHOT (Git revision  branch '') built on  by @ using go1.20.1 darwin/amd64`,
			version: &VitessImageVersion{
				CommitSha:  "",
				CommitDate: "",
				Major:      "15",
				Minor:      "0",
				Patch:      "1",
			},
		},

		{
			name:   "full output",
			output: `Version: 15.2.3 (Git revision d54b87ca0be09b678bb4490060e8f23f890ddb92 branch 'main') built on Fri Oct 28 03:30:46 UTC 2022 by brew@Monterey using go1.18.7 darwin/amd64`,
			version: &VitessImageVersion{
				CommitSha:  "d54b87ca0be09b678bb4490060e8f23f890ddb92",
				CommitDate: "Fri Oct 28 03:30:46 UTC 2022",
				Major:      "15",
				Minor:      "2",
				Patch:      "3",
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			imageVersion, err := parseVersion(tt.output)

			if tt.err != "" {
				require.EqualError(t, err, tt.err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, imageVersion, tt.version)
		})
	}
}
