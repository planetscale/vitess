/*
Copyright 2020 The Vitess Authors.

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

package main

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	findAllShardsInKeyspaceCmd = &cobra.Command{
		Use:     "FindAllShardsInKeyspace keyspace",
		Aliases: []string{"findallshardsinkeyspace"},
		Args:    cobra.ExactArgs(1),
		RunE:    commandFindAllShardsInKeyspace,
	}
	getKeyspaceCmd = &cobra.Command{
		Use:     "GetKeyspace keyspace",
		Aliases: []string{"getkeyspace"},
		Args:    cobra.ExactArgs(1),
		RunE:    commandGetKeyspace,
	}
	getKeyspacesCmd = &cobra.Command{
		Use:     "GetKeyspaces",
		Aliases: []string{"getkeyspaces"},
		Args:    cobra.NoArgs,
		RunE:    commandGetKeyspaces,
	}
)

func commandFindAllShardsInKeyspace(cmd *cobra.Command, args []string) error {
	ks := cmd.Flags().Arg(0)
	resp, err := client.FindAllShardsInKeyspace(commandCtx, &vtctldatapb.FindAllShardsInKeyspaceRequest{
		Keyspace: ks,
	})

	if err != nil {
		return err
	}

	data, err := json.Marshal(&resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)
	return nil
}

func commandGetKeyspace(cmd *cobra.Command, args []string) error {
	ks := cmd.Flags().Arg(0)
	resp, err := client.GetKeyspace(commandCtx, &vtctldatapb.GetKeyspaceRequest{
		Keyspace: ks,
	})

	if err != nil {
		return err
	}

	fmt.Printf("%+v\n", resp.Keyspace)

	return nil
}

func commandGetKeyspaces(cmd *cobra.Command, args []string) error {
	resp, err := client.GetKeyspaces(commandCtx, &vtctldatapb.GetKeyspacesRequest{})
	if err != nil {
		return err
	}

	fmt.Printf("%+v\n", resp.Keyspaces)

	return nil
}

func init() {
	rootCmd.AddCommand(findAllShardsInKeyspaceCmd)
	rootCmd.AddCommand(getKeyspaceCmd)
	rootCmd.AddCommand(getKeyspacesCmd)
}
