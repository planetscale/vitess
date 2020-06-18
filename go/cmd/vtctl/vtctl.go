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

package main

import (
	"flag"
	"fmt"
	"log/syslog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/cmd"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/workflow"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	waitTime     = flag.Duration("wait-time", 24*time.Hour, "time to wait on an action")
	detachedMode = flag.Bool("detach", false, "detached mode - run vtcl detached from the terminal")
)

func init() {
	logger := logutil.NewConsoleLogger()
	flag.CommandLine.SetOutput(logutil.NewLoggerWriter(logger))
	flag.Usage = func() {
		logger.Printf("Usage: %s [global parameters] command [command parameters]\n", os.Args[0])
		logger.Printf("\nThe global optional parameters are:\n")
		flag.PrintDefaults()
		logger.Printf("\nThe commands are listed below, sorted by group. Use '%s <command> -h' for more help.\n\n", os.Args[0])
		vtctl.PrintAllCommands(logger)
	}
}

// signal handling, centralized here
func installSignalHandlers(cancel func()) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigChan
		// we got a signal, cancel the current ctx
		cancel()
	}()
}

var rootCmd *cobra.Command

var cobraCommands = []string{"version", "bash", "vexec", "workflow"}

func addCobraCommands() {
	addUtilCommands()
	addCommands()
}

func addUtilCommands() {
	servenv.ParseFlagsWithArgs("vtctld")
	rootCmd = &cobra.Command{
		Use:   "vtctld",
		Short: "Vitess Control Plane CLI",
		Long:  `vtctld is a cli providing access to various vitess functionality to manage vitess processes and objects`,
	}
	rootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	rootCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		rootCmd.PersistentFlags().MarkHidden(f.Name)
	})
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print the version number of vtctld",
		Run: func(cmd *cobra.Command, args []string) {
			prompt := promptui.Prompt{
				Label:     "Are you sure you want to run this?",
				Default:   "n",
				IsConfirm: true,
			}
			result, err := prompt.Run()
			if err != nil {
				log.Infof("Unable to throw prompt %s", "version")
			}
			if result == "y" {
				fmt.Println("vtctld v6a")
			}
		},
	}
	rootCmd.AddCommand(versionCmd)

	bashCompletionCmd := &cobra.Command{
		Use:   "bash [--output output_file]",
		Short: "Generate bash completion for vtctld",
		Run: func(cmd *cobra.Command, args []string) {
			var fp *os.File
			var err error
			fname, _ := cmd.PersistentFlags().GetString("output")
			if fname != "" {
				if fp, err = os.OpenFile(fname, os.O_CREATE | os.O_RDWR, 0666); err != nil {
					fmt.Errorf("unable to open file", fname)
					return
				}
			}
			if fp == nil {
				fp = os.Stdout
			}
			fmt.Printf("Writing bash completions to file: %s\n", fp.Name())
			rootCmd.GenBashCompletion(fp)
		},
	}
	bashCompletionCmd.PersistentFlags().StringP("output", "o", "", "output file to store bash completion commands")

	rootCmd.AddCommand(bashCompletionCmd)
}

func runLegacyCommand(args []string) {
	action := args[0]
	ts := topo.Open()
	defer ts.Close()

	vtctl.WorkflowManager = workflow.NewManager(ts)

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	installSignalHandlers(cancel)

	err := vtctl.RunCommand(ctx, wr, args)
	cancel()
	switch err {
	case vtctl.ErrUnknownCommand:
		flag.Usage()
		exit.Return(1)
	case nil:
		// keep going
	default:
		log.Errorf("action failed: %v %v", action, err)
		exit.Return(255)
	}
}

func addLegacyCommands() {
	for _, group := range vtctl.Commands() {
		for _, cmd := range group.Commands {
			action := cmd.Name
			use := fmt.Sprintf("%s %s", action, strings.ReplaceAll(cmd.Params, "[-", "[--"))
			method := cmd.Method
			cobraCmd := &cobra.Command{
				Use:     use,
				Short:   cmd.Help,
				Long:    cmd.Help,

				Run: func(cobraCmd *cobra.Command, args []string) {
					ts := topo.Open()
					defer ts.Close()

					vtctl.WorkflowManager = workflow.NewManager(ts)
					ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
					wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
					subFlags := flag.NewFlagSet(action, flag.ContinueOnError)
					subFlags.SetOutput(logutil.NewLoggerWriter(wr.Logger()))
					installSignalHandlers(cancel)
					if err := method(ctx, wr, subFlags, args[:1]); err != nil {
						log.Errorf(err.Error())
					}
				},
			}
			if strings.Contains(use, "DEPRECATED") {
				cobraCmd.Deprecated = " please look at the documentation for new ways to do this"
				cobraCmd.Use = strings.ReplaceAll(use, "DEPRECATED ", "")
				cobraCmd.Run = func(cobraCmd *cobra.Command, args []string) {}
			}
			rootCmd.AddCommand(cobraCmd)
		}
	}
}

func main() {

	defer exit.RecoverAll()
	defer logutil.Flush()

	if *detachedMode {
		// this method will call os.Exit and kill this process
		cmd.DetachFromTerminalAndExit()
	}

	addCobraCommands()
	addLegacyCommands()

	startMsg := fmt.Sprintf("USER=%v SUDO_USER=%v %v", os.Getenv("USER"), os.Getenv("SUDO_USER"), strings.Join(os.Args, " "))

	if syslogger, err := syslog.New(syslog.LOG_INFO, "vtctl "); err == nil {
		syslogger.Info(startMsg)
	} else {
		log.Warningf("cannot connect to syslog: %v", err)
	}

	closer := trace.StartTracing("vtctl")
	defer trace.LogErrorsWhenClosing(closer)

	servenv.FireRunHooks()
	args := servenv.ParseFlagsWithArgs("vtctl")
	action := args[0]
	isCobraCommand := false
	for _, cmd := range cobraCommands {
		if cmd == action {
			isCobraCommand = true
			break
		}
	}
	if isCobraCommand {
		rootCmd.Execute()
	} else {
		runLegacyCommand(args)
	}
}
