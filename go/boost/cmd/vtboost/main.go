package main

import (
	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/cmd/vtboost/cli"
)

func main() {
	if err := cli.Main.Execute(); err != nil {
		cli.Log().Fatal("", zap.Error(err))
	}
}
