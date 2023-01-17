package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/proto/vtboost"
)

// FailureModeFlag adds the pflag.Value interface to a vtboost.Science_FailureMode.
type FailureModeFlag vtboost.Science_FailureMode

var _ pflag.Value = (*FailureModeFlag)(nil)

// Set is part of the pflag.Value interface.
func (v *FailureModeFlag) Set(arg string) error {
	value, ok := vtboost.Science_FailureMode_value[strings.ToUpper(arg)]
	if !ok {
		return fmt.Errorf("invalid FailureModeFlag")
	}
	*v = FailureModeFlag(value)
	return nil
}

// String is part of the pflag.Value interface.
func (v *FailureModeFlag) String() string {
	return vtboost.Science_FailureMode(*v).String()
}

// Type is part of the pflag.Value interface.
func (v *FailureModeFlag) Type() string {
	return "cli.FailureModeFlag"
}
