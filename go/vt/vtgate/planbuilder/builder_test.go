package planbuilder

import (
	"testing"
)

func TestGetConfiguredPlanner(t *testing.T) {
	got, err := getConfiguredPlanner(tt.args.vschema, tt.args.v3planner, tt.args.stmt)
}
