package vstreamer

import (
	"testing"

	"vitess.io/vitess/go/vt/log"
)

func TestPool(t *testing.T) {
	TestFilteredInt(t)
	TestFilteredInt(t)

	log.Infof("events streamed %v", engine.vstreamerEventsStreamed.Get())
}
