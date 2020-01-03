package main

import (
	"math/rand"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/servenv"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	servenv.RegisterDefaultFlags()
}

func TestVtctl(t *testing.T) {
	main()
}
