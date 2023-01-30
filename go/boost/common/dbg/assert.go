package dbg

import "fmt"

func Assert(assertion bool, message string, args ...any) {
	if !assertion {
		panic(fmt.Errorf(message, args...))
	}
}

func Bug(message string, args ...any) {
	panic(fmt.Errorf(message, args...))
}
