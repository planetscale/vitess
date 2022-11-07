package common

import (
	"fmt"
	"strconv"
	"strings"
)

type ColumnSet string

func NewColumnSet(cols []int) ColumnSet {
	ss := make([]string, 0, len(cols))
	for _, c := range cols {
		ss = append(ss, strconv.Itoa(c))
	}

	return ColumnSet(strings.Join(ss, ","))
}

func (c ColumnSet) ToSlice() (out []int) {
	for _, s := range strings.Split(string(c), ",") {
		v, err := strconv.Atoi(s)
		if err != nil {
			panicf("failed to parse: %v", err)
		}

		out = append(out, v)
	}

	return out
}

func panicf(format string, a ...interface{}) {
	panic(fmt.Sprintf(format, a...))
}
