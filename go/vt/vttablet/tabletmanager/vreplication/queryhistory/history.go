package queryhistory

import (
	"fmt"
	"strings"
)

type History []string

func (h History) At(index int) (string, error) {
	if len(h) <= index || index < 0 {
		return "", fmt.Errorf("index out of range: %d", index)
	}

	return h[index], nil
}

func (h History) String() string {
	return strings.Join(h, "\n")
}
