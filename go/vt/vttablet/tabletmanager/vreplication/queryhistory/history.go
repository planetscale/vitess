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

func (h History) Validate(expectations []Expectation) (bool, error) {
	// For each query in history...
	for i := range h {
		// Validate query.
		ok, err := h.ValidateAt(expectations, i)
		if !ok {
			return false, err
		}
	}

	// All queries met expectations.
	return true, nil
}

func (h History) ValidateAt(expectations []Expectation, index int) (bool, error) {
	q, err := h.At(index)
	if err != nil {
		return false, err
	}

	// For each expectation...
	for _, e := range expectations {
		// Does the history query match the expectation query?
		match, err := e.Query.Match(q)

		// If there was a matching error, return it.
		if err != nil {
			return false, fmt.Errorf("unexpected error matching query %s to expectation: %s", q, err.Error())
		}

		// If no query match, continue to next expectation.
		if !match {
			continue
		}

		// If we fail the rule, return an error.
		if ok, err := e.Ruleset.Check(h, index); !ok {
			return false, fmt.Errorf("query %s failed a rule: %s", q, err.Error())
		}

		// If we pass the rule, return true.
		// Only need to match one expectation.
		return true, nil
	}

	return false, fmt.Errorf("query did not match any expectations")
}

func (h History) String() string {
	return strings.Join(h, "\n")
}
