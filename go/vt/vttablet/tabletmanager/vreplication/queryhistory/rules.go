package queryhistory

import "fmt"

type Rule interface {
	Check(history History, index int) (bool, error)
}

type Ruleset []Rule

type occursAfter struct {
	pattern Pattern
	strict  bool
}

type occursFirst struct{}

func OccursAfter(pattern Pattern, strict bool) Rule {
	return &occursAfter{
		pattern,
		strict,
	}
}

func OccursFirst() Rule {
	return &occursFirst{}
}

func (rs Ruleset) Check(history History, index int) (bool, error) {
	for _, r := range rs {
		if ok, err := r.Check(history, index); !ok {
			return ok, err
		}
	}
	return true, nil
}

func (r *occursAfter) Check(history History, index int) (bool, error) {
	if index == 0 {
		return false, fmt.Errorf("expected query to come after a query matching pattern %s", r.pattern)
	}

	for i := index - 1; i >= 0; i-- {
		query, _ := history.At(i)
		match, err := r.pattern.Match(query)
		if err != nil {
			return false, fmt.Errorf("unexpected query pattern match error: %s", err.Error())
		}
		if match {
			return true, nil
		}
		if i == index-1 && r.strict {
			return false, fmt.Errorf("expected query to come immediately after a query matching pattern %s, but instead found %s", r.pattern, query)
		}
	}

	return false, fmt.Errorf("expected query to come after a query matching pattern %s", r.pattern)
}

func (r *occursFirst) Check(history History, index int) (bool, error) {
	if index != 0 {
		return false, fmt.Errorf("expected to query to occur first in history")
	}
	return true, nil
}
