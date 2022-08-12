package queryhistory

import "regexp"

type Pattern string

func (p Pattern) Match(query string) (bool, error) {
	if len(query) == 0 {
		return len(p) == 0, nil
	}
	if p[0] == '/' {
		result, err := regexp.MatchString(string(p)[1:], query)
		if err != nil {
			return false, err
		}
		return result, nil
	}
	return (string(p) == query), nil
}
