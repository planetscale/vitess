package vtgate

import (
	"vitess.io/vitess/go/vt/sysvars"
)

// boostEnabled implements the Boost check on Safe Session
func (session *SafeSession) boostEnabled() bool {
	if session == nil {
		return false
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	return session.SystemVariables[sysvars.BoostCachedQueries.Name] == "1"
}
