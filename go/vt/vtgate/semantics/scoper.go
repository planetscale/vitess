/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package semantics

import "vitess.io/vitess/go/vt/sqlparser"

type scoper struct {
	rScope map[*sqlparser.Select]*scope
	wScope map[*sqlparser.Select]*scope
	scopes []*scope
}

func newScoper() *scoper {
	return &scoper{
		rScope: map[*sqlparser.Select]*scope{},
		wScope: map[*sqlparser.Select]*scope{},
	}
}

func (s *scoper) down(cursor *sqlparser.Cursor) {
	switch node := cursor.Node().(type) {
	case *sqlparser.Select:
		currScope := newScope(s.currentScope())
		s.push(currScope)

		// Needed for order by with Literal to find the Expression.
		currScope.selectExprs = node.SelectExprs

		s.rScope[node] = currScope
		s.wScope[node] = newScope(nil)
	case sqlparser.TableExpr:
		if isParentSelect(cursor) {
			s.push(newScope(nil))
		}
	case sqlparser.SelectExprs:
		sel, parentIsSelect := cursor.Parent().(*sqlparser.Select)
		if !parentIsSelect {
			break
		}

		wScope, exists := s.wScope[sel]
		if !exists {
			break
		}

		wScope.tables = append(wScope.tables, createVTableInfoForExpressions(node))
	case sqlparser.OrderBy, sqlparser.GroupBy:
		s.changeScopeForOrderBy(cursor)
	case *sqlparser.Union:
		s.push(newScope(s.currentScope()))
	}
}

func (s *scoper) changeScopeForOrderBy(cursor *sqlparser.Cursor) {
	sel, ok := cursor.Parent().(*sqlparser.Select)
	if !ok {
		return
	}
	// In ORDER BY, we can see both the scope in the FROM part of the query, and the SELECT columns created
	// so before walking the rest of the tree, we change the scope to match this behaviour
	incomingScope := s.currentScope()
	nScope := newScope(incomingScope)
	s.push(nScope)
	wScope := s.wScope[sel]
	nScope.tables = append(nScope.tables, wScope.tables...)
	nScope.selectExprs = incomingScope.selectExprs

	if s.rScope[sel] != incomingScope {
		panic("BUG: scope counts did not match")
	}
}

func (s *scoper) currentScope() *scope {
	size := len(s.scopes)
	if size == 0 {
		return nil
	}
	return s.scopes[size-1]
}

func (s *scoper) push(sc *scope) {
	s.scopes = append(s.scopes, sc)
}

func (s *scoper) popScope() {
	l := len(s.scopes) - 1
	s.scopes = s.scopes[:l]
}
