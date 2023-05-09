package operators

import "vitess.io/vitess/go/vt/vtgate/semantics"

type parameteriazable interface {
	AddParams(params []*Dependency)
}

func pushDownParameter(st *semantics.SemTable, node *Node, params []*Dependency) {
	if p, ok := node.Op.(parameteriazable); ok {
		p.AddParams(params)
	}

	for _, ancestor := range node.Ancestors {
		var paramsForThis []*Dependency
		for _, param := range params {
			deps := st.RecursiveDeps(param.Column.AST[0])
			if deps.IsSolvedBy(ancestor.Covers()) {
				paramsForThis = append(paramsForThis, param)
			}
		}
		pushDownParameter(st, ancestor, paramsForThis)
	}
}

func (t *TopK) AddParams(params []*Dependency) {
	for _, param := range params {
		t.Parameters = append(t.Parameters, param.Column)
	}
}
