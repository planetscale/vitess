package operators

import (
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type NodeReuser struct {
	cache map[Hash][]*Node
}

func (r *NodeReuser) Visit(st *semantics.SemTable, node *Node) {
outer:
	for idx, ancestor := range node.Ancestors {
		if !canBeReused(ancestor) {
			continue
		}

		h := ancestor.Signature().Hash()
		cachedNodes := r.cache[h]
		for _, cached := range cachedNodes {
			if ancestor.Equals(st, cached) {
				node.Ancestors[idx] = cached
				continue outer
			}
		}
		r.cache[h] = append(r.cache[h], ancestor)
		r.Visit(st, ancestor)
	}
}

func canBeReused(node *Node) bool {
	switch node.Op.(type) {
	case *Table, *NodeTableRef:
		return false
	}

	return true
}
