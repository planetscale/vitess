package flownode

import (
	"reflect"
)

func (n *Node) Kind() string {
	return reflect.TypeOf(n.impl).Elem().Name()
}

func (n *Node) IsRoot() bool {
	_, ok := n.impl.(*Root)
	return ok
}

func (n *Node) IsTable() bool {
	_, ok := n.impl.(*Table)
	return ok
}

func (n *Node) AsTable() *Table {
	tbl, _ := n.impl.(*Table)
	return tbl
}

func (n *Node) IsDropped() bool {
	_, ok := n.impl.(*Dropped)
	return ok
}

func (n *Node) IsInternal() bool {
	_, ok := n.impl.(Internal)
	return ok
}

func (n *Node) AsInternal() Internal {
	internal, _ := n.impl.(Internal)
	return internal
}

func (n *Node) IsJoin() bool {
	_, ok := n.impl.(ingredientJoin)
	return ok
}

func (n *Node) CanQueryThrough() bool {
	_, ok := n.impl.(ingredientQueryThrough)
	return ok
}

func (n *Node) asQueryThrough() ingredientQueryThrough {
	qt, _ := n.impl.(ingredientQueryThrough)
	return qt
}

func (n *Node) IsShardMerger() bool {
	if sm, ok := n.impl.(interface{ IsShardMerger() bool }); ok {
		return sm.IsShardMerger()
	}
	return false
}

func (n *Node) IsSharder() bool {
	_, ok := n.impl.(*Sharder)
	return ok
}

func (n *Node) IsUnion() bool {
	_, ok := n.impl.(*Union)
	return ok
}

func (n *Node) AsSharder() *Sharder {
	sharder, _ := n.impl.(*Sharder)
	return sharder
}

func (n *Node) IsReader() bool {
	_, ok := n.impl.(*Reader)
	return ok
}

func (n *Node) AsReader() *Reader {
	reader, _ := n.impl.(*Reader)
	return reader
}

func (n *Node) IsIngress() bool {
	_, ok := n.impl.(*Ingress)
	return ok
}

func (n *Node) IsEgress() bool {
	_, ok := n.impl.(*Egress)
	return ok
}

func (n *Node) AsEgress() *Egress {
	egress, _ := n.impl.(*Egress)
	return egress
}

func (n *Node) IsSender() bool {
	switch n.impl.(type) {
	case *Egress, *Sharder:
		return true
	default:
		return false
	}
}
