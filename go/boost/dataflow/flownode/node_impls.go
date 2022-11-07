package flownode

import (
	"reflect"

	"vitess.io/vitess/go/boost/boostpb"
)

func (n *Node) Kind() string {
	return reflect.TypeOf(n.impl).Elem().Name()
}

func (n *Node) IsSource() bool {
	_, ok := n.impl.(*Source)
	return ok
}

func (n *Node) AsSource() *Source {
	src, _ := n.impl.(*Source)
	return src
}

type AnyBase interface {
	NodeImpl
	Schema() []boostpb.Type
}

func (n *Node) IsAnyBase() bool {
	_, ok := n.impl.(AnyBase)
	return ok
}

func (n *Node) IsBase() bool {
	_, ok := n.impl.(*Base)
	return ok
}

func (n *Node) AsBase() *Base {
	base, _ := n.impl.(*Base)
	return base
}

func (n *Node) IsExternalBase() bool {
	_, ok := n.impl.(*ExternalBase)
	return ok
}

func (n *Node) AsExternalBase() *ExternalBase {
	base, _ := n.impl.(*ExternalBase)
	return base
}

func (n *Node) IsDropped() bool {
	_, ok := n.impl.(*Dropped)
	return ok
}

func (n *Node) AsDropped() *Dropped {
	dropped, _ := n.impl.(*Dropped)
	return dropped
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
	_, ok := n.impl.(JoinIngredient)
	return ok
}

func (n *Node) RequiresFullMaterialization() bool {
	switch n.impl.(type) {
	case *Distinct:
		return true
	// TODO: Trigger would also require full materialization if we choose to support it
	default:
		return false
	}
}

func (n *Node) CanQueryThrough() bool {
	_, ok := n.impl.(IngredientQueryThrough)
	return ok
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
