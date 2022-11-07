package flownode

import (
	"fmt"

	"vitess.io/vitess/go/boost/boostpb"
)

func (n *Node) ToProto() *boostpb.Node {
	pnode := &boostpb.Node{
		Name:     n.Name,
		Index:    &n.index,
		Domain:   n.domain,
		Fields:   n.fields,
		Schema:   n.schema,
		Parents:  n.parents,
		Children: n.children,
		Taken:    n.taken,
		Purge:    n.Purge,
		Sharding: &n.shardedBy,
	}

	switch impl := n.impl.(type) {
	case *Base:
		pnode.Impl = &boostpb.Node_Base_{Base: impl.ToProto()}
	case *Egress:
		pnode.Impl = &boostpb.Node_Egress_{Egress: impl.ToProto()}
	case *ExternalBase:
		pnode.Impl = &boostpb.Node_ExternalBase_{ExternalBase: impl.ToProto()}
	case *Filter:
		pnode.Impl = &boostpb.Node_Filter{Filter: impl.ToProto()}
	case *Grouped:
		pnode.Impl = &boostpb.Node_Grouped{Grouped: impl.ToProto()}
	case *Ingress:
		pnode.Impl = &boostpb.Node_Ingress_{Ingress: impl.ToProto()}
	case *Join:
		pnode.Impl = &boostpb.Node_Join{Join: impl.ToProto()}
	case *Project:
		pnode.Impl = &boostpb.Node_Project{Project: impl.ToProto()}
	case *Reader:
		pnode.Impl = &boostpb.Node_Reader_{Reader: impl.ToProto()}
	case *Sharder:
		pnode.Impl = &boostpb.Node_Sharder_{Sharder: impl.ToProto()}
	case *Union:
		pnode.Impl = &boostpb.Node_Union{Union: impl.ToProto()}
	case *TopK:
		pnode.Impl = &boostpb.Node_TopK{TopK: impl.ToProto()}
	case *Distinct:
		pnode.Impl = &boostpb.Node_Distinct{Distinct: impl.ToProto()}
	default:
		panic(fmt.Sprintf("ToProto: unimplemented %T", impl))
	}
	return pnode
}

func NodeFromProto(v *boostpb.Node) *Node {
	node := &Node{
		Name:      v.Name,
		index:     *v.Index,
		domain:    v.Domain,
		fields:    v.Fields,
		schema:    v.Schema,
		parents:   v.Parents,
		children:  v.Children,
		taken:     v.Taken,
		Purge:     v.Purge,
		shardedBy: *v.Sharding,
	}
	switch impl := v.Impl.(type) {
	case *boostpb.Node_Base_:
		node.impl = NewBaseFromProto(impl.Base)
	case *boostpb.Node_Egress_:
		node.impl = NewEgressFromProto(impl.Egress)
	case *boostpb.Node_ExternalBase_:
		node.impl = NewExternalBaseFromProto(impl.ExternalBase)
	case *boostpb.Node_Filter:
		node.impl = NewFilterFromProto(impl.Filter)
	case *boostpb.Node_Grouped:
		node.impl = newGroupedFromProto(impl.Grouped)
	case *boostpb.Node_Ingress_:
		node.impl = NewIngressFromProto(impl.Ingress)
	case *boostpb.Node_Join:
		node.impl = NewJoinFromProto(impl.Join)
	case *boostpb.Node_Project:
		var err error
		node.impl, err = NewProjectFromProto(impl.Project)
		if err != nil {
			panic(err.Error()) // TODO
		}
	case *boostpb.Node_Reader_:
		node.impl = NewReaderFromProto(impl.Reader)
	case *boostpb.Node_Sharder_:
		node.impl = NewSharderFromProto(impl.Sharder)
	case *boostpb.Node_Union:
		node.impl = NewUnionFromProto(impl.Union)
	case *boostpb.Node_TopK:
		node.impl = NewTopKFromProto(impl.TopK)

	case *boostpb.Node_Distinct:
		node.impl = NewDistinctFromProto(impl.Distinct)
	default:
		panic(fmt.Sprintf("FromProto: unimplemented %T", impl))
	}
	return node
}
