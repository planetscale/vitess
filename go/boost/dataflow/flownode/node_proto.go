package flownode

import (
	"fmt"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
)

type Map = dataflow.Map[Node]

func MapFromProto(protomap map[dataflow.LocalNodeIdx]*flownodepb.Node) *Map {
	m := new(Map)
	for k, v := range protomap {
		m.Insert(k, NodeFromProto(v))
	}
	return m
}

func MapToProto(m *Map) map[dataflow.LocalNodeIdx]*flownodepb.Node {
	protomap := make(map[dataflow.LocalNodeIdx]*flownodepb.Node)
	m.ForEach(func(idx dataflow.LocalNodeIdx, n *Node) bool {
		protomap[idx] = n.ToProto()
		return true
	})
	return protomap
}

func (n *Node) ToProto() *flownodepb.Node {
	pnode := &flownodepb.Node{
		Name:     n.name,
		Index:    &n.index,
		Domain:   n.domain,
		Fields:   n.fields,
		Schema:   n.schema,
		Parents:  n.parents,
		Children: n.children,
		Taken:    n.taken,
		Sharding: &n.shardedBy,
	}

	switch impl := n.impl.(type) {
	case *Root:
		pnode.Impl = nil
	case *Egress:
		pnode.Impl = &flownodepb.Node_Egress_{Egress: impl.ToProto()}
	case *Table:
		pnode.Impl = &flownodepb.Node_Table_{Table: impl.ToProto()}
	case *Filter:
		pnode.Impl = &flownodepb.Node_Filter{Filter: impl.ToProto()}
	case *Grouped:
		pnode.Impl = &flownodepb.Node_Grouped{Grouped: impl.ToProto()}
	case *Ingress:
		pnode.Impl = &flownodepb.Node_Ingress_{Ingress: impl.ToProto()}
	case *Join:
		pnode.Impl = &flownodepb.Node_Join{Join: impl.ToProto()}
	case *Project:
		pnode.Impl = &flownodepb.Node_Project{Project: impl.ToProto()}
	case *Reader:
		pnode.Impl = &flownodepb.Node_Reader_{Reader: impl.ToProto()}
	case *Sharder:
		pnode.Impl = &flownodepb.Node_Sharder_{Sharder: impl.ToProto()}
	case *Union:
		pnode.Impl = &flownodepb.Node_Union{Union: impl.ToProto()}
	case *TopK:
		pnode.Impl = &flownodepb.Node_TopK{TopK: impl.ToProto()}
	case *Distinct:
		pnode.Impl = &flownodepb.Node_Distinct{Distinct: impl.ToProto()}
	case *Dropped:
		pnode.Impl = nil
	default:
		panic(fmt.Sprintf("ToProto: unimplemented %T", impl))
	}
	return pnode
}

func NodeFromProto(v *flownodepb.Node) *Node {
	node := &Node{
		name:      v.Name,
		index:     *v.Index,
		domain:    v.Domain,
		fields:    v.Fields,
		schema:    v.Schema,
		parents:   v.Parents,
		children:  v.Children,
		taken:     v.Taken,
		shardedBy: *v.Sharding,
	}
	switch impl := v.Impl.(type) {
	case *flownodepb.Node_Egress_:
		node.impl = NewEgressFromProto(impl.Egress)
	case *flownodepb.Node_Table_:
		node.impl = NewTableFromProto(impl.Table)
	case *flownodepb.Node_Filter:
		node.impl = NewFilterFromProto(impl.Filter)
	case *flownodepb.Node_Grouped:
		node.impl = newGroupedFromProto(impl.Grouped)
	case *flownodepb.Node_Ingress_:
		node.impl = NewIngressFromProto(impl.Ingress)
	case *flownodepb.Node_Join:
		node.impl = NewJoinFromProto(impl.Join)
	case *flownodepb.Node_Project:
		var err error
		node.impl, err = NewProjectFromProto(impl.Project)
		if err != nil {
			panic(err.Error()) // TODO
		}
	case *flownodepb.Node_Reader_:
		node.impl = NewReaderFromProto(impl.Reader)
	case *flownodepb.Node_Sharder_:
		node.impl = NewSharderFromProto(impl.Sharder)
	case *flownodepb.Node_Union:
		node.impl = NewUnionFromProto(impl.Union)
	case *flownodepb.Node_TopK:
		node.impl = NewTopKFromProto(impl.TopK)
	case *flownodepb.Node_Distinct:
		node.impl = NewDistinctFromProto(impl.Distinct)
	default:
		panic(fmt.Sprintf("FromProto: unimplemented %T", impl))
	}
	return node
}
