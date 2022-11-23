package operators

import "vitess.io/vitess/go/boost/graph"

type Query struct {
	Name  string
	Roots []*Node
	View  *Node
}

func (q *Query) Leaf() graph.NodeIdx {
	return q.View.Flow.Address
}

func NewQuerySingleton(name string, node *Node) *Query {
	return &Query{
		Name:  name,
		Roots: []*Node{node},
		View:  node,
	}
}

type QueryFlowParts struct {
	Name        string
	NewNodes    []graph.NodeIdx
	ReusedNodes []graph.NodeIdx
	QueryLeaf   graph.NodeIdx
	TableReport *TableReport
}

func (qfp *QueryFlowParts) GetTableReport() *TableReport {
	return qfp.TableReport
}

func (qfp *QueryFlowParts) Leaf() graph.NodeIdx {
	return qfp.QueryLeaf
}

func (qfp *QueryFlowParts) GetName() string {
	return qfp.Name
}

func (q *Query) Optimize(mapping Mapping, sec bool) (*Query, []*Node) {
	// TODO: optimize
	return q, nil
}

type MappingElement struct {
	Src string
	Dst string
}

type Mapping map[MappingElement]string

type FlowNode struct {
	Age     NodeAge
	Address graph.NodeIdx
}

func (fn *FlowNode) Valid() bool {
	return fn.Address != graph.InvalidNode
}

type NodeAge uint8

const (
	FlowNodeNew NodeAge = iota
	FlowNodeExisting
)
