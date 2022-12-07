package materialization

import (
	"fmt"

	"vitess.io/vitess/go/boost/graph"
)

type PartialOverlappingPartialIndexesError struct {
	ParentNode graph.NodeIdx
	ChildNode  graph.NodeIdx
	Index      []int
	Columns    []int
	Conflict   int
}

func (e *PartialOverlappingPartialIndexesError) Error() string {
	return fmt.Sprintf("partially overlapping partial indices; "+
		"parent = %d, pcols = %#v, child = %d, cols = %#v, conflict = %d",
		e.ParentNode, e.Index, e.ChildNode, e.Columns, e.Conflict)
}

type PartialMaterializationAboveFullMaterializationError struct {
	FullNode    graph.NodeIdx
	PartialNode graph.NodeIdx
}

func (e *PartialMaterializationAboveFullMaterializationError) Error() string {
	return fmt.Sprintf("partial materialization %d above full materialization %d", e.PartialNode, e.FullNode)
}

type ObligationResolveError struct {
	Node       graph.NodeIdx
	ParentNode graph.NodeIdx
	Column     int
}

func (e *ObligationResolveError) Error() string {
	return fmt.Sprintf(
		"could not resolve obligation past operator; node: %v, ancestor: %v, column: %v",
		e.Node, e.ParentNode, e.Column)
}

type MergeShardingByAliasedColumnError struct {
	Node         graph.NodeIdx
	ParentNode   graph.NodeIdx
	SourceColumn int
}

func (e *MergeShardingByAliasedColumnError) Error() string {
	return fmt.Sprintf(
		"attempting to merge sharding by aliased column; node: %v, ancestor: %v, source column: %v",
		e.Node, e.ParentNode, e.SourceColumn)
}
