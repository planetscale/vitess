package controller

import (
	"context"

	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/boostrpc/service"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/boost/server/controller/domainrpc"
	"vitess.io/vitess/go/boost/server/controller/materialization"
)

func NewDummyMigration() Migration {
	return NewMigration(context.Background(), zap.NewNop(), newDummyMigrationTarget())
}

func TestRecipeApplication(perform func(mig Migration) error) error {
	return safeMigration(NewDummyMigration(), perform)
}

type dummyDomainClient struct{}

func (d *dummyDomainClient) Ready(*packet.ReadyRequest) error                     { return nil }
func (d *dummyDomainClient) PrepareState(*packet.PrepareStateRequest) error       { return nil }
func (d *dummyDomainClient) StartReplay(*packet.StartReplayRequest) error         { return nil }
func (d *dummyDomainClient) WaitForReplay() error                                 { return nil }
func (d *dummyDomainClient) SetupReplayPath(*packet.SetupReplayPathRequest) error { return nil }
func (d *dummyDomainClient) UpdateEgress(*packet.UpdateEgressRequest) error       { return nil }
func (d *dummyDomainClient) AddNode(*packet.AddNodeRequest) error                 { return nil }
func (d *dummyDomainClient) RemoveNodes(*packet.RemoveNodesRequest) error         { return nil }
func (d *dummyDomainClient) UpdateSharder(*packet.UpdateSharderRequest) error     { return nil }

type dummyMigrationTarget struct {
	g        *graph.Graph[*flownode.Node]
	mat      *materialization.Materialization
	nodes    map[dataflow.DomainIdx][]graph.NodeIdx
	nDomains uint
	domains  map[dataflow.DomainIdx]struct{}
}

var _ MigrationTarget = (*dummyMigrationTarget)(nil)

func newDummyMigrationTarget() *dummyMigrationTarget {
	g := new(graph.Graph[*flownode.Node])
	if !g.AddNode(flownode.New("root", []string{"void"}, &flownode.Root{})).IsRoot() {
		panic("expected initial node to be Root")
	}

	return &dummyMigrationTarget{
		g:       g,
		mat:     materialization.NewMaterialization(config.DefaultMaterializationConfig()),
		nodes:   make(map[dataflow.DomainIdx][]graph.NodeIdx),
		domains: make(map[dataflow.DomainIdx]struct{}),
	}
}

func (d *dummyMigrationTarget) graph() *graph.Graph[*flownode.Node] {
	return d.g
}

func (d *dummyMigrationTarget) sharding() *uint {
	return nil
}

func (d *dummyMigrationTarget) domainMapping(dom dataflow.DomainIdx) map[graph.NodeIdx]dataflow.IndexPair {
	return make(map[graph.NodeIdx]dataflow.IndexPair)
}

func (d *dummyMigrationTarget) domainNodes() map[dataflow.DomainIdx][]graph.NodeIdx {
	return d.nodes
}

func (d *dummyMigrationTarget) materialization() *materialization.Materialization {
	return d.mat
}

func (d *dummyMigrationTarget) domainNext() dataflow.DomainIdx {
	next := dataflow.DomainIdx(d.nDomains)
	d.nDomains++
	return next
}

func (d *dummyMigrationTarget) domainExists(idx dataflow.DomainIdx) bool {
	_, ok := d.domains[idx]
	return ok
}

func (d *dummyMigrationTarget) domainShards(idx dataflow.DomainIdx) uint {
	return 1
}

func (d *dummyMigrationTarget) domainClient(ctx context.Context, idx dataflow.DomainIdx) domainrpc.Client {
	return &dummyDomainClient{}
}

func (d *dummyMigrationTarget) domainShardClient(ctx context.Context, idx dataflow.DomainIdx, shard uint) domainrpc.Client {
	return &dummyDomainClient{}
}

func (d *dummyMigrationTarget) domainPlace(ctx context.Context, idx dataflow.DomainIdx, maybeShardNumber *uint, innodes []nodeWithAge) error {
	for _, n := range innodes {
		_, err := d.g.Value(n.Idx).Finalize(d.g)
		if err != nil {
			return err
		}
	}

	d.domains[idx] = struct{}{}
	return nil
}

func (d *dummyMigrationTarget) domainAssignStream(ctx context.Context, req *service.AssignStreamRequest) error {
	return nil
}
