package controller

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/materialization"
	"vitess.io/vitess/go/vt/sqlparser"
)

// dummyMigration wraps a migration and disables all the APIs that actually
// perform the migration on a cluster's active domains.
type dummyMigration struct {
	*migration
}

func NewDummyMigration() Migration {
	g := new(graph.Graph[*flownode.Node])
	if !g.AddNode(flownode.New("source", []string{"void"}, &flownode.Source{})).IsSource() {
		panic("expected initial node to be Source")
	}
	return &dummyMigration{
		migration: &migration{
			Context:   context.Background(),
			log:       zap.NewNop(),
			target:    nil,
			graph:     g,
			added:     make(map[graph.NodeIdx]bool),
			readers:   make(map[graph.NodeIdx]graph.NodeIdx),
			upqueries: make(map[graph.NodeIdx]sqlparser.SelectStatement),
			start:     time.Now(),
			uuid:      uuid.New(),
		},
	}
}

func (d *dummyMigration) SendPacket(domain boostpb.DomainIndex, b *boostpb.Packet) error {
	return nil
}

func (d *dummyMigration) SendPacketSync(domain boostpb.DomainIndex, b *boostpb.SyncPacket) error {
	return nil
}

func (d *dummyMigration) DomainShards(domain boostpb.DomainIndex) uint {
	return 1
}

func (d *dummyMigration) Activate(recipe *boostplan.VersionedRecipe, schema *boostplan.SchemaInformation) (*boostplan.ActivationResult, error) {
	return recipe.Activate(d, schema)
}

func (d *dummyMigration) Commit() error {
	newNodes := maps.Clone(d.added)
	mat := materialization.NewMaterialization()

	// We need to set dummy local node addresses here
	// for the packets that the materialization creates.
	// We also assign the same domain to all nodes
	nIdx := uint32(0)
	dm := boostpb.DomainIndex(0)
	d.graph.ForEachValue(func(n *flownode.Node) bool {
		ip := boostpb.NewIndexPair(n.GlobalAddr())
		ip.SetLocal(boostpb.LocalNodeIndex(nIdx))
		n.SetFinalizedAddr(ip)
		n.AddTo(dm)
		nIdx++
		return true
	})
	return mat.Commit(d, newNodes)
}

var _ Migration = (*dummyMigration)(nil)
