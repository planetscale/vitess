package replay

import (
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/upquery"
	"vitess.io/vitess/go/boost/sql"
)

type Path struct {
	Source                dataflow.LocalNodeIdx
	Path                  []*packet.ReplayPathSegment
	NotifyDone            bool
	PartialUnicastSharder graph.NodeIdx
	Trigger               *TriggerEndpoint
	Upquery               *upquery.Upquery
}

type Partial struct {
	KeyCols         []int
	Keys            map[sql.Row]bool
	Tag             dataflow.Tag
	RequestingShard uint
	Unishard        bool
}

type Regular struct {
	Last bool
}

type Context struct {
	Partial *Partial
	Regular *Regular
}

func (ctx *Context) Key() []int {
	if partial := ctx.Partial; partial != nil {
		return partial.KeyCols
	}
	return nil
}

func (ctx *Context) Tag() dataflow.Tag {
	if partial := ctx.Partial; partial != nil {
		return partial.Tag
	}
	return dataflow.TagNone
}

type ReplayRequest struct {
	Tag  dataflow.Tag
	Data []sql.Row
}

type SourceSelectionKind = packet.SourceSelection_Kind

const (
	SourceSelectionKeyShard  = packet.SourceSelection_KEY_SHARD
	SourceSelectionSameShard = packet.SourceSelection_SAME_SHARD
	SourceSelectionAllShards = packet.SourceSelection_ALL_SHARDS
)

type SourceSelection = packet.SourceSelection

func sourceSelectionClients(sel *SourceSelection, coord *boostrpc.ChannelCoordinator, domain dataflow.DomainIdx, thisShard *uint) ([]boostrpc.DomainClient, error) {
	shard := func(shardi uint) (boostrpc.DomainClient, error) {
		return coord.GetClient(domain, shardi)
	}

	var options []boostrpc.DomainClient
	switch sel.Kind {
	case SourceSelectionKeyShard, SourceSelectionAllShards:
		for s := uint(0); s < sel.NumShards; s++ {
			client, err := shard(s)
			if err != nil {
				return nil, err
			}
			options = append(options, client)
		}
	case SourceSelectionSameShard:
		if thisShard == nil {
			panic("SelectionSameShard on unsharded domaij")
		}
		client, err := shard(*thisShard)
		if err != nil {
			return nil, err
		}
		options = append(options, client)
	}
	return options, nil
}

type TriggerKind = packet.TriggerEndpoint_Kind

const (
	TriggerNone     = packet.TriggerEndpoint_NONE
	TriggerStart    = packet.TriggerEndpoint_START
	TriggerEnd      = packet.TriggerEndpoint_END
	TriggerLocal    = packet.TriggerEndpoint_LOCAL
	TriggerExternal = packet.TriggerEndpoint_EXTERNAL
)

type TriggerEndpoint struct {
	packet.TriggerEndpoint
	Clients []boostrpc.DomainClient
}

func TriggerEndpointFromProto(tp *packet.TriggerEndpoint, coord *boostrpc.ChannelCoordinator, thisShard *uint) (*TriggerEndpoint, error) {
	var clients []boostrpc.DomainClient

	if tp.SourceSelection != nil {
		var err error
		clients, err = sourceSelectionClients(tp.SourceSelection, coord, tp.Domain, thisShard)
		if err != nil {
			return nil, err
		}
	}

	return &TriggerEndpoint{
		TriggerEndpoint: *tp,
		Clients:         clients,
	}, nil
}
