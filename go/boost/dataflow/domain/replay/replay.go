package replay

import (
	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/graph"
)

type Path struct {
	Source                boostpb.LocalNodeIndex
	Path                  []*boostpb.ReplayPathSegment
	NotifyDone            bool
	PartialUnicastSharder graph.NodeIdx
	Trigger               TriggerEndpoint
}

type TriggerEndpoint interface {
	trigger()
}

type TriggerStart struct {
	Columns []int
}

func (*TriggerStart) trigger() {}

type TriggerEnd struct {
	Source  SourceSelection
	Options []boostrpc.DomainClient
}

func (*TriggerEnd) trigger() {}

type TriggerLocal struct {
	Columns []int
}

func (*TriggerLocal) trigger() {}

type Partial struct {
	KeyCols         []int
	Keys            map[boostpb.Row]bool
	Tag             boostpb.Tag
	RequestingShard uint
	Unishard        bool
}

type Context struct {
	Partial *Partial
	Full    *bool
}

func (ctx *Context) Key() []int {
	if partial := ctx.Partial; partial != nil {
		return partial.KeyCols
	}
	return nil
}

type ReplayRequest struct {
	Tag  boostpb.Tag
	Data []boostpb.Row
}

type SourceSelectionKind int

const (
	SourceSelectionKeyShard SourceSelectionKind = iota
	SourceSelectionSameShard
	SourceSelectionAllShards
)

type SourceSelection struct {
	Kind        SourceSelectionKind
	NShards     uint
	KeyItoShard int
}

func (sel *SourceSelection) BuildOptions(coord *boostrpc.ChannelCoordinator, domain boostpb.DomainIndex, thisShard *uint) ([]boostrpc.DomainClient, error) {
	shard := func(shardi uint) (boostrpc.DomainClient, error) {
		return coord.GetClient(domain, shardi)
	}

	var options []boostrpc.DomainClient
	switch sel.Kind {
	case SourceSelectionKeyShard, SourceSelectionAllShards:
		for s := uint(0); s < sel.NShards; s++ {
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

func (sel *SourceSelection) ToProto() *boostpb.SourceSelection {
	var ps = &boostpb.SourceSelection{}
	switch sel.Kind {
	case SourceSelectionKeyShard:
		ps.Selection = &boostpb.SourceSelection_KeyShard_{
			KeyShard: &boostpb.SourceSelection_KeyShard{
				KeyIToShard: int64(sel.KeyItoShard),
				Nshards:     uint64(sel.NShards),
			},
		}
	case SourceSelectionAllShards:
		ps.Selection = &boostpb.SourceSelection_AllShards{
			AllShards: uint64(sel.NShards),
		}
	case SourceSelectionSameShard:
		ps.Selection = &boostpb.SourceSelection_SameShard{SameShard: true}
	}
	return ps
}

func SourceSelectionFromProto(sel *boostpb.SourceSelection) SourceSelection {
	switch sel := sel.Selection.(type) {
	case *boostpb.SourceSelection_AllShards:
		return SourceSelection{
			Kind:    SourceSelectionAllShards,
			NShards: uint(sel.AllShards),
		}
	case *boostpb.SourceSelection_KeyShard_:
		return SourceSelection{
			Kind:        SourceSelectionKeyShard,
			NShards:     uint(sel.KeyShard.Nshards),
			KeyItoShard: int(sel.KeyShard.KeyIToShard),
		}
	case *boostpb.SourceSelection_SameShard:
		return SourceSelection{Kind: SourceSelectionSameShard}
	default:
		panic("unexpected type in SourceSelection")
	}
}

func TriggerEndpointFromProto(tp *boostpb.TriggerEndpoint, coord *boostrpc.ChannelCoordinator, thisShard *uint) (TriggerEndpoint, error) {
	if tp == nil {
		return nil, nil
	}
	switch trigger := tp.Trigger.(type) {
	case *boostpb.TriggerEndpoint_Start_:
		return &TriggerStart{Columns: trigger.Start.Cols}, nil
	case *boostpb.TriggerEndpoint_Local_:
		return &TriggerLocal{Columns: trigger.Local.Cols}, nil
	case *boostpb.TriggerEndpoint_End_:
		source := SourceSelectionFromProto(trigger.End.Selection)
		options, err := source.BuildOptions(coord, trigger.End.Domain, thisShard)
		if err != nil {
			return nil, err
		}
		return &TriggerEnd{Source: source, Options: options}, nil
	default:
		panic("unexpected type in TriggerEndpoint")
	}
}
