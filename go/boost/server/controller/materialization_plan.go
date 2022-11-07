package controller

import (
	"context"
	"sort"

	"github.com/tidwall/btree"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
)

func materializationPlanOnJoin(g *graph.Graph[*flownode.Node]) OnJoin {
	return func(node graph.NodeIdx, cols []int, inparents []graph.NodeIdx) graph.NodeIdx {
		if len(inparents) <= 1 {
			panic("this function should only be called when there's a choice")
		}

		n := g.Value(node)
		if !n.IsInternal() {
			panic("only internal nodes should have multiple parents")
		}

		options := n.MustReplayAmong()

		// the node dictates that we *must* replay the state of some ancestor(s)
		var parents = make([]graph.NodeIdx, 0, len(inparents))
		for _, parent := range inparents {
			if _, ok := options[parent]; ok {
				parents = append(parents, parent)
			}
		}

		if columnsAreSome(cols) {
			first := cols[0]

			var universalSrc []graph.NodeIdx
			for _, pc := range n.ParentColumns(first) {
				if pc.Node == node || pc.Column < 0 {
					continue
				}
				if _, ok := options[pc.Node]; !ok {
					continue
				}

				alsoToSrc := true
				for _, c := range cols[1:] {
					if slices.IndexFunc(n.ParentColumns(c), func(pc1 flownode.NodeColumn) bool {
						return pc1.Node == pc.Node && pc1.Column >= 0
					}) < 0 {
						alsoToSrc = false
					}
				}
				if alsoToSrc {
					universalSrc = append(universalSrc, pc.Node)
				}
			}

			if len(universalSrc) > 0 {
				parents = universalSrc
			}
			// Otherwise no ancestor has all the index columns, so any will do (and we won't be partial).
		}

		if len(parents) == 1 {
			return parents[0]
		}

		// ensure that our choice of multiple possible parents is deterministic
		sort.Slice(parents, func(i, j int) bool {
			return parents[i] < parents[j]
		})

		// TODO:
		// if any required parent is empty, and we know we're building a full materialization,
		// the join must be empty (since outer join targets aren't required), and therefore
		// we can just pick that parent and get a free full materialization.

		// any parent is fine
		return parents[0]
	}
}

type PendingReplay struct {
	Tag          boostpb.Tag
	Source       boostpb.LocalNodeIndex
	SourceDomain boostpb.DomainIndex
	TargetDomain boostpb.DomainIndex
}

type domainTag struct {
	tag    boostpb.Tag
	domain boostpb.DomainIndex
}

type plan struct {
	m       *Materialization
	graph   *graph.Graph[*flownode.Node]
	node    graph.NodeIdx
	domains map[boostpb.DomainIndex]*DomainHandle
	workers map[WorkerID]*Worker
	partial bool

	tags    map[common.ColumnSet][]domainTag
	paths   map[boostpb.Tag][]graph.NodeIdx
	pending []PendingReplay
}

func (p *plan) add(ctx context.Context, indexOn []int) error {
	log := common.Logger(ctx)
	indexOnKey := common.NewColumnSet(indexOn)
	if !p.partial && len(p.paths) > 0 {
		// non-partial views should not have one replay path per index. that would cause us to
		// replay several times, even though one full replay should always be sufficient.
		// we do need to keep track of the fact that there should be an index here though.
		if _, ok := p.tags[indexOnKey]; !ok {
			p.tags[indexOnKey] = []domainTag{}
		}
		return nil
	}

	paths := p.findPaths(indexOn)

	// all right, story time!
	//
	// image you have this graph:
	//
	//     a     b
	//     +--+--+
	//        |
	//       u_1
	//        |
	//     +--+--+
	//     c     d
	//     +--+--+
	//        |
	//       u_2
	//        |
	//     +--+--+
	//     e     f
	//     +--+--+
	//        |
	//       u_3
	//        |
	//        v
	//
	// where c-f are all stateless. you will end up with 8 paths for replays to v.
	// a and b will both appear as the root of 4 paths, and will be upqueried that many times.
	// while inefficient (TODO), that is not in and of itself a problem. the issue arises at
	// the unions, which need to do union buffering (that is, they need to forward _one_
	// upquery response for each set of upquery responses they get). specifically, u_1 should
	// forward 4 responses, even though it receives 8. u_2 should forward 2 responses, even
	// though it gets 4, etc. we may later optimize that (in theory u_1 should be able to only
	// forward _one_ response to multiple children, and a and b should only be upqueried
	// _once_), but for now we need to deal with the correctness issue that arises if the
	// unions do not buffer correctly.
	//
	// the issue, ultimately, is what the unions "group" upquery responses by. they can't group
	// by tag (like shard mergers do), since there are 8 tags here, so there'd be 8 groups each
	// with one response. here are the replay paths for u_1:
	//
	//  1. a -> c -> e
	//  2. a -> c -> f
	//  3. a -> d -> e
	//  4. a -> d -> f
	//  5. b -> c -> e
	//  6. b -> c -> f
	//  7. b -> d -> e
	//  8. b -> d -> f
	//
	// we want to merge 1 with 5 since they're "going the same way". similarly, we want to
	// merge 2 and 6, 3 and 7, and 4 and 8. the "grouping" here then is really the suffix of
	// the replay's path beyond the union we're looking at. for u_2:
	//
	//  1/5. a/b -> c -> e
	//  2/6. a/b -> c -> f
	//  3/7. a/b -> d -> e
	//  4/8. a/b -> d -> f
	//
	// we want to merge 1/5 and 3/7, again since they are going the same way _from here_.
	// and similarly, we want to merge 2/6 and 4/8.
	//
	// so, how do we communicate this grouping to each of the unions?
	// well, most of the infrastructure is actually already there in the domains.
	// for each tag, each domain keeps some per-node state (`ReplayPathSegment`).
	// we can inject the information there!
	//
	// we're actually going to play an additional trick here, as it allows us to simplify the
	// implementation a fair amount. since we know that tags 1 and 5 are identical beyond u_1
	// (that's what we're grouping by after all!), why don't we just rewrite all 1 tags to 5s?
	// and all 2s to 6s, and so on. that way, at u_2, there will be no replays with tag 1 or 3,
	// only 5 and 7. then we can pull the same trick there -- rewrite all 5s to 7s, so that at
	// u_3 we only need to deal with 7s (and 8s). this simplifies the implementation since
	// unions can now _always_ just group by tags, and it'll just magically work.
	//
	// this approach also gives us the property that we have a deterministic subset of the tags
	// (and of strictly decreasing cardinality!) tags downstream of unions. this may (?)
	// improve cache locality, but could perhaps also allow further optimizations later (?).

	// find all paths through each union with the same suffix

	var assignedTags []boostpb.Tag
	for range paths {
		assignedTags = append(assignedTags, p.m.nextTag())
	}

	type suffix struct {
		union graph.NodeIdx
		path  Path
		pi    []int
	}

	var unionSuffixes = btree.NewGeneric(func(a, b *suffix) bool {
		if a.union < b.union {
			return true
		}
		if a.union > b.union {
			return false
		}
		return a.path.Compare(b.path) < 0
	})

	for pi, path := range paths {
		for at, seg := range path {
			n := p.graph.Value(seg.Node)
			if n.IsUnion() && !n.IsShardMerger() {
				suf := &suffix{
					union: seg.Node,
					path:  path[at+1:],
				}
				if s, ok := unionSuffixes.Get(suf); ok {
					s.pi = append(s.pi, pi)
				} else {
					suf.pi = []int{pi}
					unionSuffixes.Set(suf)
				}
			}
		}
	}

	type grouping struct {
		union graph.NodeIdx
		pi    int
	}

	// map each suffix-sharing group of paths at each union to one tag at that union
	pathGrouping := make(map[grouping]boostpb.Tag)
	unionSuffixes.Scan(func(s *suffix) bool {
		// at this union, all the given paths share a suffix
		// make all of the paths use a single identifier from that point on
		tagAll := assignedTags[s.pi[0]]
		for _, pi := range s.pi {
			pathGrouping[grouping{s.union, pi}] = tagAll
		}
		return true
	})

	var tags []domainTag
	for pi, path := range paths {
		tag := assignedTags[pi]

		var pathNodes []graph.NodeIdx
		for _, seg := range path {
			pathNodes = append(pathNodes, seg.Node)
		}
		p.paths[tag] = pathNodes

		// what key are we using for partial materialization (if any)?
		var partial []int
		if p.partial {
			partial = path[0].Columns
		}

		// if this is a partial replay path, and the target node is sharded, then we need to
		// make sure that the last sharder on the path knows to only send the replay response
		// to the requesting shard, as opposed to all shards. in order to do that, that sharder
		// needs to know who it is!
		var partialUnicastSharder = graph.InvalidNode
		if partial != nil && !p.graph.Value(path[len(path)-1].Node).Sharding().IsNone() {
			for n := len(path) - 1; n >= 0; n-- {
				ni := path[n].Node
				if p.graph.Value(ni).IsSharder() {
					partialUnicastSharder = ni
				}
			}
		}

		type segment struct {
			domain boostpb.DomainIndex
			path   Path
		}
		var segments []segment
		var lastDomain = boostpb.InvalidDomainIndex

		for _, pe := range path {
			dom := p.graph.Value(pe.Node).Domain()
			if lastDomain == boostpb.InvalidDomainIndex || dom != lastDomain {
				segments = append(segments, segment{domain: dom})
				lastDomain = dom
			}

			var key []int
			if p.partial {
				key = pe.Columns
			}
			seg := &segments[len(segments)-1]
			seg.path = append(seg.path, PathElement{Node: pe.Node, Columns: key})
		}

		log.Debug("domain replay path configured", zap.Any("path", segments), tag.Zap())

		var pending *PendingReplay
		var seen = make(map[boostpb.DomainIndex]struct{})

		for i, seg := range segments {
			// TODO:
			//  a domain may appear multiple times in this list if a path crosses into the same
			//  domain more than once. currently, that will cause a deadlock.
			if _, seen := seen[seg.domain]; seen {
				panic("detected a-b-a replay path")
			}
			seen[seg.domain] = struct{}{}

			iternodes := seg.path
			if i == 0 {
				// we're not replaying through the starter node
				iternodes = iternodes[1:]
			}

			var locals = make([]*boostpb.ReplayPathSegment, 0, len(iternodes))
			for _, seg := range iternodes {
				forceTag, ok := pathGrouping[grouping{seg.Node, pi}]
				if !ok {
					forceTag = boostpb.TagInvalid
				}

				locals = append(locals, &boostpb.ReplayPathSegment{
					Node:       p.graph.Value(seg.Node).LocalAddr(),
					ForceTagTo: forceTag,
					PartialKey: seg.Columns,
				})
			}

			if len(locals) == 0 {
				if i != 0 {
					panic("unexpected empty locals")
				}
				continue
			}

			var setup = &boostpb.SyncPacket_SetupReplayPath{
				Tag:                   tag,
				Source:                boostpb.InvalidLocalNode,
				Path:                  locals,
				PartialUnicastSharder: partialUnicastSharder,
				NotifyDone:            false,
				Trigger:               nil,
			}
			if i == 0 {
				setup.Source = p.graph.Value(seg.path[0].Node).LocalAddr()
			}

			if partial != nil {
				// for partial materializations, nodes need to know how to trigger replays
				switch {
				case len(segments) == 1:
					setup.Trigger = &boostpb.TriggerEndpoint{
						Trigger: &boostpb.TriggerEndpoint_Local_{
							Local: &boostpb.TriggerEndpoint_Local{
								Cols: partial,
							},
						},
					}
				case i == 0:
					// first domain needs to be told about partial replay trigger
					setup.Trigger = &boostpb.TriggerEndpoint{
						Trigger: &boostpb.TriggerEndpoint_Start_{
							Start: &boostpb.TriggerEndpoint_Start{
								Cols: partial,
							},
						},
					}
				case i == len(segments)-1:
					// if the source is sharded, we need to know whether we should ask all
					// the shards, or just one. if the replay key is the same as the
					// sharding key, we just ask one, and all is good. if the replay key
					// and the sharding key differs, we generally have to query *all* the
					// shards.
					//
					// there is, however, an exception to this: if we have two domains that
					// have the same sharding, but a replay path between them on some other
					// key than the sharding key, the right thing to do is to *only* query
					// the same shard as ourselves. this is because any answers from other
					// shards would necessarily just be with records that do not match our
					// sharding key anyway, and that we should thus never see.
					srcSharding := p.graph.Value(segments[0].path[0].Node).Sharding()
					shards := common.UnwrapOr(srcSharding.TryGetShards(), uint(1))

					var lookupKeyToShard *int
					if c, _, ok := srcSharding.ByColumn(); ok {
						lookupKey := seg.path[0].Columns
						if len(lookupKey) == 1 {
							if c == lookupKey[0] {
								var key0 = 0
								lookupKeyToShard = &key0
							}
						} else {
							// we're using a compound key to look up into a node that's
							// sharded by a single column. if the sharding key is one
							// of the lookup keys, then we indeed only need to look at
							// one shard, otherwise we need to ask all
							//
							// NOTE: this _could_ be merged with the if arm above,
							// but keeping them separate allows us to make this case
							// explicit and more obvious
							var key int
							for key = 0; key < len(lookupKey); key++ {
								if lookupKey[key] == c {
									lookupKeyToShard = &key
									break
								}
							}
						}
					}

					var selection replay.SourceSelection

					if lookupKeyToShard != nil {
						// if we are not sharded, all is okay.
						//
						// if we are sharded:
						//
						//  - if there is a shuffle above us, a shard merger + sharder
						//    above us will ensure that we hear the replay response.
						//
						//  - if there is not, we are sharded by the same column as the
						//    source. this also means that the replay key in the
						//    destination is the sharding key of the destination. to see
						//    why, consider the case where the destination is sharded by x.
						//    the source must also be sharded by x for us to be in this
						//    case. we also know that the replay lookup key on the source
						//    must be x since lookup_on_shard_key == true. since no shuffle
						//    was introduced, src.x must resolve to dst.x assuming x is not
						//    aliased in dst. because of this, it should be the case that
						//    KeyShard == SameShard; if that were not the case, the value
						//    in dst.x should never have reached dst in the first place.
						selection = replay.SourceSelection{
							Kind:        replay.SourceSelectionKeyShard,
							NShards:     shards,
							KeyItoShard: *lookupKeyToShard,
						}
					} else {
						// replay key != sharding key
						// normally, we'd have to query all shards, but if we are sharded
						// the same as the source (i.e., there are no shuffles between the
						// source and us), then we should *only* query the same shard of
						// the source (since it necessarily holds all records we could
						// possibly see).
						//
						// note that the no-sharding case is the same as "ask all shards"
						// except there is only one (shards == 1).
						findMergers := func(segments []segment) bool {
							for _, seg := range segments {
								for _, pathseg := range seg.path {
									if p.graph.Value(pathseg.Node).IsShardMerger() {
										return true
									}
								}
							}
							return false
						}

						if srcSharding.IsNone() || findMergers(segments) {
							selection = replay.SourceSelection{Kind: replay.SourceSelectionAllShards, NShards: shards}
						} else {
							selection = replay.SourceSelection{Kind: replay.SourceSelectionSameShard}
						}
					}

					setup.Trigger = &boostpb.TriggerEndpoint{Trigger: &boostpb.TriggerEndpoint_End_{End: &boostpb.TriggerEndpoint_End{
						Selection: selection.ToProto(),
						Domain:    segments[0].domain,
					}}}
				}
			} else {
				// for full materializations, the last domain should report when it's done
				if i == len(segments)-1 {
					setup.NotifyDone = true
					pending = &PendingReplay{
						Tag:          tag,
						Source:       p.graph.Value(segments[0].path[0].Node).LocalAddr(),
						SourceDomain: segments[0].domain,
						TargetDomain: seg.domain,
					}
				}
			}

			if i < len(segments)-1 {
				// since there is a later domain, the last node of any non-final domain
				// must either be an egress or a Sharder. If it's an egress, we need
				// to tell it about this replay path so that it knows
				// what path to forward replay packets on.
				n := p.graph.Value(seg.path[len(seg.path)-1].Node)

				var pkt boostpb.Packet
				pkt.Inner = &boostpb.Packet_UpdateEgress_{
					UpdateEgress: &boostpb.Packet_UpdateEgress{
						Node:  n.LocalAddr(),
						NewTx: nil,
						NewTag: &boostpb.Packet_UpdateEgress_Tag{
							Tag:  tag,
							Node: segments[i+1].path[0].Node,
						},
					},
				}

				if n.IsEgress() {
					if err := p.domains[seg.domain].SendToHealthy(ctx, &pkt, p.workers); err != nil {
						return err
					}
				} else {
					if !n.IsSharder() {
						panic("node should be Egress or Sharder")
					}
				}
			}

			var pkt boostpb.SyncPacket
			pkt.Inner = &boostpb.SyncPacket_SetupReplayPath_{SetupReplayPath: setup}

			sender := p.domains[seg.domain]
			if err := sender.SendToHealthySync(ctx, &pkt, p.workers); err != nil {
				return err
			}
		}

		if !p.partial {
			if pending == nil {
				panic("no replay for full materialization?")
			}
			p.pending = append(p.pending, *pending)
		}
		tags = append(tags, domainTag{tag, lastDomain})
	}

	p.tags[indexOnKey] = append(p.tags[indexOnKey], tags...)
	return nil
}

func (p *plan) finalize(ctx context.Context) (*boostpb.Packet_PrepareState, []PendingReplay, error) {
	var state boostpb.Packet_PrepareState

	node := p.graph.Value(p.node)
	state.Node = node.LocalAddr()

	if r := node.AsReader(); r != nil {
		if p.partial {
			if !r.IsMaterialized() {
				panic("expected reader to be materialized")
			}

			lastDomain := node.Domain()
			numShards := p.domains[lastDomain].Shards()

			state.State = &boostpb.Packet_PrepareState_PartialGlobal_{PartialGlobal: &boostpb.Packet_PrepareState_PartialGlobal{
				Gid:  p.node,
				Cols: len(node.Fields()),
				Key:  r.Key(),
				TriggerDomain: &boostpb.Packet_PrepareState_PartialGlobal_TriggerDomain{
					Domain: lastDomain,
					Shards: numShards,
				},
			},
			}
		} else {
			state.State = &boostpb.Packet_PrepareState_Global_{Global: &boostpb.Packet_PrepareState_Global{
				Gid:  p.node,
				Cols: len(node.Fields()),
				Key:  r.Key(),
			}}
		}
	} else {
		if p.partial {
			local := &boostpb.Packet_PrepareState_PartialLocal{}
			for k, paths := range p.tags {
				var tags []boostpb.Tag
				for _, dt := range paths {
					tags = append(tags, dt.tag)
				}
				local.Index = append(local.Index, &boostpb.Packet_PrepareState_PartialLocal_Index{
					Key:  k.ToSlice(),
					Tags: tags,
				})
			}
			state.State = &boostpb.Packet_PrepareState_PartialLocal_{PartialLocal: local}
		} else {
			local := &boostpb.Packet_PrepareState_IndexedLocal{}
			for k := range p.tags {
				local.Index = append(local.Index, &boostpb.Packet_PrepareState_IndexedLocal_Index{
					Key: k.ToSlice(),
				})
			}
			state.State = &boostpb.Packet_PrepareState_IndexedLocal_{IndexedLocal: local}
		}
	}

	err := p.domains[node.Domain()].SendToHealthy(ctx,
		&boostpb.Packet{Inner: &boostpb.Packet_PrepareState_{PrepareState: &state}},
		p.workers,
	)
	if err != nil {
		return nil, nil, err
	}

	if !p.partial {
		// we know that this must be a *new* fully materialized node:
		//
		//  - finalize() is only called by setup()
		//  - setup() is only called for existing nodes if they are partial
		//  - this branch has !self.partial
		//
		// if we're constructing a new view, there is no reason to replay any given path more
		// than once. we do need to be careful here though: the fact that the source and
		// destination of a path are the same does *not* mean that the path is the same (b/c of
		// unions), and we do not want to eliminate different paths!
		// TODO: dedup
		if len(p.pending) == 0 {
			panic("pending should not be empty")
		}
	} else {
		if len(p.pending) > 0 {
			panic("pending should be empty")
		}
	}
	return &state, p.pending, nil
}

func (p *plan) cutPath(path []PathElement) []PathElement {
	var cut = 1
	for cut < len(path) {
		node := path[cut].Node
		cut++

		if p.m.have.contains(node) {
			//we want to take this node, but not any later ones
			break
		}
	}
	path = path[:cut]

	// reverse the path
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	return path
}

func (p *plan) findPaths(columns []int) [][]PathElement {
	graph := p.graph
	ni := p.node
	paths := ProvenanceOf(graph, ni, columns, materializationPlanOnJoin(graph))

	// cut paths so they only reach to the the closest materialized node
	for i, path := range paths {
		paths[i] = p.cutPath(path)
	}

	// since we cut off part of each path, we *may* now have multiple paths that are the same
	// (i.e., if there was a union above the nearest materialization). this would be bad, as it
	// would cause a domain to request replays *twice* for a key from one view!
	sort.Slice(paths, func(i, j int) bool {
		return Path(paths[i]).Compare(paths[j]) < 0
	})
	paths = slices.CompactFunc(paths, func(a, b []PathElement) bool {
		return Path(a).Compare(b) == 0
	})
	return paths
}

func newMaterializationPlan(m *Materialization, g *graph.Graph[*flownode.Node], node graph.NodeIdx, domains map[boostpb.DomainIndex]*DomainHandle, workers map[WorkerID]*Worker) *plan {
	return &plan{
		m:       m,
		graph:   g,
		node:    node,
		domains: domains,
		workers: workers,
		partial: m.partial[node],

		tags:  make(map[common.ColumnSet][]domainTag),
		paths: make(map[boostpb.Tag][]graph.NodeIdx),
	}
}
