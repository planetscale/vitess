package worker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vthash"
)

type ExternalTableClient struct {
	desc        *boostpb.ExternalTableDescriptor
	shardingKey int
	makerecord  func(row *querypb.Row, sign bool) *boostpb.Record
	shards      []boostrpc.DomainClient
	next        *ExternalTableClient

	offsetCache []int
}

type srcmap struct {
	col int
	t   sqltypes.Type
}

func computeSourceMap(oldColumns []string, oldFields []boostpb.Type, newFields []*querypb.Field) []srcmap {
	newColIdx := make(map[string]int)
	for idx, f := range newFields {
		name := strings.ToLower(f.Name)
		newColIdx[name] = idx
	}

	var identity = true
	var output = make([]srcmap, len(oldFields))
	for oldIdx, col := range oldColumns {
		col = strings.ToLower(col)

		newIdx, found := newColIdx[col]
		if !found {
			output[oldIdx] = srcmap{col: -1}
			identity = false
			continue
		}

		output[oldIdx] = srcmap{col: newIdx, t: newFields[newIdx].Type}
		if oldIdx != newIdx {
			identity = false
		}
	}

	if len(oldFields) == len(newFields) && identity {
		return nil
	}
	return output
}

func (etc *ExternalTableClient) OnSchemaChange(newFields []*querypb.Field) {
	mapping := computeSourceMap(etc.desc.Columns, etc.desc.Schema, newFields)
	if mapping == nil {
		return
	}

	etc.offsetCache = make([]int, len(newFields))
	etc.makerecord = func(row *querypb.Row, sign bool) *boostpb.Record {
		var offset int
		for i := 0; i < len(etc.offsetCache); i++ {
			length := row.Lengths[i]
			if length < 0 {
				continue
			}
			etc.offsetCache[i] = offset
			offset += int(length)
		}

		build := boostpb.NewRowBuilder(len(mapping))
		for _, m := range mapping {
			if m.col < 0 {
				build.Add(boostpb.NULL)
				continue
			}

			length := int(row.Lengths[m.col])
			if length < 0 {
				build.Add(boostpb.NULL)
				continue
			}

			offset := etc.offsetCache[m.col]
			build.AddVitess(sqltypes.MakeTrusted(m.t, row.Values[offset:offset+length]))
		}

		record := build.Finish().ToRecord(sign)
		return &record
	}
}

func (etc *ExternalTableClient) OnEvent(ctx context.Context, events []*binlogdatapb.RowChange, gtid string) error {
	if len(etc.shards) == 1 {
		for _, ev := range events {
			var before *boostpb.Record
			var after *boostpb.Record
			if ev.Before != nil {
				before = etc.makerecord(ev.Before, false)
			}
			if ev.After != nil {
				after = etc.makerecord(ev.After, true)
			}
			err := etc.shards[0].ProcessAsync(ctx, &boostpb.Packet{
				Inner: &boostpb.Packet_Vstream_{Vstream: &boostpb.Packet_Vstream{
					Link:   &boostpb.Link{Dst: etc.desc.Addr, Src: etc.desc.Addr},
					Before: before,
					After:  after,
					Gtid:   gtid,
				}},
			})
			if err != nil {
				return err
			}
		}
		return nil
	}

	shardWrites := make([][]*boostpb.Packet, len(etc.shards))
	shardKey := etc.shardingKey
	shardType := etc.desc.Schema[shardKey]
	shardCount := uint(len(etc.shards))
	var hasher vthash.Hasher

	for _, ev := range events {
		var shard uint
		var before *boostpb.Record
		var after *boostpb.Record

		if ev.Before != nil {
			before = etc.makerecord(ev.Before, false)
			shard = before.Row.ShardValue(&hasher, shardKey, shardType, shardCount)
		}
		if ev.After != nil {
			after = etc.makerecord(ev.After, true)
			shardn := after.Row.ShardValue(&hasher, shardKey, shardType, shardCount)

			if ev.Before != nil && shardn != shard {
				return fmt.Errorf("sharding key changed in update")
			}
			shard = shardn
		}
		shardWrites[shard] = append(shardWrites[shard], &boostpb.Packet{
			Inner: &boostpb.Packet_Vstream_{Vstream: &boostpb.Packet_Vstream{
				Link:   &boostpb.Link{Dst: etc.desc.Addr, Src: etc.desc.Addr},
				Before: before,
				After:  after,
				Gtid:   gtid,
			}},
		})
	}

	for s, ps := range shardWrites {
		if len(ps) == 0 {
			continue
		}
		for _, p := range ps {
			err := etc.shards[s].ProcessAsync(ctx, p)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type EventProcessor struct {
	log      *zap.Logger
	resolver Resolver
	sender   *DomainSenders
	stats    *workerStats

	cancel context.CancelFunc
	wg     sync.WaitGroup

	tables  map[string]*ExternalTableClient
	targets []*vstreamTarget
}

type vstreamTarget struct {
	pb       *querypb.Target
	position string
}

func NewEventProcessor(log *zap.Logger, stats *workerStats, coord *boostrpc.ChannelCoordinator, resolver Resolver) *EventProcessor {
	return &EventProcessor{
		log:      log.With(zap.String("context", "EventProcessor")),
		resolver: resolver,
		sender: &DomainSenders{
			coord: coord,
			conns: common.NewSyncMap[boostpb.DomainAddr, boostrpc.DomainClient](),
		},
		stats:  stats,
		tables: make(map[string]*ExternalTableClient),
	}
}

func NewExternalTableClientFromProto(pb *boostpb.ExternalTableDescriptor, sender *DomainSenders) (*ExternalTableClient, error) {
	var shards = make([]boostrpc.DomainClient, 0, len(pb.Txs))

	for _, addr := range pb.Txs {
		client, err := sender.conns.GetOrSet(*addr, func() (boostrpc.DomainClient, error) {
			return sender.coord.GetClient(addr.Domain, addr.Shard)
		})
		if err != nil {
			return nil, err
		}
		shards = append(shards, client)
	}

	var shardingKey int
	if len(shards) > 1 {
		if pb.Key == nil {
			return nil, fmt.Errorf("sharded External Table without sharding key")
		}
		if len(pb.Key) > 1 {
			return nil, fmt.Errorf("sharded External Table with complex sharding key")
		}
		shardingKey = pb.Key[0]
	}

	schema := pb.Schema
	makerecord := func(row *querypb.Row, sign bool) *boostpb.Record {
		offset := int64(0)
		build := boostpb.NewRowBuilder(len(schema))
		for i, length := range row.Lengths {
			if length < 0 {
				build.AddVitess(sqltypes.NULL)
				continue
			}

			build.AddVitess(sqltypes.MakeTrusted(schema[i].T, row.Values[offset:offset+length]))
			offset += length
		}

		record := build.Finish().ToRecord(sign)
		return &record
	}

	return &ExternalTableClient{
		desc:        pb,
		shardingKey: shardingKey,
		shards:      shards,
		makerecord:  makerecord,
	}, nil
}

func (ep *EventProcessor) AssignTables(ctx context.Context, tables []*boostpb.ExternalTableDescriptor) error {
	ep.log.Info("assigning new vstream tables", zap.Int("num_tables", len(tables)))

	if ep.cancel != nil {
		ep.log.Debug("cancelling existing vstream...")
		ep.cancel()
		ep.wg.Wait()
		ep.cancel = nil
	}

	for _, table := range tables {
		client, err := NewExternalTableClientFromProto(table, ep.sender)
		if err != nil {
			return err
		}

		tname := table.Keyspace + "." + table.TableName
		client.next = ep.tables[tname]
		ep.tables[tname] = client
	}

	return ep.process(ctx)
}

func (ep *EventProcessor) process(ctx context.Context) error {
	var newGenerationCtx context.Context
	newGenerationCtx, ep.cancel = context.WithCancel(ctx)

	keyspaces := make(map[string][]string)
	for _, table := range ep.tables {
		ks := table.desc.Keyspace
		keyspaces[ks] = append(keyspaces[ks], table.desc.TableName)
	}

	for ks, tables := range keyspaces {
		tables := tables
		shards, _, err := ep.resolver.GetAllShards(ctx, ks, topodatapb.TabletType_PRIMARY)
		if err != nil {
			return err
		}

		for _, shard := range shards {
			shard := shard

			var target *vstreamTarget
			for _, t := range ep.targets {
				if proto.Equal(t.pb, shard.Target) {
					target = t
					break
				}
			}
			if target == nil {
				target = &vstreamTarget{pb: shard.Target}
				ep.targets = append(ep.targets, target)
			}

			ep.wg.Add(1)
			go func() {
				ep.processTarget(newGenerationCtx, shard.Gateway, target, tables)
			}()
		}
	}
	return nil
}

func (ep *EventProcessor) processTarget(ctx context.Context, gateway srvtopo.Gateway, target *vstreamTarget, interestingTables []string) {
	defer ep.wg.Done()

	log := ep.log.With(
		zap.String("target.keyspace", target.pb.Keyspace),
		zap.String("target.shard", target.pb.Shard),
		zap.String("target.cell", target.pb.Cell),
	)

	log.Info("started VStream")

	rules := make([]*binlogdatapb.Rule, 0, len(interestingTables))
	for _, table := range interestingTables {
		rules = append(rules, &binlogdatapb.Rule{
			Match: table,
			// TODO: remove once upstream Vitess has fixed this issue
			Filter: fmt.Sprintf("select * from %s", table),
		})
	}

	request := &binlogdatapb.VStreamRequest{
		Target: target.pb,
		Filter: &binlogdatapb.Filter{Rules: rules},
	}

	if target.position == "" {
		request.Position = "current"
	} else {
		request.Position = mysql.Mysql56FlavorID + "/" + target.position
	}

	var retries int
	for {
		err := gateway.VStream(ctx, request, func(events []*binlogdatapb.VEvent) error {
			retries = 0

			for _, ev := range events {
				switch ev.Type {
				case binlogdatapb.VEventType_ROW:
					rowev := ev.RowEvent
					tablename := rowev.Keyspace + "." + rowev.TableName

					if target.position == "" {
						log.Warn("failed to process Event: no previous GTID event received")
						continue
					}

					for client := ep.tables[tablename]; client != nil; client = client.next {
						if err := client.OnEvent(ctx, rowev.RowChanges, target.position); err != nil {
							log.Error("failed to process Event on client", zap.String("table", tablename), zap.Error(err))
							ep.stats.onVStreamError()
						}
					}
					ep.stats.onVStreamRows(len(rowev.RowChanges))

				case binlogdatapb.VEventType_FIELD:
					fieldev := ev.FieldEvent
					tablename := fieldev.Keyspace + "." + fieldev.TableName

					for client := ep.tables[tablename]; client != nil; client = client.next {
						client.OnSchemaChange(fieldev.Fields)
					}
					ep.stats.onVStreamFields()

				case binlogdatapb.VEventType_GTID:
					var flavor string
					flavor, target.position, _ = strings.Cut(ev.Gtid, "/")
					if flavor != mysql.Mysql56FlavorID {
						return fmt.Errorf("unexpected GTID flavor in VStream: %q (Boost only supports MySQL 5.6 GTIDs)", flavor)
					}
				}
			}
			return nil
		})

		if ctx.Err() != nil {
			log.Info("vstream finished")
			return
		}

		retries++
		delay := time.Duration(retries) * time.Second
		if delay > 5*time.Second {
			delay = 5 * time.Second
		}

		log.Error("vstream failed; retrying...", zap.Error(err), zap.Duration("delay", delay))
		ep.stats.onVStreamError()
		time.Sleep(delay)
	}
}
