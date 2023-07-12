package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/boost/server/controller/config"

	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/boostrpc/service"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vthash"
)

type SchemaConflictTracker struct {
	mu              sync.Mutex
	affectedQueries map[string]struct{}
	ch              chan []string
	update          TopoUpdater
}

func (sct *SchemaConflictTracker) MarkAffectedQueries(dependencies []string) {
	sct.ch <- dependencies
}

func (sct *SchemaConflictTracker) Clear(ctx context.Context) error {
	sct.mu.Lock()
	defer sct.mu.Unlock()

	sct.affectedQueries = make(map[string]struct{})
	return sct.update.UpdateTopo(ctx, func(worker *vtboostpb.TopoWorkerEntry) error {
		worker.UnhealthyQueries = nil
		return nil
	})
}

func (sct *SchemaConflictTracker) updateDependencies(ctx context.Context, log *zap.Logger, dependencies []string) {
	sct.mu.Lock()
	defer sct.mu.Unlock()

	var report []string
	for _, dep := range dependencies {
		if _, ok := sct.affectedQueries[dep]; !ok {
			sct.affectedQueries[dep] = struct{}{}
			report = append(report, dep)
		}
	}

	log.Error("vstream inconsistency", zap.Strings("affected_queries", dependencies), zap.Strings("reported_queries", report))

	err := sct.update.UpdateTopo(ctx, func(worker *vtboostpb.TopoWorkerEntry) error {
		for _, dep := range report {
			worker.UnhealthyQueries = append(worker.UnhealthyQueries, &vtboostpb.TopoWorkerEntry_Query{
				QueryId: dep,
				Status:  vtboostpb.TopoWorkerEntry_SCHEMA_CHANGE_CONFLICT,
			})
		}
		return nil
	})
	if err != nil {
		log.Error("failed to update Worker status in topo", zap.Error(err))
	}
}

func (sct *SchemaConflictTracker) listen(ctx context.Context, log *zap.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case dependencies := <-sct.ch:
			sct.updateDependencies(ctx, log, dependencies)
		}
	}
}

type ExternalTableClient struct {
	desc        *service.ExternalTableDescriptor
	shardingKey int
	shards      []boostrpc.DomainClient
	next        *ExternalTableClient
}

type srcmap struct {
	col int
	t   sqltypes.Type
}

func trackColumnChange(desc *service.ExternalTableDescriptor, column string, conflict func(queryIDs []string)) {
	if dep, ok := desc.DependentColumns[column]; ok {
		conflict(dep.DependentQueries)
	}
}

func computeSourceMap(desc *service.ExternalTableDescriptor, newFields []*querypb.Field, conflict func(queryIDs []string)) []srcmap {
	newColIdx := make(map[string]int)
	for idx, f := range newFields {
		name := strings.ToLower(f.Name)
		newColIdx[name] = idx
	}

	var identity = true
	var output = make([]srcmap, len(desc.Columns))
	for oldIdx, col := range desc.Columns {
		col = strings.ToLower(col)

		newIdx, found := newColIdx[col]
		if !found {
			trackColumnChange(desc, col, conflict)
			output[oldIdx] = srcmap{col: -1}
			identity = false
			continue
		}

		if !isTypeChangeSafe(desc.Schema[oldIdx].T, newFields[newIdx].Type) {
			trackColumnChange(desc, col, conflict)
		}

		output[oldIdx] = srcmap{col: newIdx, t: newFields[newIdx].Type}
		if oldIdx != newIdx {
			identity = false
		}
	}

	if len(desc.Columns) == len(newFields) && identity {
		return nil
	}
	return output
}

func isTypeChangeSafe(t1 sqltypes.Type, t2 querypb.Type) bool {
	switch {
	case t1 == t2:
		return true
	case sqltypes.IsSigned(t1) && sqltypes.IsSigned(t2):
		return true
	case sqltypes.IsUnsigned(t1) && sqltypes.IsUnsigned(t2):
		return true
	default:
		return false
	}
}

func (target *vstreamTarget) OnSchemaChange(client *ExternalTableClient, newFields []*querypb.Field, qt *SchemaConflictTracker) {
	mapping := computeSourceMap(client.desc, newFields, qt.MarkAffectedQueries)
	if mapping == nil {
		return
	}

	offsetCache := make([]int, len(newFields))

	target.mappers[client] = func(row *querypb.Row, sign bool) sql.Record {
		var offset int
		for i := 0; i < len(offsetCache); i++ {
			length := row.Lengths[i]
			if length < 0 {
				continue
			}
			offsetCache[i] = offset
			offset += int(length)
		}

		build := sql.NewRowBuilder(len(mapping))
		for _, m := range mapping {
			if m.col < 0 {
				build.Add(sql.NULL)
				continue
			}

			length := int(row.Lengths[m.col])
			if length < 0 {
				build.Add(sql.NULL)
				continue
			}

			cachedOffset := offsetCache[m.col]
			build.AddVitess(sqltypes.MakeTrusted(m.t, row.Values[cachedOffset:cachedOffset+length]))
		}

		return build.Finish().ToRecord(sign)
	}
}

func (etc *ExternalTableClient) adjustOffsets(rs []sql.Record) {
	for i := range rs {
		rs[i].Offset = int32(i)
	}
}

func (etc *ExternalTableClient) OnEvent(ctx context.Context, makerecord mapper, events []*binlogdatapb.RowChange, gtid string) error {
	if len(etc.shards) == 1 {
		vstream := &packet.Message{
			Link:    &packet.Link{Dst: etc.desc.Addr, Src: etc.desc.Addr},
			Gtid:    gtid,
			Records: make([]sql.Record, 0, len(events)),
		}
		for _, ev := range events {
			if ev.Before != nil {
				vstream.Records = append(vstream.Records, makerecord(ev.Before, false))
			}
			if ev.After != nil {
				vstream.Records = append(vstream.Records, makerecord(ev.After, true))
			}
		}
		vstream.Records = sql.OffsetRecords(vstream.Records)
		_, err := etc.shards[0].SendMessage(ctx, vstream)
		return err
	}

	shardWrites := make([][]sql.Record, len(etc.shards))
	shardKey := etc.shardingKey
	shardType := etc.desc.Schema[shardKey]
	shardCount := uint(len(etc.shards))
	var hasher vthash.Hasher

	for _, ev := range events {
		var shard uint
		var before bool

		if ev.Before != nil {
			r := makerecord(ev.Before, false)
			shard = r.Row.ShardValue(&hasher, shardKey, shardType, shardCount)
			before = true

			shardWrites[shard] = append(shardWrites[shard], r)
		}
		if ev.After != nil {
			r := makerecord(ev.After, true)
			shard2 := r.Row.ShardValue(&hasher, shardKey, shardType, shardCount)

			if before && shard2 != shard {
				return fmt.Errorf("sharding key changed in update")
			}

			shardWrites[shard2] = append(shardWrites[shard2], r)
		}
	}

	for s, rs := range shardWrites {
		if len(rs) == 0 {
			continue
		}
		pkt := &packet.Message{
			Link:    &packet.Link{Dst: etc.desc.Addr, Src: etc.desc.Addr},
			Records: sql.OffsetRecords(rs),
			Gtid:    gtid,
		}
		_, err := etc.shards[s].SendMessage(ctx, pkt)
		if err != nil {
			return err
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

	conflicts SchemaConflictTracker

	vstreamStartTimeout time.Duration
	vstreamStartRetries int
}

type vstreamTarget struct {
	pb       *querypb.Target
	position string
	started  atomic.Bool
	mappers  map[*ExternalTableClient]mapper
}

type mapper func(row *querypb.Row, sign bool) sql.Record

func NewEventProcessor(topo TopoUpdater, log *zap.Logger, stats *workerStats, coord *boostrpc.ChannelCoordinator, resolver Resolver, cfg *config.Config) *EventProcessor {
	return &EventProcessor{
		log:      log.With(zap.String("context", "EventProcessor")),
		resolver: resolver,
		sender: &DomainSenders{
			coord: coord,
			conns: common.NewSyncMap[dataflow.DomainAddr, boostrpc.DomainClient](),
		},
		stats:  stats,
		tables: make(map[string]*ExternalTableClient),
		conflicts: SchemaConflictTracker{
			affectedQueries: make(map[string]struct{}),
			ch:              make(chan []string, 4),
			update:          topo,
		},
		vstreamStartTimeout: cfg.VstreamStartTimeout,
		vstreamStartRetries: cfg.VstreamStartRetries,
	}
}

func NewExternalTableClientFromProto(pb *service.ExternalTableDescriptor, sender *DomainSenders) (*ExternalTableClient, error) {
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

	return &ExternalTableClient{
		desc:        pb,
		shardingKey: shardingKey,
		shards:      shards,
	}, nil
}

func defaultMapper(schema []sql.Type) mapper {
	return func(row *querypb.Row, sign bool) sql.Record {
		offset := int64(0)
		build := sql.NewRowBuilder(len(schema))
		for i, length := range row.Lengths {
			if length < 0 {
				build.AddVitess(sqltypes.NULL)
				continue
			}
			build.AddVitess(sqltypes.MakeTrusted(schema[i].T, row.Values[offset:offset+length]))
			offset += length
		}
		return build.Finish().ToRecord(sign)
	}
}

func (ep *EventProcessor) Cancel() {
	if ep.cancel != nil {
		ep.log.Debug("cancelling existing vstream...")
		ep.cancel()
		ep.wg.Wait()
		ep.cancel = nil
	}
}

func (ep *EventProcessor) AssignTables(ctx context.Context, tables []*service.ExternalTableDescriptor) error {
	ep.log.Debug("assigning new vstream tables", zap.Int("num_tables", len(tables)))

	ep.Cancel()

	err := ep.conflicts.Clear(ctx)
	if err != nil {
		return err
	}

nextTable:
	for _, table := range tables {
		tname := table.Keyspace + "." + table.TableName

		for t := ep.tables[tname]; t != nil; t = t.next {
			if t.desc.Node == table.Node {
				continue nextTable
			}
		}

		client, err := NewExternalTableClientFromProto(table, ep.sender)
		if err != nil {
			return err
		}

		client.next = ep.tables[tname]
		ep.tables[tname] = client
	}

	tries := ep.vstreamStartRetries
	for tries > 0 {
		err = ep.process(ctx)
		if err == nil {
			break
		}

		ep.Cancel()
		if errors.Is(err, errVStreamTimeout) {
			var pending int
			for _, t := range ep.targets {
				if !t.started.Load() {
					pending++
				}
			}
			ep.log.Warn("vstream start timed out waiting for streams", zap.Int("streams", len(ep.targets)), zap.Int("pending", pending), zap.Error(err))
			tries--
			continue
		}
		return err
	}
	return err
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
				target = &vstreamTarget{
					pb:      shard.Target,
					mappers: make(map[*ExternalTableClient]mapper),
				}
				ep.targets = append(ep.targets, target)
			}

			ep.wg.Add(1)
			target.started.Store(false)

			go func() {
				ep.processTarget(newGenerationCtx, shard.Gateway, target, tables)
			}()
		}
	}

	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()

	start := time.Now()
	pending := ep.targets
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-tick.C:
			starting := pending
			pending = pending[:0]
			for _, tgt := range starting {
				if !tgt.started.Load() {
					pending = append(pending, tgt)
				}
			}
			if len(pending) == 0 {
				ep.log.Info("all vstreams started", zap.Duration("duration", now.Sub(start)))
				return nil
			}
			if now.Sub(start) >= ep.vstreamStartTimeout {
				return errVStreamTimeout
			}
		}
	}
}

var errVStreamTimeout = errors.New("timed out while waiting for vstreams to start")

func (ep *EventProcessor) processTarget(ctx context.Context, gateway srvtopo.Gateway, target *vstreamTarget, interestingTables []string) {
	defer ep.wg.Done()

	log := ep.log.With(
		zap.String("target.keyspace", target.pb.Keyspace),
		zap.String("target.shard", target.pb.Shard),
		zap.String("target.cell", target.pb.Cell),
	)

	log.Debug("started VStream")

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

	for _, tableClient := range ep.tables {
		for client := tableClient; client != nil; client = client.next {
			if _, found := target.mappers[client]; !found {
				target.mappers[client] = defaultMapper(client.desc.Schema)
			}
		}
	}

	var retries int
	for {
		err := gateway.VStream(ctx, request, func(events []*binlogdatapb.VEvent) error {
			target.started.Store(true)
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
						mapper := target.mappers[client]
						if err := client.OnEvent(ctx, mapper, rowev.RowChanges, target.position); err != nil {
							log.Error("failed to process Event on client", zap.String("table", tablename), zap.Error(err))
							ep.stats.onVStreamError()
						}
					}
					ep.stats.onVStreamRows(len(rowev.RowChanges))

				case binlogdatapb.VEventType_FIELD:
					fieldev := ev.FieldEvent
					tablename := fieldev.Keyspace + "." + fieldev.TableName

					for client := ep.tables[tablename]; client != nil; client = client.next {
						target.OnSchemaChange(client, fieldev.Fields, &ep.conflicts)
					}
					ep.stats.onVStreamFields()

				case binlogdatapb.VEventType_DDL:
					if err := ep.handleDDLStatement(target, ev.Keyspace, ev.Statement); err != nil {
						return err
					}

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

		// explicit context cancellation
		if ctx.Err() != nil {
			return
		}

		// forced cancellation from VStream because of unrecoverable error
		if errors.Is(err, context.Canceled) {
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

func (ep *EventProcessor) handleDDLStatement(target *vstreamTarget, ks, statement string) error {
	ddl, err := sqlparser.ParseStrictDDL(statement)
	if err != nil {
		// if we don't know how to parse a statement we assume it's not something that
		// will affect the behavior of Boost
		return nil
	}

	switch ddl := ddl.(type) {
	case *sqlparser.DropTable:
		for _, tbl := range ddl.FromTables {
			tablename := ks + "." + tbl.Name.String()
			for client := ep.tables[tablename]; client != nil; client = client.next {
				target.OnSchemaChange(client, nil, &ep.conflicts)
			}
		}
	}

	return nil
}
