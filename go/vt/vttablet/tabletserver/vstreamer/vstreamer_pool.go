package vstreamer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

func (vs *vstreamer) isPoolEnabled() bool {
	return true //vs.config.ExperimentalFlags&vttablet.VReplicationExperimentalFlagVStreamerPooling != 0
}

type vstreamerPoolConnection struct {
	stream     *vstreamPoolStream
	startPos   *replication.Position
	lastEvent  *cachedEvent
	ch         chan mysql.BinlogEvent
	formatSent bool
}

func (c vstreamerPoolConnection) Stream() (events <-chan mysql.BinlogEvent, errs <-chan error) {
	c.ch = make(chan mysql.BinlogEvent, 100)
	go func() {
		log.Infof(">>>>>>>>>>> POOL:  vstreamerPoolConnection: Starting stream")
		firstEvent, ok := c.stream.eventsMap[c.startPos.GTIDSet.String()]
		if !ok {
			//FIXME: handle error
			log.Infof(">>>>>>>>>>> POOL:  start position %s not found in stream", c.startPos.GTIDSet.String())
			log.Flush()
			//panic("start position not found in stream")
			close(c.ch)
			return
		} else {
			log.Infof(">>>>>>>>>>> POOL:  start position %d, %v found in stream,next %v", firstEvent.idx, firstEvent.gtid, firstEvent.nextEvent)
		}
		found := false
		for nextEvent := firstEvent.nextEvent; nextEvent != nil; nextEvent = nextEvent.nextEvent {
			log.Infof(">>>>>>>>>>> POOL:  Checking next event %+v", nextEvent)
			if nextEvent.isGTID {
				firstEvent = nextEvent
				found = true
				log.Infof(">>>>>>>>>>> POOL:  next GTID event found in stream %s", firstEvent.gtid.String())
				break
			}
		}
		if !found {
			log.Infof(">>>>>>>>>>> POOL:  GTID event not found in stream")
			close(c.ch)
			return
		}
		log.Infof(">>>>>>>>>>> POOL:  Starting stream from %s, firstEvent %+v", c.startPos.GTIDSet.String(), firstEvent)
		var currEvent *cachedEvent
		for {
			// FIXME: add check for context done
			idle := false
			log.Infof(">>>>>>>>>>> POOL:  Checking for new events")
			c.stream.mu.Lock()
			if c.lastEvent == nil {
				log.Infof(">>>>>>>>>>> POOL:  lastEvent is nil")
				currEvent = firstEvent
			} else {
				log.Infof(">>>>>>>>>>> POOL:  lastEvent is not nil %v", c.lastEvent.idx)
				if c.lastEvent.nextEvent != nil {
					log.Infof(">>>>>>>>>>> POOL:  lastEvent.nextEvent is not nil %v", c.lastEvent.nextEvent.idx)
					currEvent = c.lastEvent.nextEvent
				} else {
					log.Infof(">>>>>>>>>>> POOL:  lastEvent.nextEvent is nil, marking idle")
					idle = true
				}

			}
			log.Infof(">>>>>>>>>>> POOL:  Idle: %v", idle)
			if !idle {
				for currEvent != nil {
					if c.lastEvent != nil {
						log.Infof(">>>>>>>>>>> POOL:  Setting lastEvent.nextEvent to %+v", currEvent.idx)
						c.lastEvent.nextEvent = currEvent
						c.lastEvent = currEvent
					} else {
						log.Infof(">>>>>>>>>>> POOL:  Setting lastEvent to %+v", currEvent.idx)
						c.lastEvent = currEvent
					}
					if !c.formatSent {
						c.ch <- *c.stream.formatEvent
						c.formatSent = true
					}
					log.Infof(">>>>>>>>>>> POOL:  Sending event %+v", *currEvent)
					c.ch <- *currEvent.event
					currEvent = currEvent.nextEvent
				}
			}
			c.stream.mu.Unlock()
			// log.Infof(">>>>>>>>>>> POOL:  Idle, sleeping for 1 second")
			time.Sleep(1 * time.Second)
		}
	}()
	return c.ch, c.stream.errs
}

type cachedEvent struct {
	event     *mysql.BinlogEvent
	isGTID    bool
	gtid      replication.GTIDSet // probably redundant, but useful for debugging.
	nextEvent *cachedEvent
	idx       int
}

type vstreamPoolStream struct {
	mu            sync.Mutex
	events        []*cachedEvent
	eventsMap     map[string]*cachedEvent
	start         *replication.Position
	head          *replication.Position
	tail          *replication.Position
	conns         []*vstreamerPoolConnection
	errs          <-chan error
	format        mysql.BinlogFormat
	formatEvent   *mysql.BinlogEvent
	currentGTID   replication.GTID
	waitCondition *sync.Cond
	lastId        int
}

func newVStreamPoolStream() *vstreamPoolStream {
	vps := &vstreamPoolStream{
		eventsMap:     make(map[string]*cachedEvent),
		waitCondition: sync.NewCond(&sync.Mutex{}),
	}
	return vps
}

func (s *vstreamPoolStream) push(event *mysql.BinlogEvent) *cachedEvent {

	var cachedEv cachedEvent
	cachedEv.event = event
	if (*event).IsGTID() {
		cachedEv.isGTID = true
		gtid, _, _ := (*event).GTID(s.format)
		cachedEv.gtid = gtid.GTIDSet()
	}
	cachedEv.idx = s.lastId
	s.lastId++
	if len(s.events) > 0 {
		s.events[len(s.events)-1].nextEvent = &cachedEv
	}
	s.events = append(s.events, &cachedEv)
	if cachedEv.isGTID {
		s.eventsMap[cachedEv.gtid.String()] = &cachedEv
	}
	log.Infof(">>>>>>>>>>> POOL:  Pushed event %v, %v", cachedEv.idx, cachedEv.gtid)
	return &cachedEv
}

func (s *vstreamPoolStream) startStreaming(ctx context.Context, cp dbconfigs.Connector, pos *replication.Position) error {
	s.start = pos
	conn, err := binlog.NewBinlogConnection(cp)
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Infof(">>>>>>>>>>> POOL:  startStreaming: Starting StartBinlogDumpFromPosition at %v", pos)
	events, errs, err := conn.StartBinlogDumpFromPosition(ctx, "", *pos)
	if err != nil {
		return err
	}
	gtid := pos.GTIDSet
	got := 0
	signalled := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case err2, ok := <-errs:
			gtid = nil
			if !ok {
				return nil
			}
			if err2 != nil {
				return err
			}
		case ev, ok := <-events:
			log.Infof(">>>>>>>>>>> POOL:  Event received: %t:%t:%t", ev.IsFormatDescription(), ev.IsGTID(), ev.IsXID())
			if !ok {
				return nil
			}
			s.mu.Lock()
			switch {
			case ev.IsFormatDescription():
				s.formatEvent = &ev
				s.format, err = ev.Format()
				if err != nil {
					s.mu.Unlock()
					return err
				}
			case ev.IsGTID():
				if s.format.IsZero() {
					s.mu.Unlock()
					return fmt.Errorf("GTID event received before FormatDescription event")
				}
				g, _, err := ev.GTID(s.format)
				gtid = g.GTIDSet()
				if err != nil {
					s.mu.Unlock()
					return err
				}
				log.Infof(">>>>>>>>>>> POOL:  GTID event received, gtid %s", gtid)
				pos2 := &replication.Position{
					GTIDSet: gtid,
				}
				if s.head == nil {
					s.head = pos2
				}
				s.tail = pos2
			case ev.IsRotate():
				if gtid == nil {
					s.mu.Unlock()
					return fmt.Errorf("rotate event received before GTID event")
				}

			default:
				if !ev.IsValid() {
					s.mu.Unlock()
					return fmt.Errorf("invalid event received")
				}
				switch {
				case ev.IsQuery():
					q, err := ev.Query(s.format)
					if err != nil {
						return err
					}
					log.Infof(">>>>>>>>>>> POOL:  Query event received")
					switch cat := sqlparser.Preview(q.SQL); cat {
					case sqlparser.StmtBegin:
						log.Infof(">>>>>>>>>>> POOL:  Begin event received")
					case sqlparser.StmtCommit:
						log.Infof(">>>>>>>>>>> POOL:  Commit event received")
						got++
					default:
						log.Infof(">>>>>>>>>>> POOL:  Query event received, ignoring %s", q.SQL)
					}
				case ev.IsFormatDescription():
					log.Infof(">>>>>>>>>>> POOL:  FormatDescription event received")
				case ev.IsXID():
					log.Infof(">>>>>>>>>>> POOL:  XID event received")
					got++
				case ev.IsTableMap():
					log.Infof(">>>>>>>>>>> POOL:  TableMap event received")
				case ev.IsWriteRows() || ev.IsUpdateRows() || ev.IsDeleteRows():
					log.Infof(">>>>>>>>>>> POOL:  Rows event received")
				}
			}
			//log.Infof(">>>>>>>>>>> POOL:  Caching event %+v", ev)
			cev := s.push(&ev)
			if ev.IsRotate() {
				s.eventsMap[gtid.String()] = cev
			}
			if !signalled && got == 1 {
				log.Infof(">>>>>>>>>>> POOL:  Got 1 commit event, breaking")
				s.waitCondition.L.Lock()
				s.waitCondition.Broadcast()
				s.waitCondition.L.Unlock()
				signalled = true
			}
			s.mu.Unlock()
		}
	}
}

type vstreamerPool struct {
	mu      sync.Mutex
	streams []*vstreamPoolStream
}

var theVStreamerPool *vstreamerPool

func init() {
	theVStreamerPool = newVStreamerPool()
}

func newVStreamerPool() *vstreamerPool {
	log.Infof(">>>>>>>>>>> POOL:  Creating new VStreamer pool")
	return &vstreamerPool{}
}

func (vsp *vstreamerPool) lock() {
	// log.Infof(">>>>>>>>>>> POOL:  vstreamerPool: Locking")
	vsp.mu.Lock()
}

func (vsp *vstreamerPool) unlock() {
	// log.Infof(">>>>>>>>>>> POOL:  vstreamerPool: Unlocking")
	vsp.mu.Unlock()
}

func (vsp *vstreamerPool) find(pos *replication.Position) (*vstreamerPoolConnection, error) {
	for _, stream := range vsp.streams {
		if pos.AtLeast(*stream.head) && stream.tail.AtLeast(*pos) {
			return &vstreamerPoolConnection{
				stream:   stream,
				startPos: pos,
			}, nil
		}
	}
	return nil, nil
}

func (vsp *vstreamerPool) create(vs *vstreamer) (*vstreamerPoolConnection, error) {
	stream := newVStreamPoolStream()
	stream.waitCondition.L.Lock()
	go func() {
		if err := stream.startStreaming(vs.ctx, vs.cp, &vs.pos); err != nil {
			log.Errorf("Error starting streaming: %v", err)
		}
	}()
	stream.waitCondition.Wait()
	stream.waitCondition.L.Unlock()
	log.Infof(">>>>>>>>>>> POOL:  vstreamerPool: Created new stream, eventsMap %+v", stream.eventsMap)
	vsp.streams = append(vsp.streams, stream)
	return &vstreamerPoolConnection{
		stream:   stream,
		startPos: &vs.pos,
	}, nil
}

func (vsp *vstreamerPool) Get(vs *vstreamer) (*vstreamerPoolConnection, error) {
	vsp.lock()
	defer vsp.unlock()
	log.Infof(">>>>>>>>>>> POOL:  vstreamerPool: Get called with pos %v", vs.pos)
	conn, err := vsp.find(&vs.pos)
	if err != nil {
		return nil, err
	}
	log.Infof(">>>>>>>>>>> POOL:  vstreamerPool: Found connection %v", conn)
	if conn == nil {
		log.Infof(">>>>>>>>>>> POOL:  vstreamerPool: Connection not found, creating new connection")
		conn, err = vsp.create(vs)
		log.Infof(">>>>>>>>>>> POOL:  vstreamerPool: Created connection %v, err %v", conn, err)
		if err != nil {
			return nil, err
		}
		if conn == nil {
			return nil, fmt.Errorf("failed to create connection")
		}
	}
	conn.stream.conns = append(conn.stream.conns, conn)
	return conn, nil
}

func (vsp *vstreamerPool) Put(conn *vstreamerPoolConnection) {
	vsp.lock()
	defer vsp.unlock()
	for i, c := range conn.stream.conns {
		if c == conn {
			conn.stream.conns = append(conn.stream.conns[:i], conn.stream.conns[i+1:]...)
			break
		}
	}
}
