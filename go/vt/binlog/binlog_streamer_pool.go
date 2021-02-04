package binlog

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
)

/*
todos:
* metrics for skipped events for testing
* pool size: reject >
* METRICS:
 */

var theStreamerPool *streamerPool

type streamerPool struct {
	producers []*producer
	mu        sync.Mutex
}

func init() {
	theStreamerPool = &streamerPool{}
}

func dbg(s string) {
	log.Infof("BSP: %s", s)
	log.Flush()
}

func (pool *streamerPool) get(ctx context.Context, bc IBinlogConnection, pos mysql.Position) (mysql.Position, <-chan mysql.BinlogEvent, func(), error) {
	var prod *producer
	var err error
	pool.mu.Lock()
	defer pool.mu.Unlock()

	// want to stream from "current" position, so join a random producer
	if pos.IsZero() {
		if len(pool.producers) > 0 {
			prod = pool.producers[rand.Intn(len(pool.producers))]  // rand is seeded when tabletserver comes up
			return prod.addConsumer(ctx, pos)
		}
	}

	prod, err = pool.findClosestProducer(ctx, pos)
	if err != nil {
		return mysql.Position{}, nil, nil, err
	}
	if prod == nil {
		prod, err = pool.newProducer(pos, bc)
		if err != nil {
			return mysql.Position{}, nil, nil, err
		}
	}
	return prod.addConsumer(ctx, pos)
}

type StreamStatus int

const (
	StreamNotStarted StreamStatus = 0
	StreamStarted                 = 1
)

type consumer struct {
	ctx                  context.Context
	startPos, currentPos mysql.Position
	eventCh              chan mysql.BinlogEvent
	mu                   sync.Mutex
	state                StreamStatus
}

type producer struct {
	ctx      context.Context
	bc       IBinlogConnection
	mu       sync.Mutex
	wg       sync.WaitGroup
	eventCh  chan mysql.BinlogEvent
	errCh    chan error
	cancel   func()
	startPos mysql.Position
	format   mysql.BinlogFormat

	consumers  []*consumer
	currentPos mysql.Position
}

func (pool *streamerPool) newProducer(startPos mysql.Position, bc IBinlogConnection) (*producer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	conn, err := connectForReplication(bc.getConnector())
	if err != nil {
		dbg(err.Error())
		return nil, err
	}
	bc.setConnection(conn)
	dbg(fmt.Sprintf("got connection %v", conn))

	if startPos.IsZero() {
		startPos, err = conn.MasterPosition()
		if err != nil {
			dbg(err.Error())
			return nil, fmt.Errorf("failed to get master position: %v", err)
		}
	}

	prod := &producer{
		startPos: startPos,
		ctx:      ctx,
		cancel:   cancel,
		bc:       bc,
		eventCh:  make(chan mysql.BinlogEvent),
		errCh:    make(chan error),
	}
	pool.producers = append(pool.producers, prod)
	dbg("Before running go routines")
	prod.currentPos = startPos
	if err := bc.SendBinlogDumpCommand(bc.getServerID(), startPos); err != nil {
		dbg(fmt.Sprintf("couldn't send binlog dump command: %v", err))
		return nil, err
	}
	go prod.stream()
	go prod.propagate()
	go prod.pollHealth()
	return prod, nil
}

func (pool *streamerPool) findClosestProducer(ctx context.Context, pos mysql.Position) (*producer, error) {
	var closest *producer
	var minDistance int64 = math.MaxInt64
	for _, prod := range pool.producers {
		distance, err := pos.GTIDSet.Distance(prod.currentPos.GTIDSet)
		if err != nil {
			return nil, err
		}
		if distance < minDistance {
			minDistance = distance
			closest = prod
		}
	}
	return closest, nil
}

func (prod *producer) addConsumer(ctx context.Context, pos mysql.Position) (mysql.Position, <-chan mysql.BinlogEvent, func(), error) {
	cons := &consumer{
		ctx:        ctx,
		startPos:   pos,
		currentPos: mysql.Position{},
		eventCh:    make(chan mysql.BinlogEvent),
		state:      StreamNotStarted,
	}
	dbg("Locking prod")
	prod.mu.Lock()
	defer prod.mu.Unlock()
	prod.consumers = append(prod.consumers, cons)
	dbg(fmt.Sprintf("Adding consumer %v to producer %v starting with gtid %v", cons, prod, pos))
	close := func() {
		prod.removeConsumer(cons)
	}
	return cons.currentPos, cons.eventCh, close, nil
}

const eventPropagationTimeout = 1 * time.Second

func (prod *producer) splitConsumer(cons *consumer) {
	dbg("Locking pool")
	theStreamerPool.mu.Lock()
	defer theStreamerPool.mu.Unlock()
	prod.mu.Lock()
	defer prod.mu.Unlock()
	var consumers []*consumer
	for _, cons2 := range prod.consumers {
		if cons2 != cons {
			consumers = append(consumers, cons)
		}
	}
	prod.consumers = consumers
	newProd, err := theStreamerPool.newProducer(prod.currentPos, prod.bc)
	if err != nil {
		close(cons.eventCh)
		return
	}
	dbg("Locking new prod")
	newProd.mu.Lock()
	defer newProd.mu.Unlock()
	newProd.consumers = append(newProd.consumers, cons)
}

func (pool *streamerPool) removeProducer(prod *producer) {
	dbg("Locking pool")
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if len(prod.consumers) > 0 {
		// new consumer joined
		return
	}
	var producers []*producer
	for _, prod2 := range theStreamerPool.producers {
		if prod2 != prod {
			producers = append(producers, prod2)
		}
	}
	pool.producers = producers
}

func (prod *producer) removeConsumer(cons *consumer) {
	dbg("Locking prod")
	prod.mu.Lock()
	defer prod.mu.Unlock()
	var consumers []*consumer
	close(cons.eventCh)
	for _, cons2 := range prod.consumers {
		if cons2 != cons {
			consumers = append(consumers, cons)
		}
	}
	if len(consumers) == 0 {
		prod.teardown()
	} else {
		prod.consumers = consumers
	}
}

func (prod *producer) teardown() { //FIXME: do we need all of this?!
	dbg("Locking prod")
	prod.mu.Lock()
	defer prod.mu.Unlock()
	select {
	case <-prod.ctx.Done():
		return
	default:
	}

	dbg("td 44444444444444444")
	prod.cancel()

	dbg(fmt.Sprintf("teardown() is being called for prod %v", prod))
	dbg("td 1111111111111111")
	close(prod.eventCh)
	dbg(fmt.Sprintf("td 22222222222222 prod.errCh %v", prod.errCh))
	close(prod.errCh)
	dbg("td 3333333333333333")
	for _, cons := range prod.consumers {
		close(cons.eventCh)
	}
	dbg("td 55555555555555555")
	prod.bc.closeConnection()
	dbg("td 6666666666666666")
	theStreamerPool.removeProducer(prod)
	dbg("td 7777777777777777")
}

func (prod *producer) pollHealth() {
	select {
	case <-prod.ctx.Done():
		return
	case err := <-prod.errCh:
		dbg(fmt.Sprintf("producer got error %v, tearing down", err))
		prod.teardown()
	}
}

func (prod *producer) propagate() {
	log.Infof("In propagate for %v, waiting for event channel", prod, prod.eventCh)
	for {
		select {
		case <-prod.ctx.Done():
			return
		case event := <-prod.eventCh:
			dbg(fmt.Sprintf("Found event in propagate %v", event))
			gtid, mustsend, err := prod.parseEvent(event)
			if err != nil {
				dbg(fmt.Sprintf("Error parsing propagated event, error %v, errCh %v", err, prod.errCh))
				prod.errCh <- err
				return
			}
			dbg(fmt.Sprintf("got gtid %v", gtid))
			if gtid != nil {
				prod.currentPos = mysql.AppendGTID(prod.currentPos, gtid)
			}
			prod.wg.Add(len(prod.consumers))

			for _, cons := range prod.consumers {
				prod.wg.Add(1)
				// process events concurrently
				go func(cons2 *consumer) {
					defer prod.wg.Done()
					if cons2.state == StreamNotStarted {
						dbg(fmt.Sprintf("stream not started startPos %v currentPos %v", prod.startPos, prod.currentPos))
						if gtid != nil && prod.currentPos.AtLeast(prod.startPos) {
							dbg("setting stream status to StreamStarted")
							cons2.state = StreamStarted
						} else {
							if gtid != nil && prod.startPos.AtLeast(prod.currentPos) { // sanity check
								dbg("found event past or equal startPos, should never occur")
								prod.removeConsumer(cons2)
								return
							}
							dbg("not yet at start position ")
							if !mustsend {
								dbg("ignoring event since we are not yet at start position")
								return // not yet at start pos
							} else {
								dbg("sending mandatory event though we are not yet at start position")
							}
						}
					}
					dbg(fmt.Sprintf("propagating event %v into consumer eventCh %v", event, cons2.eventCh))
					t := time.NewTimer(eventPropagationTimeout)
					select {
					case cons2.eventCh <- event:
					case <-prod.ctx.Done():
						return
					case <-t.C:
						dbg("timer ticked, removing consumer")
						prod.removeConsumer(cons2) // stream did not process event in time
					}
				}(cons)
			}
			prod.wg.Wait()
		}
	}
}

func (prod *producer) stream() {
	dbg(fmt.Sprintf("Starting streaming for prod %v", prod))

	for {
		dbg("start of stream() from loop: Reading event")
		event, err := prod.bc.ReadBinlogEvent()
		dbg(fmt.Sprintf("found event %v", event))
		if err != nil {
			if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Number() == mysql.CRServerLost {
				// CRServerLost = Lost connection to MySQL server during query
				// This is not necessarily an error. It could just be that we closed
				// the connection from outside.
				dbg(fmt.Sprintf("connection closed during binlog stream (possibly intentional): %v", err))
			}
			dbg(fmt.Sprintf("read error while streaming binlog events: %v", err))
			prod.errCh <- err
			return
		}

		select {
		case <-prod.ctx.Done():
			return
		default:
			dbg(fmt.Sprintf("sending event into prod eventCh %v: event %v", prod.eventCh, event))
			prod.eventCh <- event
			dbg("Done sending event into prod eventCh")
		}
		dbg("End of stream() for loop")
	}
}

func (prod *producer) parseEvent(event mysql.BinlogEvent) (mysql.GTID, bool, error) {
	if !event.IsValid() {
		dbg("got invalid event")
		return nil, false, fmt.Errorf("can't parse binlog event: invalid data: %#v", event)
	}

	// We need to keep checking for FORMAT_DESCRIPTION_EVENT even after we've
	// seen one, because another one might come along (e.g. on log rotate due to
	// binlog settings change) that changes the format.
	if event.IsFormatDescription() {
		dbg("found format description")
		var err error
		prod.format, err = event.Format()
		if err != nil {
			dbg("error parsing format description")
			return nil, false, fmt.Errorf("can't parse FORMAT_DESCRIPTION_EVENT: %v, event data: %#v", err, event)
		}
		return nil, true, nil
	}

	// We can't parse anything until we get a FORMAT_DESCRIPTION_EVENT that
	// tells us the size of the event header.
	if prod.format.IsZero() {
		// The only thing that should come before the FORMAT_DESCRIPTION_EVENT
		// is a fake ROTATE_EVENT
		if event.IsRotate() {
			dbg("Rotate event")

			return nil, true, nil
		}
		dbg(fmt.Sprintf(">>> got a real event before FORMAT_DESCRIPTION_EVENT: %v, erroring out", event))
		return nil, false, fmt.Errorf("got a real event before FORMAT_DESCRIPTION_EVENT: %#v", event)
	}

	if event.IsGTID() {
		gtid, _, err := event.GTID(prod.format)
		if err != nil {
			dbg("error getting gtid for gtid event")
			return nil, false, err
		}
		return gtid, false, nil
	}
	return nil, false, nil
}

/*
TODOs: closing connection, mutex locks, garbage collection, cache/timeout parameters


*/
