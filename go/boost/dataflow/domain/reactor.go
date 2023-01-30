package domain

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/sql"
)

func (d *Domain) ProcessAsync(ctx context.Context, pkt packet.AsyncPacket) error {
	d.chanPackets <- pkt
	return nil
}

func (d *Domain) WaitForReplay(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.aborted:
		return errDomainAborted
	case <-d.finishedReplays:
		return nil
	}
}

func (d *Domain) ProcessSync(ctx context.Context, pkt packet.SyncPacket) error {
	sync := &syncPacket{
		pkt:  pkt,
		done: make(chan struct{}),
	}
	d.chanSyncPackets <- sync

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.aborted:
		return errDomainAborted
	case <-sync.done:
		return nil
	}
}

func (d *Domain) delayForSelf(m packet.AsyncPacket) {
	select {
	case d.chanDelayedPackets <- m:
	default:
		d.log.Error("Domain.delayForSelf: too many queued packets")
	}
}

// errDomainAborted is returned in RPCs when Domain.Reactor is canceled and RPCs
// are in-flight.
var errDomainAborted = errors.New("domain reactor aborted")

func (d *Domain) Reactor(ctx context.Context, executor processing.Executor) {
	// On completion, signal that RPCs should also abort.
	defer close(d.aborted)

	ctx = common.ContextWithLogger(ctx, d.log)
	for ctx.Err() == nil {
		d.reactor(ctx, executor)
	}
}

const CatchPanic = false

func (d *Domain) reactor(ctx context.Context, executor processing.Executor) {
	if CatchPanic {
		defer func() {
			if err := recover(); err != nil {
				d.log.Error("PANIC while processing in reactor", zap.Any("recover", err))
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return

		case pkt := <-d.chanPackets:
			err := d.handle(ctx, pkt, executor)
			if err != nil {
				d.log.Error("failed to handle packet", zap.Error(err))
			}

		case pkt := <-d.chanSyncPackets:
			err := d.handleSync(ctx, pkt.pkt, pkt.done, executor)
			if err != nil {
				d.log.Error("failed to handle sync packet", zap.Error(err))
			}

		case pkt := <-d.chanDelayedPackets:
			err := d.handle(ctx, pkt, executor)
			if err != nil {
				d.log.Error("failed to handle delayed packet", zap.Error(err))
			}

		moreDelayed:
			for {
				select {
				case pkt := <-d.chanDelayedPackets:
					err := d.handle(ctx, pkt, executor)
					if err != nil {
						d.log.Error("failed to handle delayed packet", zap.Error(err))
					}
				default:
					break moreDelayed
				}
			}

		case ts := <-d.chanElapsedReplays:
			type elapsedreplay struct {
				tag        dataflow.Tag
				requesting uint
				keys       map[sql.Row]bool
				unishard   bool
			}

			getElapsedReplay := func(ts tagshard) elapsedreplay {
				rpl := d.bufferedReplayRequests[ts]
				delete(d.bufferedReplayRequests, ts)
				return elapsedreplay{
					tag:        ts.Tag,
					requesting: ts.Shard,
					keys:       rpl.Keys,
					unishard:   rpl.Unishard,
				}
			}

			var elapsedReplays []elapsedreplay
			elapsedReplays = append(elapsedReplays, getElapsedReplay(ts))

		moreElapsed:
			for {
				select {
				case ts := <-d.chanElapsedReplays:
					elapsedReplays = append(elapsedReplays, getElapsedReplay(ts))
				default:
					break moreElapsed
				}
			}

			for _, er := range elapsedReplays {
				err := d.seedAll(ctx, er.tag, er.requesting, er.keys, er.unishard, executor)
				if err != nil {
					d.log.Error("failed to seed queued replays", zap.Error(err))
				}
			}
		}
	}
}

func (d *Domain) handleSync(ctx context.Context, m packet.SyncPacket, done chan struct{}, executor processing.Executor) error {
	var err error

	switch pkt := m.(type) {
	case *packet.ReadyRequest:
		err = d.handleReady(pkt, done)
	case *packet.SetupReplayPathRequest:
		err = d.handleSetupReplayPath(pkt, done)
	default:
		panic(fmt.Sprintf("unsupported SYNC packet: %T", pkt))
	}
	return err
}

func (d *Domain) handle(ctx context.Context, m packet.AsyncPacket, executor processing.Executor) error {
	var err error

	switch pkt := m.(type) {
	case *packet.Message, *packet.Input:
		err = d.dispatch(ctx, packet.ActiveFlowPacket{Inner: pkt.(packet.FlowPacket)}, executor)
	case *packet.ReplayPiece:
		err = d.handleReplay(ctx, pkt, executor)
	case *packet.StartReplayRequest:
		err = d.handleStartReplay(ctx, pkt, executor)
	case *packet.FinishReplayRequest:
		err = d.handleFinishReplay(ctx, pkt, executor)
	case *packet.EvictRequest:
		err = d.handleEvictAny(ctx, pkt, executor)
	case *packet.EvictKeysRequest:
		err = d.handleEvictKeys(ctx, pkt, executor)
	case *packet.PrepareStateRequest:
		err = d.handlePrepareState(ctx, pkt)
	case *packet.UpdateEgressRequest:
		err = d.handleUpdateEgress(pkt)
	case *packet.UpdateSharderRequest:
		err = d.handleUpdateSharder(pkt)
	case *packet.ReaderReplayRequest:
		err = d.handleReaderReplay(ctx, pkt)
	case *packet.PartialReplayRequest:
		err = d.handlePartialReplay(pkt)
	case *packet.AddNodeRequest:
		err = d.handleAddNode(pkt)
	case *packet.RemoveNodesRequest:
		err = d.handleRemoveNodes(pkt)
	default:
		panic(fmt.Sprintf("unsupported packet: %T", pkt))
	}
	return err
}
