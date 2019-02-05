/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topo

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"path"
	"sync"

	"github.com/golang/protobuf/proto"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains the utility methods to manage SrvKeyspace objects.

func srvKeyspaceFileName(keyspace string) string {
	return path.Join(KeyspacesPath, keyspace, SrvKeyspaceFile)
}

// WatchSrvKeyspaceData is returned / streamed by WatchSrvKeyspace.
// The WatchSrvKeyspace API guarantees exactly one of Value or Err will be set.
type WatchSrvKeyspaceData struct {
	Value *topodatapb.SrvKeyspace
	Err   error
}

// WatchSrvKeyspace will set a watch on the SrvKeyspace object.
// It has the same contract as Conn.Watch, but it also unpacks the
// contents into a SrvKeyspace object.
func (ts *Server) WatchSrvKeyspace(ctx context.Context, cell, keyspace string) (*WatchSrvKeyspaceData, <-chan *WatchSrvKeyspaceData, CancelFunc) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return &WatchSrvKeyspaceData{Err: err}, nil, nil
	}

	filePath := srvKeyspaceFileName(keyspace)
	current, wdChannel, cancel := conn.Watch(ctx, filePath)
	if current.Err != nil {
		return &WatchSrvKeyspaceData{Err: current.Err}, nil, nil
	}
	value := &topodatapb.SrvKeyspace{}
	if err := proto.Unmarshal(current.Contents, value); err != nil {
		// Cancel the watch, drain channel.
		cancel()
		for range wdChannel {
		}
		return &WatchSrvKeyspaceData{Err: fmt.Errorf("error unpacking initial SrvKeyspace object: %v", err)}, nil, nil
	}

	changes := make(chan *WatchSrvKeyspaceData, 10)

	// The background routine reads any event from the watch channel,
	// translates it, and sends it to the caller.
	// If cancel() is called, the underlying Watch() code will
	// send an ErrInterrupted and then close the channel. We'll
	// just propagate that back to our caller.
	go func() {
		defer close(changes)

		for wd := range wdChannel {
			if wd.Err != nil {
				// Last error value, we're done.
				// wdChannel will be closed right after
				// this, no need to do anything.
				changes <- &WatchSrvKeyspaceData{Err: wd.Err}
				return
			}

			value := &topodatapb.SrvKeyspace{}
			if err := proto.Unmarshal(wd.Contents, value); err != nil {
				cancel()
				for range wdChannel {
				}
				changes <- &WatchSrvKeyspaceData{Err: fmt.Errorf("error unpacking SrvKeyspace object: %v", err)}
				return
			}

			changes <- &WatchSrvKeyspaceData{Value: value}
		}
	}()

	return &WatchSrvKeyspaceData{Value: value}, changes, cancel
}

// GetSrvKeyspaceNames returns the SrvKeyspace objects for a cell.
func (ts *Server) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	children, err := conn.ListDir(ctx, KeyspacesPath, false /*full*/)
	switch {
	case err == nil:
		return DirEntriesToStringArray(children), nil
	case IsErrType(err, NoNode):
		return nil, nil
	default:
		return nil, err
	}
}

// GetShardServingCells returns cells where this shard is serving
func (ts *Server) GetShardServingCells(ctx context.Context, si *ShardInfo) (servingCells []string, err error) {
	cells, err := ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	servingCells = make([]string, len(cells))
	var mu sync.Mutex
	for _, cell := range cells {
		wg.Add(1)
		go func(cell, keyspace string) {
			defer wg.Done()
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, si.keyspace)
			switch {
			case err != nil:
				for _, partition := range srvKeyspace.GetPartitions() {
					for _, shardReference := range partition.ShardReferences {
						if shardReference.GetName() == si.ShardName() {
							mu.Lock()
							defer mu.Unlock()
							servingCells = append(servingCells, cell)
						}
					}
				}
			case IsErrType(err, NoNode):
				// NOOP
				return
			default:
				rec.RecordError(err)
				return
			}
		}(cell, si.Keyspace())
	}
	wg.Wait()
	if rec.HasErrors() {
		return nil, NewError(PartialResult, rec.Error().Error())
	}
	return servingCells, nil
}

// GetShardServingTypes returns served types for given shard across all cells
func (ts *Server) GetShardServingTypes(ctx context.Context, si *ShardInfo) (servingTypes []topodatapb.TabletType, err error) {
	cells, err := ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	servingTypes = make([]topodatapb.TabletType, len(cells))
	var mu sync.Mutex
	for _, cell := range cells {
		wg.Add(1)
		go func(cell, keyspace string) {
			defer wg.Done()
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, si.keyspace)
			switch {
			case err != nil:
				for _, partition := range srvKeyspace.GetPartitions() {
					for _, shardReference := range partition.ShardReferences {
						if shardReference.GetName() == si.ShardName() {
							mu.Lock()
							defer mu.Unlock()
							found := false
							for _, servingType := range servingTypes {
								if servingType == partition.ServedType {
									found = true
								}
							}
							if !found {
								servingTypes = append(servingTypes, partition.ServedType)
							}
						}
					}
				}
			case IsErrType(err, NoNode):
				// NOOP
				return
			default:
				rec.RecordError(err)
				return
			}
		}(cell, si.Keyspace())
	}
	wg.Wait()
	if rec.HasErrors() {
		return nil, NewError(PartialResult, rec.Error().Error())
	}
	return servingTypes, nil
}

// RemoveShardServingKeyspace ...
func (ts *Server) RemoveShardServingKeyspace(ctx context.Context, si *ShardInfo, cells []string) (err error) {
	if err = CheckKeyspaceLocked(ctx, si.keyspace); err != nil {
		return err
	}

	// The caller intents to update all cells in this case
	if len(cells) == 0 {
		cells, err = ts.GetCellInfoNames(ctx)
		if err != nil {
			return err
		}
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell, keyspace string) {
			defer wg.Done()
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, si.keyspace)
			switch {
			case err == nil:
				shardReferences := make([]*topodatapb.ShardReference, 0)
				for _, partition := range srvKeyspace.GetPartitions() {

					for _, shardReference := range partition.ShardReferences {
						if shardReference.GetName() != si.ShardName() {
							shardReferences = append(shardReferences, shardReference)
						}
					}
					if err := OrderAndCheckPartitions(cell, srvKeyspace); err != nil {
						rec.RecordError(err)
						return
					}

				}

				if err := OrderAndCheckPartitions(cell, srvKeyspace); err != nil {
					rec.RecordError(err)
					return
				}
				err = ts.UpdateSrvKeyspace(ctx, cell, si.keyspace, srvKeyspace)
				if err != nil {
					rec.RecordError(err)
					return
				}
			case IsErrType(err, NoNode):
				// NOOP
			default:
				rec.RecordError(err)
				return
			}
			if err != nil {
				rec.RecordError(err)
				return
			}
		}(cell, si.Keyspace())
	}
	wg.Wait()
	if rec.HasErrors() {
		return NewError(PartialResult, rec.Error().Error())
	}
	return nil
}

// UpdateDisableQueryService will make sure the disableQueryService is
// set appropriately in the shard record. Note we don't support a lot
// of the corner cases:
// - we don't support DisableQueryService at the same time as BlacklistedTables,
//   because it's not used in the same context (vertical vs horizontal sharding)
// This function should be called while holding the keyspace lock.
func (ts *Server) UpdateDisableQueryService(ctx context.Context, si *ShardInfo, tabletType topodatapb.TabletType, cells []string, disableQueryService bool) (err error) {
	if err = CheckKeyspaceLocked(ctx, si.keyspace); err != nil {
		return err
	}
	tc := si.GetTabletControl(tabletType)
	// we have an existing record, check table list is empty and
	// DisableQueryService is set
	if tc != nil && len(tc.BlacklistedTables) > 0 {
		return fmt.Errorf("cannot safely alter DisableQueryService as BlacklistedTables is set")
	}

	// The caller intents to update all cells in this case
	if len(cells) == 0 {
		cells, err = ts.GetCellInfoNames(ctx)
		if err != nil {
			return err
		}
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell, keyspace string) {
			defer wg.Done()
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, si.keyspace)
			if err != nil {
				rec.RecordError(err)
				return
			}
			for _, partition := range srvKeyspace.GetPartitions() {
				if partition.GetServedType() != tabletType {
					continue
				}
				found := false
				for _, tabletControl := range partition.GetShardTabletControls() {
					if tabletControl.GetName() == si.ShardName() {
						found = true
						tabletControl.QueryServiceDisabled = disableQueryService
					}
				}

				if !found {
					shardTabletControl := &topodatapb.ShardTabletControl{
						Name:                 si.ShardName(),
						KeyRange:             si.KeyRange,
						QueryServiceDisabled: true,
					}
					partition.ShardTabletControls = append(partition.GetShardTabletControls(), shardTabletControl)
				}
			}
			err = ts.UpdateSrvKeyspace(ctx, cell, si.keyspace, srvKeyspace)
			if err != nil {
				rec.RecordError(err)
				return
			}

		}(cell, si.Keyspace())
	}
	wg.Wait()
	if rec.HasErrors() {
		return NewError(PartialResult, rec.Error().Error())
	}
	return nil
}

// UpdateSrvKeyspace saves a new SrvKeyspace. It is a blind write.
func (ts *Server) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := srvKeyspaceFileName(keyspace)
	data, err := proto.Marshal(srvKeyspace)
	if err != nil {
		return err
	}
	_, err = conn.Update(ctx, nodePath, data, nil)
	return err
}

// DeleteSrvKeyspace deletes a SrvKeyspace.
func (ts *Server) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := srvKeyspaceFileName(keyspace)
	return conn.Delete(ctx, nodePath, nil)
}

// GetSrvKeyspaceAllCells returns the SrvKeyspace for all cells
func (ts *Server) GetSrvKeyspaceAllCells(ctx context.Context, keyspace string) ([]*topodatapb.SrvKeyspace, error) {
	cells, err := ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, err
	}

	srvKeyspaces := make([]*topodatapb.SrvKeyspace, len(cells))
	for _, cell := range cells {
		srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
		switch {
		case err == nil:
			srvKeyspaces = append(srvKeyspaces, srvKeyspace)
		case IsErrType(err, NoNode):
			// NOOP
		default:
			return srvKeyspaces, err
		}
	}
	return srvKeyspaces, nil
}

// GetSrvKeyspace returns the SrvKeyspace for a cell/keyspace.
func (ts *Server) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	nodePath := srvKeyspaceFileName(keyspace)
	data, _, err := conn.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}
	srvKeyspace := &topodatapb.SrvKeyspace{}
	if err := proto.Unmarshal(data, srvKeyspace); err != nil {
		return nil, fmt.Errorf("SrvKeyspace unmarshal failed: %v %v", data, err)
	}
	return srvKeyspace, nil
}

// OrderAndCheckPartitions will re-order the partition list, and check
// it's correct.
func OrderAndCheckPartitions(cell string, srvKeyspace *topodatapb.SrvKeyspace) error {
	// now check them all
	for _, partition := range srvKeyspace.Partitions {
		tabletType := partition.ServedType
		topoproto.ShardReferenceArray(partition.ShardReferences).Sort()

		// check the first Start is MinKey, the last End is MaxKey,
		// and the values in between match: End[i] == Start[i+1]
		first := partition.ShardReferences[0]
		if first.KeyRange != nil && len(first.KeyRange.Start) != 0 {
			return fmt.Errorf("keyspace partition for %v in cell %v does not start with min key", tabletType, cell)
		}
		last := partition.ShardReferences[len(partition.ShardReferences)-1]
		if last.KeyRange != nil && len(last.KeyRange.End) != 0 {
			return fmt.Errorf("keyspace partition for %v in cell %v does not end with max key", tabletType, cell)
		}
		for i := range partition.ShardReferences[0 : len(partition.ShardReferences)-1] {
			currShard := partition.ShardReferences[i]
			nextShard := partition.ShardReferences[i+1]
			currHasKeyRange := currShard.KeyRange != nil
			nextHasKeyRange := nextShard.KeyRange != nil
			if currHasKeyRange != nextHasKeyRange {
				return fmt.Errorf("shards with inconsistent KeyRanges for %v in cell %v. shards: %v, %v", tabletType, cell, currShard, nextShard)
			}
			if !currHasKeyRange {
				// this is the custom sharding case, all KeyRanges must be nil
				continue
			}
			if bytes.Compare(currShard.KeyRange.End, nextShard.KeyRange.Start) != 0 {
				return fmt.Errorf("non-contiguous KeyRange values for %v in cell %v at shard %v to %v: %v != %v", tabletType, cell, i, i+1, hex.EncodeToString(currShard.KeyRange.End), hex.EncodeToString(nextShard.KeyRange.Start))
			}
		}
	}

	return nil
}

// ShardIsServing returns true if this shard is found in any of the partitions in the srvKeyspace
func ShardIsServing(srvKeyspace *topodatapb.SrvKeyspace, shard *topodatapb.Shard) bool {
	for _, partition := range srvKeyspace.GetPartitions() {
		for _, shardReference := range partition.GetShardReferences() {
			if shardReference.GetKeyRange() == shard.KeyRange {
				return true
			}
		}
	}
	return false
}
