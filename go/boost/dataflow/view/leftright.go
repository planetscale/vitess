package view

import (
	"runtime"
	"sync/atomic"
	_ "unsafe"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
)

//go:linkname sync_runtime_procPin sync.runtime_procPin
//go:nosplit
func sync_runtime_procPin() int

//go:linkname sync_runtime_procUnpin sync.runtime_procUnpin
//go:nosplit
func sync_runtime_procUnpin()

const Reading = 1
const NotReading = 0

const Left = -1

type atomicInt64 struct {
	atomic.Int64
	pad [120]byte
}

type leftright struct {
	active        atomic.Int64
	writerVersion atomic.Uint64
	readerVersion [2][]atomicInt64

	left, right offheap.CRowsTable
}

func (lr *leftright) init(maxprocs int) {
	lr.left = make(offheap.CRowsTable)
	lr.right = make(offheap.CRowsTable)
	lr.readerVersion = [2][]atomicInt64{
		make([]atomicInt64, maxprocs),
		make([]atomicInt64, maxprocs),
	}
	lr.active.Swap(Left)
}

func (lr *leftright) readerArrive(version uint64, tid int) {
	v := lr.readerVersion[version&0x1]
	// TODO: all Swaps in this file can be a Store when Go gets its shit together
	// https://github.com/golang/go/issues/58020
	v[tid].Swap(Reading)
}

func (lr *leftright) readerDepart(version uint64, tid int) {
	v := lr.readerVersion[version&0x1]
	v[tid].Swap(NotReading)
}

func (lr *leftright) readerIsEmpty(version uint64) bool {
	v := lr.readerVersion[version&0x1]
	for t := range v {
		if v[t].Load() == Reading {
			return false
		}
	}
	return true
}

func (lr *leftright) Read(callback func(tbl offheap.CRowsTable, version uint64)) {
	tid := sync_runtime_procPin()
	vi := lr.writerVersion.Load()

	lr.readerArrive(vi, tid)

	if lr.active.Load() == Left {
		callback(lr.left, vi)
	} else {
		callback(lr.right, vi)
	}

	lr.readerDepart(vi, tid)
	sync_runtime_procUnpin()
}

func (lr *leftright) publish(active int64, callback func(tbl offheap.CRowsTable)) {
	lr.active.Swap(-active)

	prevVersion := lr.writerVersion.Load()
	nextVersion := prevVersion + 1

	for !lr.readerIsEmpty(nextVersion) {
		runtime.Gosched()
	}
	lr.writerVersion.Swap(nextVersion)

	for !lr.readerIsEmpty(prevVersion) {
		runtime.Gosched()
	}

	if -active == Left {
		callback(lr.right)
	} else {
		callback(lr.left)
	}
}

func (lr *leftright) Publish(callback func(tbl offheap.CRowsTable)) {
	lr.publish(lr.active.Load(), callback)
}

func (lr *leftright) Write(callback func(tbl offheap.CRowsTable)) {
	active := lr.active.Load()
	if active == Left {
		callback(lr.right)
	} else {
		callback(lr.left)
	}
	lr.publish(active, callback)
}

func (lr *leftright) Writer() offheap.CRowsTable {
	if lr.active.Load() == Left {
		return lr.right
	}
	return lr.left
}
