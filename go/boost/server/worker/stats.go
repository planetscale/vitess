package worker

import (
	"github.com/google/uuid"

	"vitess.io/vitess/go/stats"
)

var (
	StatViewReads     = stats.NewCountersWithSingleLabel("BoostWorkerRead", "The total number of viewRead calls executed", "Worker")
	StatVStreamRows   = stats.NewCountersWithSingleLabel("BoostWorkerVStreamRows", "The total number of rows processed in the VStream", "Worker")
	StatVStreamFields = stats.NewCountersWithSingleLabel("BoostWorkerVStreamFields", "The total number FIELD events processed by the VStream", "Worker")
	StatVStreamError  = stats.NewCountersWithSingleLabel("BoostWorkerVStreamError", "The total number of errors encountered while processing the VStream", "Worker")
)

type workerStats struct {
	worker string
}

func (s *workerStats) onRead() {
	StatViewReads.Add(s.worker, 1)
}

func (s *workerStats) onVStreamRows(count int) {
	StatVStreamRows.Add(s.worker, int64(count))
}

func (s *workerStats) onVStreamFields() {
	StatVStreamFields.Add(s.worker, 1)
}

func (s *workerStats) onVStreamError() {
	StatVStreamError.Add(s.worker, 1)
}

func newWorkerStats(uuid uuid.UUID) *workerStats {
	return &workerStats{worker: uuid.String()}
}
