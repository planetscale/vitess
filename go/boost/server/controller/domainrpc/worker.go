package domainrpc

import (
	"net"
	"time"

	"storj.io/drpc/drpcconn"

	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"

	"vitess.io/vitess/go/boost/boostrpc/service"
)

// WorkerID is an UUID identifying this worker
type WorkerID string

type Worker struct {
	UUID          WorkerID
	Healthy       bool
	Client        service.DRPCWorkerServiceClient
	LastHeartbeat time.Time

	addr string
}

func (w *Worker) Update(_ *vtboostpb.TopoWorkerEntry) {
	w.LastHeartbeat = time.Now()
}

func NewWorker(id WorkerID, addr string) (*Worker, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Worker{
		UUID:          id,
		Healthy:       true,
		Client:        service.NewDRPCWorkerServiceClient(drpcconn.New(conn)),
		LastHeartbeat: time.Now(),
		addr:          addr,
	}, nil
}
