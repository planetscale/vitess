package domainrpc

import (
	"net"
	"time"

	"storj.io/drpc/drpcconn"

	"vitess.io/vitess/go/boost/boostpb"
)

// WorkerID is an UUID identifying this worker
type WorkerID string

type Worker struct {
	UUID          WorkerID
	Healthy       bool
	Client        boostpb.DRPCWorkerServiceClient
	LastHeartbeat time.Time

	addr string
}

func NewWorker(id WorkerID, addr string) (*Worker, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Worker{
		UUID:          id,
		Healthy:       true,
		Client:        boostpb.NewDRPCWorkerServiceClient(drpcconn.New(conn)),
		LastHeartbeat: time.Now(),
		addr:          addr,
	}, nil
}
