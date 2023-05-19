package domainrpc

import (
	"time"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/boost/boostrpc"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"

	"vitess.io/vitess/go/boost/boostrpc/service"
)

// WorkerID is an UUID identifying this worker
type WorkerID string

type Worker struct {
	UUID          WorkerID
	Healthy       bool
	Client        service.WorkerServiceClient
	LastHeartbeat time.Time

	entry *vtboostpb.TopoWorkerEntry
	conn  *grpc.ClientConn
}

func (w *Worker) Update(entry *vtboostpb.TopoWorkerEntry) error {
	w.LastHeartbeat = time.Now()

	if entry.AdminAddr != w.entry.AdminAddr {
		_ = w.conn.Close()
		if err := w.connectToAdmin(entry.AdminAddr); err != nil {
			return err
		}
	}
	w.entry = entry
	return nil
}

func (w *Worker) Close() error {
	return w.conn.Close()
}

func (w *Worker) ReaderAddr() string {
	return w.entry.GetReaderAddr()
}

func (w *Worker) connectToAdmin(addr string) (err error) {
	w.conn, err = boostrpc.NewClientConn(addr)
	if err != nil {
		w.Healthy = false
		return err
	}

	w.Client = service.NewWorkerServiceClient(w.conn)
	return
}

func NewWorker(id WorkerID, entry *vtboostpb.TopoWorkerEntry) (*Worker, error) {
	w := &Worker{
		UUID:          id,
		Healthy:       true,
		LastHeartbeat: time.Now(),
		entry:         entry,
	}

	return w, w.connectToAdmin(entry.AdminAddr)
}
