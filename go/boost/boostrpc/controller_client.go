package boostrpc

import (
	"vitess.io/vitess/go/boost/boostrpc/drpc"
	"vitess.io/vitess/go/boost/boostrpc/drpc/middleware/client"
	"vitess.io/vitess/go/vt/proto/vtboost"
)

func NewControllerClient(address string) (vtboost.DRPCControllerServiceClient, error) {
	conn, err := NewClientConn(address)
	if err != nil {
		return nil, err
	}
	return vtboost.NewDRPCControllerServiceClient(conn), nil
}

func NewClientConn(address string) (*drpc.Client, error) {
	// TODO: TLS when needed
	clientOptions := []drpc.ClientOption{
		drpc.WithPoolCapacity(5),
		drpc.WithClientMiddleware(
			client.PanicRecovery(),
		),
	}
	return drpc.NewClient("tcp", address, clientOptions...)
}
