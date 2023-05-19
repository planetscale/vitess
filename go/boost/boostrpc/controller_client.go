package boostrpc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"vitess.io/vitess/go/vt/proto/vtboost"
)

func NewControllerClient(address string) (vtboost.ControllerServiceClient, *grpc.ClientConn, error) {
	conn, err := NewClientConn(address)
	if err != nil {
		return nil, nil, err
	}
	return vtboost.NewControllerServiceClient(conn), conn, nil
}

func NewClientConn(address string) (*grpc.ClientConn, error) {
	return grpc.Dial(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(ProtoCodec{})),
	)
}

func NewServer() *grpc.Server {
	return grpc.NewServer(grpc.ForceServerCodec(ProtoCodec{}))
}
