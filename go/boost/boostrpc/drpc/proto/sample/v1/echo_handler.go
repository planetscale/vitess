package samplev1

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	empty "google.golang.org/protobuf/types/known/emptypb"
)

// Error returns a textual representation of the `FaultyError` instance. Making sure
// `FaultyError` can be used as an `error` value.
func (x *FaultyError) Error() string {
	md := make([]string, len(x.Metadata))
	i := 0
	for k, v := range x.Metadata {
		md[i] = fmt.Sprintf("%s=%s", k, v)
		i++
	}
	return fmt.Sprintf("%d: %s (%s)", x.Code, x.Desc, strings.Join(md, "|"))
}

// toStatus converts a `FaultyError` instance to a grpc.Status compatible error.
// All errors returned by gRPC servers are expected to be of type [grpc.Status].
//
// [grpc.Status]: https://godoc.org/google.golang.org/grpc/status
func (x *FaultyError) toStatus() error {
	st := status.New(codes.Code(x.Code), x.Desc)
	rs, err := st.WithDetails(x)
	if err != nil {
		// When failing to append rich details to the status instance; fallback
		// to returning original error message
		return x
	}
	return rs.Err()
}

// EchoHandler defines a sample implementation of the echo service provider.
type EchoHandler struct {
	UnimplementedEchoAPIServer
}

// Ping provides a sample reachability test method.
func (eh *EchoHandler) Ping(_ context.Context, _ *empty.Empty) (*Pong, error) {
	return &Pong{Ok: true}, nil
}

// Health provides a sample health check method.
func (eh *EchoHandler) Health(_ context.Context, _ *empty.Empty) (*HealthResponse, error) {
	return &HealthResponse{Alive: true}, nil
}

// Echo will process the incoming echo operation.
func (eh *EchoHandler) Echo(_ context.Context, req *EchoRequest) (*EchoResponse, error) {
	return &EchoResponse{
		Result: fmt.Sprintf("you said: %s", req.Value),
	}, nil
}

// Faulty will return an error about 20% of the time.
func (eh *EchoHandler) Faulty(_ context.Context, _ *empty.Empty) (*DummyResponse, error) {
	if x := ri(1, 9); x == 2 || x == 4 {
		// Return a custom error type
		err := &FaultyError{
			Code: uint32(codes.InvalidArgument),
			Desc: "dummy error",
			Metadata: map[string]string{
				"foo":     "bar",
				"x-value": fmt.Sprintf("%d", x),
			},
		}
		return nil, err.toStatus()
	}
	return &DummyResponse{Ok: true}, nil
}

// Slow will report a random latency between 10 to 200ms.
func (eh *EchoHandler) Slow(_ context.Context, _ *empty.Empty) (*DummyResponse, error) {
	time.Sleep(time.Duration(ri(10, 200)) * time.Millisecond)
	return &DummyResponse{Ok: true}, nil
}
