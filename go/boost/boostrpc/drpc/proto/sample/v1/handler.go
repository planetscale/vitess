package samplev1

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log"
	"math/big"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	empty "google.golang.org/protobuf/types/known/emptypb"
)

// Handler provides a simple test server.
type Handler struct {
	UnimplementedBarAPIServer
	UnimplementedFooAPIServer
	Name string
}

// Ping provides a sample reachability test method.
func (s *Handler) Ping(_ context.Context, _ *empty.Empty) (*Pong, error) {
	return &Pong{Ok: true}, nil
}

// Health provides a sample health check method.
func (s *Handler) Health(_ context.Context, _ *empty.Empty) (*HealthResponse, error) {
	return &HealthResponse{Alive: true}, nil
}

// Request provides a sample request handler. If a `sticky-metadata` value is provided
// it will be returned to the client.
func (s *Handler) Request(ctx context.Context, _ *empty.Empty) (*Response, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		log.Println("== request metadata ==")
		for k, v := range md {
			log.Printf("%s: %s\n", k, v)
		}

		// Send `sticky-metadata` back to the client
		if sm := md.Get("sticky-metadata"); len(sm) > 0 {
			clientMD := metadata.New(map[string]string{
				"sticky-metadata": sm[0],
			})
			if err := grpc.SendHeader(ctx, clientMD); err != nil {
				return nil, err
			}
		}
	}
	return &Response{Name: s.Name}, nil
}

// OpenServerStream starts a streaming operation on the server side.
func (s *Handler) OpenServerStream(_ *empty.Empty, stream FooAPI_OpenServerStreamServer) error {
	for i := 0; i < 10; i++ {
		t := <-time.After(100 * time.Millisecond) // random latency
		c := &GenericStreamChunk{
			Sender: s.Name,
			Stamp:  t.Unix(),
		}
		if err := stream.Send(c); err != nil {
			return err
		}
	}
	return nil
}

// OpenClientStream starts a streaming operation on the client side.
func (s *Handler) OpenClientStream(stream FooAPI_OpenClientStreamServer) (err error) {
	res := &StreamResult{Received: 0}
	for {
		_, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(res)
		}
		if err != nil {
			return err
		}
		res.Received++
	}
}

// Faulty will return an error about 20% of the time.
func (s *Handler) Faulty(_ context.Context, _ *empty.Empty) (*DummyResponse, error) {
	if x := ri(1, 9); x == 2 || x == 4 {
		return nil, status.Error(codes.Internal, "dummy error")
	}
	return &DummyResponse{Ok: true}, nil
}

// Slow will report a random latency between 10 to 200ms.
func (s *Handler) Slow(_ context.Context, _ *empty.Empty) (*DummyResponse, error) {
	time.Sleep(time.Duration(ri(10, 200)) * time.Millisecond)
	return &DummyResponse{Ok: true}, nil
}

func ri(min, max int) int {
	m := big.NewInt(int64(max))
	r, err := rand.Int(rand.Reader, m)
	if err != nil {
		return 0 + min
	}
	res := int(r.Int64()) + min
	if res > max {
		res = max
	}
	return res
}
