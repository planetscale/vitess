package drpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
	"storj.io/drpc"

	clMW "vitess.io/vitess/go/boost/boostrpc/drpc/middleware/client"
	srvMW "vitess.io/vitess/go/boost/boostrpc/drpc/middleware/server"
	sampleV1 "vitess.io/vitess/go/boost/boostrpc/drpc/proto/sample/v1"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestMetadata(t *testing.T) {
	assert := assert.New(t)
	ctx := ContextWithMetadata(context.TODO(), map[string]string{
		"user.id": "user-123",
	})
	md, ok := MetadataFromContext(ctx)
	assert.True(ok, "failed to retrieve metadata")
	assert.Equal(md["user.id"], "user-123", "invalid value")
}

func TestPool(t *testing.T) {
	assert := assert.New(t)

	// uninteresting pool of random integers between 0 and 100
	ri := &pool[int]{
		limit: 2,
		new: func() (int, error) {
			return rand.Intn(100), nil
		},
		free: func(_ int) error {
			return nil
		},
	}

	// initial empty state
	idle, active := ri.Stats()
	assert.Equal(0, idle, "idle state")
	assert.Equal(0, active, "active state")

	// get first: 1 active, 0 idle
	n1, err := ri.Get()
	assert.Nil(err, "get")
	idle, active = ri.Stats()
	assert.Equal(0, idle, "idle state")
	assert.Equal(1, active, "active state")

	// get second: 2 active, 0 idle
	n2, err := ri.Get()
	assert.Nil(err, "get")
	idle, active = ri.Stats()
	assert.Equal(0, idle, "idle state")
	assert.Equal(2, active, "active state")

	// exceed capacity
	_, err = ri.Get()
	assert.NotNil(err, "this should fail")

	// put back items: 0 active, 2 idle
	ri.Put(n1)
	ri.Put(n2)
	idle, active = ri.Stats()
	assert.Equal(2, idle, "idle state")
	assert.Equal(0, active, "active state")

	// drain pool
	for err := range ri.Drain() {
		t.Error(err)
	}
	idle, active = ri.Stats()
	assert.Equal(0, idle, "idle state")
	assert.Equal(0, active, "active state")
}

func TestServer(t *testing.T) {
	assert := assert.New(t)

	// Main logger
	ll, _ := zap.NewDevelopment()

	// Server middleware
	smw := []srvMW.Middleware{
		srvMW.Logging(ll.With(zap.String("component", "server")), nil),
		srvMW.PanicRecovery(),
	}

	// Client middleware
	cmw := []clMW.Middleware{
		clMW.Metadata(map[string]string{"metadata.user": "rick"}),
		clMW.Logging(ll.With(zap.String("component", "client")), nil),
		clMW.PanicRecovery(),
		clMW.RateLimit(10),
	}

	t.Run("WithPort", func(t *testing.T) {
		port := 8000 + int32(rand.Intn(1000))
		// RPC server
		opts := []Option{
			WithPort(port),
			WithServiceProvider(sampleServiceProvider()),
			WithMiddleware(smw...),
		}
		srv, err := NewServer(opts...)
		assert.Nil(err, "new server")
		go func() {
			_ = srv.Start()
		}()

		// Client options
		clOpts := []ClientOption{
			WithPoolCapacity(2),
			WithClientMiddleware(cmw...),
		}

		// Client connection
		cl, err := NewClient("tcp", fmt.Sprintf(":%d", port), clOpts...)
		assert.Nil(err, "client connection")

		// RPC client
		client := sampleV1.NewDRPCFooAPIClient(cl)

		t.Run("Ping", func(t *testing.T) {
			_, err := client.Ping(context.TODO(), &emptypb.Empty{})
			assert.Nil(err, "ping")
		})

		t.Run("Health", func(t *testing.T) {
			_, err := client.Health(context.TODO(), &emptypb.Empty{})
			assert.Nil(err, "health")
		})

		t.Run("RecoverPanic", func(t *testing.T) {
			_, err := client.Faulty(context.TODO(), &emptypb.Empty{})
			assert.NotNil(err, "failed to recover panic")
		})

		t.Run("ConcurrentClients", func(t *testing.T) {
			// Simulate a randomly slow RPC client
			startWorker := func(cl *Client, wg *sync.WaitGroup) {
				wk := sampleV1.NewDRPCFooAPIClient(cl)
				for i := 0; i < 10; i++ {
					<-time.After(time.Duration(rand.Intn(100)) * time.Millisecond)
					_, err := wk.Ping(context.TODO(), &emptypb.Empty{})
					assert.Nil(err)
				}
				wg.Done()
			}

			// Run 'x' number of concurrent RPC clients
			run := func(x int, cl *Client, wg *sync.WaitGroup) {
				wg.Add(x)
				for i := 0; i < x; i++ {
					go startWorker(cl, wg)
				}
			}

			// Start 2 concurrent RPC clients and wait for them
			// to complete
			wg := sync.WaitGroup{}
			run(2, cl, &wg)
			wg.Wait()

			// Verify client state
			assert.False(cl.IsActive(), "client should be inactive")
		})

		// Close client connection
		assert.Nil(cl.Close(), "close client connection")

		// Verify connection pool state
		idle, active := cl.cache.Stats()
		assert.Equal(0, idle, "number of idle connections")
		assert.Equal(0, active, "number of active connections")

		// Stop server
		_ = srv.Stop()
	})

	t.Run("WithUnixSocket", func(t *testing.T) {
		// Temp socket file
		socket := filepath.Join(os.TempDir(), "test-drpc-server.sock")

		// Start server
		srv, err := NewServer(
			WithUnixSocket(socket),
			WithServiceProvider(sampleServiceProvider()),
			WithMiddleware(smw...),
		)
		assert.Nil(err, "new server")
		go func() {
			_ = srv.Start()
		}()

		// Client connection
		cl, err := NewClient("unix", socket)
		assert.Nil(err, "client connection")

		// RPC client
		client := sampleV1.NewDRPCFooAPIClient(cl)
		res, err := client.Ping(context.TODO(), &emptypb.Empty{})
		assert.Nil(err, "ping")
		assert.True(res.Ok, "ping result")

		// Close client connection
		assert.Nil(cl.Close(), "close client connection")

		// Stop server
		_ = srv.Stop()
		_ = os.Remove(socket)
	})

	t.Run("WithTLS", func(t *testing.T) {
		caCert, _ := os.ReadFile("testdata/ca.sample_cer")
		cert, _ := os.ReadFile("testdata/server.sample_cer")
		key, _ := os.ReadFile("testdata/server.sample_key")
		port := 8000 + int32(rand.Intn(1000))

		// RPC server
		opts := []Option{
			WithPort(port),
			WithServiceProvider(sampleServiceProvider()),
			WithMiddleware(smw...),
			WithTLS(ServerTLS{
				Cert:             cert,
				PrivateKey:       key,
				CustomCAs:        [][]byte{caCert},
				IncludeSystemCAs: true,
			}),
		}
		srv, err := NewServer(opts...)
		assert.Nil(err, "new server")
		go func() {
			_ = srv.Start()
		}()

		// Client connection
		cl, err := NewClient("tcp", fmt.Sprintf(":%d", port), WithClientTLS(ClientTLS{
			IncludeSystemCAs: true,
			CustomCAs:        [][]byte{caCert},
			ServerName:       "node-01",
			SkipVerify:       false,
		}))
		assert.Nil(err, "new client")

		// RPC client
		client := sampleV1.NewDRPCFooAPIClient(cl)
		res, err := client.Ping(context.TODO(), &emptypb.Empty{})
		assert.Nil(err, "ping")
		assert.True(res.Ok, "ping result")

		// Close client connection
		assert.Nil(cl.Close(), "close client connection")

		// Stop server
		_ = srv.Stop()
	})

	t.Run("WithHTTP", func(t *testing.T) {
		port := 8000 + int32(rand.Intn(1000))
		// RPC server
		opts := []Option{
			WithPort(port),
			WithServiceProvider(sampleServiceProvider()),
			WithMiddleware(smw...),
			WithHTTP(),
		}
		srv, err := NewServer(opts...)
		assert.Nil(err, "new server")
		go func() {
			_ = srv.Start()
		}()

		// Client connection
		cl, err := NewClient("tcp", fmt.Sprintf(":%d", port), WithProtocolHeader())
		assert.Nil(err, "client connection")

		// RPC client
		client := sampleV1.NewDRPCFooAPIClient(cl)
		res, err := client.Ping(context.TODO(), &emptypb.Empty{})
		assert.Nil(err, "ping")
		assert.True(res.Ok, "ping result")

		// HTTP request
		hr, err := http.Post(fmt.Sprintf("http://localhost:%d/sample.v1.FooAPI/Ping", port), "application/json", strings.NewReader(`{}`))
		assert.Nil(err, "POST request")
		assert.Equal(hr.StatusCode, http.StatusOK, "HTTP status")
		_ = hr.Body.Close()

		// Close client connection
		assert.Nil(cl.Close(), "close client connection")

		// Stop server
		_ = srv.Stop()
	})

	t.Run("WithHTTPAndTLS", func(t *testing.T) {
		caCert, _ := os.ReadFile("testdata/ca.sample_cer")
		cert, _ := os.ReadFile("testdata/server.sample_cer")
		key, _ := os.ReadFile("testdata/server.sample_key")
		port := 8000 + int32(rand.Intn(1000))

		// RPC server
		opts := []Option{
			WithHTTP(),
			WithPort(port),
			WithServiceProvider(sampleServiceProvider()),
			WithMiddleware(smw...),
			WithTLS(ServerTLS{
				Cert:             cert,
				PrivateKey:       key,
				CustomCAs:        [][]byte{caCert},
				IncludeSystemCAs: true,
			}),
		}
		srv, err := NewServer(opts...)
		assert.Nil(err, "new server")
		go func() {
			_ = srv.Start()
		}()

		// Client connection
		clientOpts := []ClientOption{
			WithProtocolHeader(),
			WithClientTLS(ClientTLS{
				IncludeSystemCAs: true,
				CustomCAs:        [][]byte{caCert},
				ServerName:       "node-01",
				SkipVerify:       false,
			}),
		}
		cl, err := NewClient("tcp", fmt.Sprintf(":%d", port), clientOpts...)
		assert.Nil(err, "new client")

		// RPC request
		client := sampleV1.NewDRPCFooAPIClient(cl)
		res, err := client.Ping(context.TODO(), &emptypb.Empty{})
		assert.Nil(err, "ping")
		assert.True(res.Ok, "ping result")

		// HTTP request
		hcl := getHTTPClient()
		hr, err := hcl.Post(fmt.Sprintf("https://localhost:%d/sample.v1.FooAPI/Ping", port), "application/json", strings.NewReader(`{}`))
		assert.Nil(err, "POST request")
		assert.Equal(hr.StatusCode, http.StatusOK, "HTTP status")
		_ = hr.Body.Close()

		// Close client connection
		assert.Nil(cl.Close(), "close client connection")

		// Stop server
		_ = srv.Stop()
	})

	t.Run("WithAuth", func(t *testing.T) {
		// Auth middleware
		auth := srvMW.AuthByToken("auth.token", func(token string) bool {
			return token == "super-secure-credentials"
		})

		port := 8000 + int32(rand.Intn(1000))

		// RPC server
		opts := []Option{
			WithPort(port),
			WithServiceProvider(sampleServiceProvider()),
			WithMiddleware(append(smw, auth)...),
		}
		srv, err := NewServer(opts...)
		assert.Nil(err, "new server")
		go func() {
			_ = srv.Start()
		}()

		// Client connection
		cl, err := NewClient("tcp", fmt.Sprintf(":%d", port))
		assert.Nil(err, "client connection")

		// RPC client
		client := sampleV1.NewDRPCFooAPIClient(cl)

		t.Run("NoCredentials", func(t *testing.T) {
			_, err := client.Ping(context.TODO(), &emptypb.Empty{})
			assert.NotNil(err, "invalid auth")
			assert.Equal(err.Error(), "authentication: missing credentials")
		})

		t.Run("InvalidCredentials", func(t *testing.T) {
			// Submit metadata values to the server
			ctx := ContextWithMetadata(context.TODO(), map[string]string{
				"auth.token": "invalid-credentials",
				"user.id":    "user-123",
			})

			_, err := client.Ping(ctx, &emptypb.Empty{})
			assert.NotNil(err, "invalid auth")
			assert.Equal(err.Error(), "authentication: invalid credentials")
		})

		t.Run("Authenticated", func(t *testing.T) {
			// Submit metadata values to the server
			ctx := ContextWithMetadata(context.TODO(), map[string]string{
				"auth.token": "super-secure-credentials",
				"user.id":    "user-123",
			})
			_, err := client.Ping(ctx, &emptypb.Empty{})
			assert.Nil(err, "invalid auth")
		})

		// Close client connection
		assert.Nil(cl.Close(), "close client connection")

		// Stop server
		_ = srv.Stop()
	})

	t.Run("WithRetry", func(t *testing.T) {
		port := 8000 + int32(rand.Intn(1000))
		// RPC server
		opts := []Option{
			WithPort(port),
			WithServiceProvider(sampleServiceProvider()),
		}
		srv, err := NewServer(opts...)
		assert.Nil(err, "new server")
		go func() {
			_ = srv.Start()
		}()

		// Client connection
		clm := append(cmw, clMW.Retry(5, ll.With(zap.String("component", "client"))))
		cl, err := NewClient("tcp", fmt.Sprintf(":%d", port), WithClientMiddleware(clm...))
		assert.Nil(err, "client connection")

		// RPC client
		client := sampleV1.NewDRPCFooAPIClient(cl)

		// Call operation with automatic retries
		_, err = client.Slow(context.TODO(), &emptypb.Empty{})
		assert.Nil(err, "unexpected error")

		// Close client connection
		assert.Nil(cl.Close(), "close client connection")

		// Stop server
		_ = srv.Stop()
	})

	t.Run("WithRateLimit", func(t *testing.T) {
		port := 8000 + int32(rand.Intn(1000))

		// RPC server, enforce a limit of 1 request per-second
		opts := []Option{
			WithPort(port),
			WithServiceProvider(sampleServiceProvider()),
			WithMiddleware(append(smw, srvMW.RateLimit(1))...),
		}
		srv, err := NewServer(opts...)
		assert.Nil(err, "new server")
		go func() {
			_ = srv.Start()
		}()

		// Client connection
		cl, err := NewClient("tcp", fmt.Sprintf(":%d", port))
		assert.Nil(err, "client connection")

		// RPC client
		client := sampleV1.NewDRPCFooAPIClient(cl)

		// First request should work, second shouldn't
		_, err = client.Ping(context.TODO(), &emptypb.Empty{})
		assert.Nil(err, "invalid result")
		_, err = client.Ping(context.TODO(), &emptypb.Empty{})
		assert.NotNil(err, "invalid result")
		assert.Equal(err.Error(), "rate: limit exceeded")

		// After a second rate is re-established
		<-time.After(1 * time.Second)
		_, err = client.Ping(context.TODO(), &emptypb.Empty{})
		assert.Nil(err, "invalid result")

		// Close client connection
		assert.Nil(cl.Close(), "close client connection")

		// Stop server
		_ = srv.Stop()
	})
}

func ExampleNewServer() {
	// Get RPC service
	myService := sampleServiceProvider()

	// Server options
	opts := []Option{
		WithPort(8080),
		WithServiceProvider(myService),
	}

	// Create new server
	srv, err := NewServer(opts...)
	if err != nil {
		panic(err)
	}

	// Wait for requests in the background
	go func() {
		_ = srv.Start()
	}()

	// ... do something else ...
}

func ExampleNewClient() {
	// Client connection
	cl, err := NewClient("tcp", ":8080")
	if err != nil {
		panic(err)
	}

	// RPC client
	client := sampleV1.NewDRPCEchoAPIClient(cl)

	// Consume the RPC service
	res, _ := client.Ping(context.TODO(), &emptypb.Empty{})
	fmt.Printf("ping: %+v", res)

	// Close client connection when no longer required
	_ = cl.Close()
}

type fooServiceProvider struct {
	// By embedding the handler instance the type itself provides the
	// implementation required when registering the element as a service
	// provider. This allows us to simplify the "ServiceProvider" interface
	// even further.
	*sampleV1.Handler
}

func (fsp *fooServiceProvider) DRPCDescription() drpc.Description {
	return sampleV1.DRPCFooAPIDescription{}
}

// This custom implementation of the Faulty method will panic and
// crash the server =(.
func (fsp *fooServiceProvider) Faulty(_ context.Context, _ *emptypb.Empty) (*sampleV1.DummyResponse, error) {
	panic("cool services MUST never panic!!!")
}

func (fsp *fooServiceProvider) OpenServerStream(_ *emptypb.Empty, stream sampleV1.DRPCFooAPI_OpenServerStreamStream) error {
	// Send 10 messages to the client
	for i := 0; i < 10; i++ {
		t := <-time.After(100 * time.Millisecond)
		c := &sampleV1.GenericStreamChunk{
			Sender: fsp.Name,
			Stamp:  t.Unix(),
		}
		if err := stream.Send(c); err != nil {
			return err
		}
	}

	// Close the stream
	return stream.Close()
}

func (fsp *fooServiceProvider) OpenClientStream(stream sampleV1.DRPCFooAPI_OpenClientStreamStream) (err error) {
	res := &sampleV1.StreamResult{Received: 0}
	for {
		_, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			// SendAndClose doesn't currently work when exposing stream operations
			// through WebSockets. If that's required we can use bidirectional streams
			// instead.
			return stream.Close()
		}
		if err != nil {
			return
		}
		res.Received++
	}
}

func sampleServiceProvider() *fooServiceProvider {
	return &fooServiceProvider{
		Handler: &sampleV1.Handler{Name: "foo"},
	}
}

func getHTTPClient() http.Client {
	return http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}}
}
