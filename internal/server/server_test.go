package server

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	api "github.com/codeaucafe/distributed-log/api/v1"
	"github.com/codeaucafe/distributed-log/internal/config"
	"github.com/codeaucafe/distributed-log/internal/log"
)

func TestServer(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, rootClient api.LogClient, nobodyClient api.LogClient, config *Config)
	}{
		{"produce/consume a message to/from the log succeeds", testProduceConsume},
		{"produce/consume stream succeeds", testProduceConsumeStream},
		{"consume past log boundary fails", testConsumePastBoundary},
		{"unauthorized fails", testUnauthorized},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()
				rootClient, nobodyClient, config, teardown := setupTest(t, nil)
				defer teardown()
				tt.fn(t, rootClient, nobodyClient, config)
			},
		)
	}
}

// setupTest creates a gRPC server and client for testing.
// The optional fn allows customizing the server config before startup.
// Returns a teardown function that must be deferred to clean up resources.
func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.LogClient, nobodyClient api.LogClient, cfg *Config, teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0") // port 0 = OS assigns available port
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		tlsConfig, err := config.SetupTLSConfig(
			config.TLSConfig{
				CertFile: crtPath,
				KeyFile:  keyPath,
				CAFile:   config.CAFile,
				Server:   false,
			},
		)
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.NewClient(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(config.RootClientCertFile, config.RootClientKeyFile)
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: l.Addr().String(),
			Server:        true,
		},
	)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir := t.TempDir()
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	// teardown stops the server and closes all connections.
	// This forcefully terminates any active streams (e.g., ProduceStream blocked on Recv).
	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Close()
	}
}

// testProduceConsume verifies basic unary produce and consume RPCs.
func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(
		ctx, &api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)

	consume, err := client.Consume(
		ctx, &api.ConsumeRequest{
			Offset: produce.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

// testConsumePastBoundary verifies that consuming beyond the log returns ErrOffsetOutOfRange.
func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(
		ctx, &api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		},
	)
	require.NoError(t, err)

	// Request offset beyond what exists in the log.
	consume, err := client.Consume(
		ctx, &api.ConsumeRequest{
			Offset: produce.Offset + 1,
		},
	)
	require.Nil(t, consume)
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, want, got)
}

// testProduceConsumeStream verifies bidirectional streaming for produce and server-side streaming for consume.
func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	// Bidirectional stream: send records and verify server-assigned offsets.
	pStream, err := client.ProduceStream(ctx)
	require.NoError(t, err)

	for offset, record := range records {
		err = pStream.Send(
			&api.ProduceRequest{
				Record: record,
			},
		)
		require.NoError(t, err)
		// Recv blocks until server responds with the assigned offset.
		res, err := pStream.Recv()
		require.NoError(t, err)
		require.Equal(t, uint64(offset), res.Offset)
	}

	// Server-side stream: read back all records starting from offset 0.
	cStream, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{Offset: 0},
	)
	require.NoError(t, err)

	for _, record := range records {
		res, err := cStream.Recv()
		require.NoError(t, err)
		require.Equal(t, &api.Record{Value: record.Value, Offset: record.Offset}, res.Record)
	}
}

func testUnauthorized(t *testing.T, _, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{Value: []byte("hello world")},
		},
	)
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code %d, want %d", gotCode, wantCode)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code %d, want %d", gotCode, wantCode)
	}
}
