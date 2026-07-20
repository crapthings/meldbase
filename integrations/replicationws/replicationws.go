// Package replicationws adapts Meldbase's trusted-server replication protocol
// to WebSocket. It intentionally has no browser/session authentication: callers
// must supply an authorization callback backed by mTLS, a private network
// identity, or another server-to-server credential.
package replicationws

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/coder/websocket"
	"github.com/crapthings/meldbase"
	"github.com/crapthings/meldbase/integrations/replicationauth"
)

const (
	defaultMaxFrameBytes = 16 << 20
	defaultBuffer        = 16
)

// Authorize must authenticate a server-to-server peer and return its stable
// durable-consumer name. The name scopes retention and must not be derived from
// an untrusted frame field. Returning an error rejects the WebSocket upgrade.
type Authorize = replicationauth.Authorize

type Config struct {
	DB            *meldbase.DB
	Authorize     Authorize
	MaxFrameBytes int
	Buffer        int
}

// Handler serves one authenticated primary/source replication connection.
// Each connection is pull/acknowledge serialized: it reads hello, writes one
// batch, waits for its matching ACK, then repeats. This bounds server memory
// and guarantees that a peer cannot acknowledge an unseen token.
type Handler struct{ config Config }

func New(config Config) (*Handler, error) {
	if config.DB == nil || config.Authorize == nil {
		return nil, errors.New("replication websocket requires database and authorizer")
	}
	if config.MaxFrameBytes == 0 {
		config.MaxFrameBytes = defaultMaxFrameBytes
	}
	if config.MaxFrameBytes < 64<<10 || config.MaxFrameBytes > meldbase.DefaultReplicationMaxFrameBytes {
		return nil, errors.New("replication websocket frame limit must be between 64 KiB and 96 MiB")
	}
	if config.Buffer == 0 {
		config.Buffer = defaultBuffer
	}
	if config.Buffer < 1 || config.Buffer > 1024 {
		return nil, errors.New("replication websocket buffer must be between 1 and 1024")
	}
	return &Handler{config: config}, nil
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if handler == nil || request == nil || request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Replication is never a browser API. An Origin header means this request
	// crossed a browser security boundary, which this adapter intentionally does
	// not model with CORS or realtime tickets.
	if request.Header.Get("Origin") != "" {
		http.Error(writer, "forbidden", http.StatusForbidden)
		return
	}
	if request.TLS == nil || request.TLS.Version < tls.VersionTLS12 {
		http.Error(writer, "TLS 1.2 or later is required", http.StatusBadRequest)
		return
	}
	consumerName, err := handler.config.Authorize(request)
	if err != nil || consumerName == "" {
		http.Error(writer, "unauthenticated", http.StatusUnauthorized)
		return
	}
	lease, err := handler.config.DB.AcquireReplicationSourceLease(consumerName)
	if err != nil {
		if errors.Is(err, meldbase.ErrReplicaSourceActive) {
			http.Error(writer, "replication session already active", http.StatusConflict)
			return
		}
		// The durable name comes from server-side authorization. Do not expose
		// configuration details or DB state to an unauthenticated endpoint.
		http.Error(writer, "unauthenticated", http.StatusUnauthorized)
		return
	}
	defer lease.Release()
	connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		return
	}
	defer connection.Close(websocket.StatusNormalClosure, "")
	connection.SetReadLimit(int64(handler.config.MaxFrameBytes))
	ctx := request.Context()
	if err := handler.serve(ctx, connection, consumerName); err != nil && !errors.Is(err, context.Canceled) {
		_ = connection.Close(websocket.StatusPolicyViolation, "replication protocol error")
	}
}

func (handler *Handler) serve(ctx context.Context, connection *websocket.Conn, consumerName string) error {
	frame, err := handler.readFrame(ctx, connection)
	if err != nil {
		return err
	}
	if frame.Type != meldbase.ReplicationHelloFrame {
		return meldbase.ErrReplicaProtocol
	}
	subscription, err := handler.config.DB.OpenDurableDatabaseChanges(ctx, consumerName, handler.config.Buffer)
	if errors.Is(err, meldbase.ErrDurableConsumerNotFound) {
		subscription, err = handler.config.DB.CreateDurableDatabaseChanges(ctx, consumerName, frame.AfterToken, handler.config.Buffer)
	}
	if errors.Is(err, meldbase.ErrHistoryLost) {
		return handler.writeFrame(ctx, connection, meldbase.ReplicationFrame{Type: meldbase.ReplicationResyncFrame, DatabaseID: handler.config.DB.DatabaseID(), Reason: "history_lost"})
	}
	if err != nil {
		return err
	}
	defer subscription.Close()
	session, err := meldbase.NewReplicationSourceSession(handler.config.DB, subscription, meldbase.ReplicationFrameLimits{MaxFrameBytes: handler.config.MaxFrameBytes})
	if err != nil {
		return err
	}
	defer session.Close()
	if response, err := session.AcceptHello(frame); err != nil {
		return err
	} else if response != nil {
		return handler.writeFrame(ctx, connection, *response)
	}
	for {
		outbound, err := session.NextFrame(ctx)
		if err != nil {
			return err
		}
		if err := handler.writeFrame(ctx, connection, *outbound); err != nil {
			return err
		}
		if outbound.Type == meldbase.ReplicationResyncFrame {
			return nil
		}
		ack, err := handler.readFrame(ctx, connection)
		if err != nil {
			return err
		}
		if err := session.AcceptAck(ack); err != nil {
			return err
		}
	}
}

func (handler *Handler) readFrame(ctx context.Context, connection *websocket.Conn) (meldbase.ReplicationFrame, error) {
	messageType, raw, err := connection.Read(ctx)
	if err != nil {
		return meldbase.ReplicationFrame{}, err
	}
	if messageType != websocket.MessageText || len(raw) == 0 || len(raw) > handler.config.MaxFrameBytes {
		return meldbase.ReplicationFrame{}, meldbase.ErrReplicaProtocol
	}
	return meldbase.UnmarshalReplicationFrame(raw, meldbase.ReplicationFrameLimits{MaxFrameBytes: handler.config.MaxFrameBytes})
}

func (handler *Handler) writeFrame(ctx context.Context, connection *websocket.Conn, frame meldbase.ReplicationFrame) error {
	raw, err := meldbase.MarshalReplicationFrame(frame, meldbase.ReplicationFrameLimits{MaxFrameBytes: handler.config.MaxFrameBytes})
	if err != nil {
		return err
	}
	return connection.Write(ctx, websocket.MessageText, raw)
}

// ReceiverConfig configures a follower-side WebSocket client. DialOptions is
// where callers install an mTLS HTTP client, private-network proxy or other
// server-to-server transport identity. It must not be used for browser tokens.
type ReceiverConfig struct {
	Follower              *meldbase.V2Follower
	URL                   string
	DialOptions           *websocket.DialOptions
	MaxFrameBytes         int
	SourceCheckpointToken uint64
	BootstrapToken        uint64
}

// Receive connects one follower to a primary replication endpoint. It returns
// only after context cancellation, a terminal resync, authentication/transport
// failure, or a failed follower apply. A batch is ACKed only after it is either
// covered by the verified bootstrap snapshot or durably applied locally.
func Receive(ctx context.Context, config ReceiverConfig) error {
	if config.Follower == nil || config.Follower.DB() == nil {
		return meldbase.ErrClosed
	}
	endpoint, err := url.Parse(config.URL)
	if err != nil || endpoint.Scheme != "wss" || endpoint.Host == "" || endpoint.User != nil {
		return errors.New("replication receiver requires wss URL")
	}
	if config.MaxFrameBytes == 0 {
		config.MaxFrameBytes = defaultMaxFrameBytes
	}
	if config.MaxFrameBytes < 64<<10 || config.MaxFrameBytes > meldbase.DefaultReplicationMaxFrameBytes {
		return errors.New("replication receiver frame limit must be between 64 KiB and 96 MiB")
	}
	db := config.Follower.DB()
	localToken := db.Stats().CommitSequence
	if config.BootstrapToken == 0 {
		config.BootstrapToken = localToken
	}
	if config.BootstrapToken != localToken || config.SourceCheckpointToken > config.BootstrapToken {
		return meldbase.ErrReplicaProtocol
	}
	if config.SourceCheckpointToken == 0 {
		config.SourceCheckpointToken = localToken
	}
	connection, response, err := websocket.Dial(ctx, config.URL, cloneNoRedirectDialOptions(config.DialOptions))
	if err != nil {
		return err
	}
	if response == nil {
		_ = connection.Close(websocket.StatusPolicyViolation, "TLS is required")
		return errors.New("replication receiver requires a TLS response")
	}
	if err := replicationauth.RequireVerifiedTLS(response.TLS); err != nil {
		_ = connection.Close(websocket.StatusPolicyViolation, "TLS verification is required")
		return fmt.Errorf("replication receiver requires verified TLS: %w", err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "")
	connection.SetReadLimit(int64(config.MaxFrameBytes))
	hello := meldbase.ReplicationFrame{
		Type: meldbase.ReplicationHelloFrame, DatabaseID: db.DatabaseID(),
		AfterToken: config.SourceCheckpointToken, MaxBytes: config.MaxFrameBytes,
	}
	if err := receiverWriteFrame(ctx, connection, hello, config.MaxFrameBytes); err != nil {
		return err
	}
	lastSeen := config.SourceCheckpointToken
	for {
		frame, err := receiverReadFrame(ctx, connection, config.MaxFrameBytes)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if frame.DatabaseID != db.DatabaseID() {
			return meldbase.ErrDatabaseIdentity
		}
		if frame.Type == meldbase.ReplicationResyncFrame {
			return receiverResyncError(frame.Reason)
		}
		if frame.Type != meldbase.ReplicationBatchFrame || frame.Batch == nil || frame.Batch.Token != lastSeen+1 {
			return meldbase.ErrReplicaProtocol
		}
		batch := frame.Batch
		if batch.Token > config.BootstrapToken {
			if err := config.Follower.ApplyFrame(ctx, frame); err != nil {
				return err
			}
		}
		ack := meldbase.ReplicationFrame{Type: meldbase.ReplicationAckFrame, DatabaseID: db.DatabaseID(), AckToken: batch.Token}
		if err := receiverWriteFrame(ctx, connection, ack, config.MaxFrameBytes); err != nil {
			return err
		}
		lastSeen = batch.Token
	}
}

func cloneNoRedirectDialOptions(options *websocket.DialOptions) *websocket.DialOptions {
	var copied websocket.DialOptions
	if options != nil {
		copied = *options
	}
	client := copied.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	copied.HTTPClient = &clientCopy
	return &copied
}

func receiverReadFrame(ctx context.Context, connection *websocket.Conn, maxFrameBytes int) (meldbase.ReplicationFrame, error) {
	messageType, raw, err := connection.Read(ctx)
	if err != nil {
		return meldbase.ReplicationFrame{}, err
	}
	if messageType != websocket.MessageText || len(raw) == 0 || len(raw) > maxFrameBytes {
		return meldbase.ReplicationFrame{}, meldbase.ErrReplicaProtocol
	}
	return meldbase.UnmarshalReplicationFrame(raw, meldbase.ReplicationFrameLimits{MaxFrameBytes: maxFrameBytes})
}

func receiverWriteFrame(ctx context.Context, connection *websocket.Conn, frame meldbase.ReplicationFrame, maxFrameBytes int) error {
	raw, err := meldbase.MarshalReplicationFrame(frame, meldbase.ReplicationFrameLimits{MaxFrameBytes: maxFrameBytes})
	if err != nil {
		return err
	}
	return connection.Write(ctx, websocket.MessageText, raw)
}

func receiverResyncError(reason string) error {
	switch reason {
	case "history_lost":
		return meldbase.ErrHistoryLost
	case "identity_mismatch":
		return meldbase.ErrDatabaseIdentity
	case "snapshot_required", "protocol_error":
		return meldbase.ErrReplicaProtocol
	default:
		return meldbase.ErrReplicaProtocol
	}
}
