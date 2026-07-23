package replicationws

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/crapthings/meldbase"
)

func TestHandlerStreamsOneDurablyAcknowledgedBatch(t *testing.T) {
	db, err := meldbase.Open(filepath.Join(t.TempDir(), "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(Config{DB: db, Authorize: func(*http.Request) (string, error) { return "peer-a", nil }, MaxFrameBytes: 1 << 20, Buffer: 2})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	endpoint := "wss" + strings.TrimPrefix(server.URL, "https")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "")
	hello := meldbase.ReplicationFrame{Type: meldbase.ReplicationHelloFrame, DatabaseID: db.DatabaseID(), AfterToken: db.Stats().CommitSequence, MaxBytes: 1 << 20}
	raw, err := meldbase.MarshalReplicationFrame(hello, meldbase.ReplicationFrameLimits{MaxFrameBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"value": 2}}); err != nil {
		t.Fatal(err)
	}
	messageType, raw, err := connection.Read(ctx)
	if err != nil || messageType != websocket.MessageText {
		t.Fatalf("batch read type=%v err=%v", messageType, err)
	}
	batch, err := meldbase.UnmarshalReplicationFrame(raw, meldbase.ReplicationFrameLimits{MaxFrameBytes: 1 << 20})
	if err != nil || batch.Type != meldbase.ReplicationBatchFrame || batch.Batch == nil || batch.Batch.Token != 2 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	ackRaw, err := meldbase.MarshalReplicationFrame(meldbase.ReplicationFrame{Type: meldbase.ReplicationAckFrame, DatabaseID: db.DatabaseID(), AckToken: batch.Batch.Token}, meldbase.ReplicationFrameLimits{MaxFrameBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Write(ctx, websocket.MessageText, ackRaw); err != nil {
		t.Fatal(err)
	}
	// The source cannot send this second batch until it has processed and
	// durably stored the first ACK.
	if _, err := db.Collection("items").UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"value": 3}}); err != nil {
		t.Fatal(err)
	}
	messageType, raw, err = connection.Read(ctx)
	if err != nil || messageType != websocket.MessageText {
		t.Fatalf("second batch read type=%v err=%v", messageType, err)
	}
	second, err := meldbase.UnmarshalReplicationFrame(raw, meldbase.ReplicationFrameLimits{MaxFrameBytes: 1 << 20})
	if err != nil || second.Batch == nil || second.Batch.Token != 3 {
		t.Fatalf("second batch=%+v err=%v", second, err)
	}
	subscription, err := db.OpenDurableDatabaseChanges(context.Background(), "peer-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if checkpoint, err := subscription.Checkpoint(); err != nil || checkpoint != batch.Batch.Token {
		t.Fatalf("durable replication checkpoint=%d err=%v", checkpoint, err)
	}
}

func TestHandlerRejectsBrowserOriginAndUnauthenticatedPeer(t *testing.T) {
	db := meldbase.New()
	defer db.Close()
	handler, err := New(Config{DB: db, Authorize: func(*http.Request) (string, error) { return "", errors.New("no") }})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://example/replication", nil)
	request.Header.Set("Origin", "https://browser.example")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("browser origin status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodGet, "http://example/replication", nil)
	request.TLS = &tls.ConnectionState{Version: tls.VersionTLS13}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodGet, "http://example/replication", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("plain HTTP status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodGet, "https://example/replication", nil)
	request.TLS = &tls.ConnectionState{Version: tls.VersionTLS11}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("obsolete TLS status=%d", response.Code)
	}
}

func TestHandlerRejectsConcurrentSessionForAuthorizedConsumer(t *testing.T) {
	db := meldbase.New()
	defer db.Close()
	handler, err := New(Config{DB: db, Authorize: func(*http.Request) (string, error) { return "peer-a", nil }})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireReplicationSourceLease("peer-a")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	request := httptest.NewRequest(http.MethodGet, "http://example/replication", nil)
	request.TLS = &tls.ConnectionState{Version: tls.VersionTLS13}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("concurrent replication status=%d", response.Code)
	}
}

func TestReceiveAppliesPrimaryWebSocketTailToFollower(t *testing.T) {
	directory := t.TempDir()
	source, err := meldbase.Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	id, err := source.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Backup(context.Background(), filepath.Join(directory, "bootstrap.meld2")); err != nil {
		t.Fatal(err)
	}
	follower, err := meldbase.OpenFollower(filepath.Join(directory, "bootstrap.meld2"), meldbase.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	if err := Receive(context.Background(), ReceiverConfig{Follower: follower, URL: "ws://example.test/replication", MaxFrameBytes: 1 << 20}); err == nil || !strings.Contains(err.Error(), "wss") {
		t.Fatalf("insecure receiver URL err=%v", err)
	}
	handler, err := New(Config{DB: source, Authorize: func(*http.Request) (string, error) { return "receiver", nil }, MaxFrameBytes: 1 << 20, Buffer: 2})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	endpoint := "wss" + strings.TrimPrefix(server.URL, "https")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Receive(ctx, ReceiverConfig{Follower: follower, URL: endpoint, MaxFrameBytes: 1 << 20, DialOptions: &websocket.DialOptions{HTTPClient: server.Client()}})
	}()
	// Give the receiver enough time to establish its hello/checkpoint before
	// producing the tail batch. The follower query below is the eventual proof.
	time.Sleep(25 * time.Millisecond)
	if _, err := source.Collection("items").UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"value": 2}}); err != nil {
		cancel()
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		document, queryErr := follower.DB().Collection("items").FindOne(context.Background(), meldbase.Filter{"_id": id})
		value, ok := document["value"].Int64()
		if queryErr == nil && ok && value == 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			err := <-done
			t.Fatalf("follower did not apply WebSocket tail: document=%v query=%v receiver=%v", document, queryErr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("receiver close err=%v", err)
	}
}

func TestReceiveAcknowledgesBootstrapCoveredBatchesBeforeApplyingTail(t *testing.T) {
	directory := t.TempDir()
	source, err := meldbase.Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	id, err := source.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := source.Stats().CommitSequence
	seed, err := source.CreateDurableDatabaseChanges(context.Background(), "bootstrap-receiver", checkpoint, 2)
	if err != nil {
		t.Fatal(err)
	}
	seed.Close()
	if _, err := source.Collection("items").UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"value": 2}}); err != nil {
		t.Fatal(err)
	}
	bootstrapToken := source.Stats().CommitSequence
	if _, err := source.Backup(context.Background(), filepath.Join(directory, "bootstrap.meld2")); err != nil {
		t.Fatal(err)
	}
	follower, err := meldbase.OpenFollower(filepath.Join(directory, "bootstrap.meld2"), meldbase.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	if follower.DB().Stats().CommitSequence != bootstrapToken {
		t.Fatalf("follower bootstrap token=%d want=%d", follower.DB().Stats().CommitSequence, bootstrapToken)
	}
	handler, err := New(Config{DB: source, Authorize: func(*http.Request) (string, error) { return "bootstrap-receiver", nil }, MaxFrameBytes: 1 << 20, Buffer: 2})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Receive(ctx, ReceiverConfig{
			Follower: follower, URL: "wss" + strings.TrimPrefix(server.URL, "https"), MaxFrameBytes: 1 << 20,
			DialOptions:           &websocket.DialOptions{HTTPClient: server.Client()},
			SourceCheckpointToken: checkpoint, BootstrapToken: bootstrapToken,
		})
	}()
	time.Sleep(25 * time.Millisecond)
	if _, err := source.Collection("items").UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"value": 3}}); err != nil {
		cancel()
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		document, queryErr := follower.DB().Collection("items").FindOne(context.Background(), meldbase.Filter{"_id": id})
		value, ok := document["value"].Int64()
		if queryErr == nil && ok && value == 3 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			err := <-done
			t.Fatalf("bootstrap follower did not apply tail: document=%v query=%v receiver=%v", document, queryErr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("bootstrap receiver close err=%v", err)
	}
	subscription, err := source.OpenDurableDatabaseChanges(context.Background(), "bootstrap-receiver", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	// Seeing token 3 proves the source processed the ACK for bootstrap-covered
	// token 2 before it sent the tail. The ACK for token 3 may still be in the
	// server read loop when cancellation closes this test connection.
	if durable, err := subscription.Checkpoint(); err != nil || (durable != source.Stats().CommitSequence-1 && durable != source.Stats().CommitSequence) {
		t.Fatalf("bootstrap checkpoint=%d source=%d err=%v", durable, source.Stats().CommitSequence, err)
	}
}
