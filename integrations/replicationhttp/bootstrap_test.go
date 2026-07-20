package replicationhttp

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/crapthings/meldbase"
	"github.com/crapthings/meldbase/integrations/replicationws"
)

func TestFetchImportsVerifiedHTTPSBootstrap(t *testing.T) {
	directory := t.TempDir()
	source, err := meldbase.OpenV2(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	id, err := source.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(7)})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewSource(SourceConfig{
		DB: source,
		Authorize: func(*http.Request) (string, error) {
			return "replica-a", nil
		},
		ArchiveDirectory: directory,
		Buffer:           8,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	destination := filepath.Join(directory, "follower.meld2")
	bootstrap, err := Fetch(context.Background(), FetchConfig{URL: server.URL, HTTPClient: server.Client(), Destination: destination})
	if err != nil {
		t.Fatal(err)
	}
	if bootstrap.SnapshotToken != bootstrap.Backup.CommitSequence || bootstrap.CheckpointToken > bootstrap.SnapshotToken || bootstrap.Backup.Bytes == 0 {
		t.Fatalf("bootstrap=%+v", bootstrap)
	}
	follower, err := meldbase.OpenV2Follower(destination, meldbase.V2Options{RequireGraphAudit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	document, err := follower.DB().Collection("items").FindOne(context.Background(), meldbase.Filter{"_id": id})
	if err != nil || !document["value"].Equal(meldbase.Int(7)) {
		t.Fatalf("follower document=%v err=%v", document, err)
	}
	// The named checkpoint remains after a successful download, ready for the
	// WebSocket tail phase. A new archive cannot accidentally reset it.
	if subscription, err := source.OpenDurableDatabaseChanges(context.Background(), "replica-a", 1); err != nil || subscription == nil {
		t.Fatalf("tail checkpoint subscription=%v err=%v", subscription, err)
	} else {
		subscription.Close()
	}
}

func TestFetchBootstrapHandsOffToSameDurableWSSConsumer(t *testing.T) {
	directory := t.TempDir()
	source, err := meldbase.OpenV2(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	id, err := source.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	bootstrapSource, err := NewSource(SourceConfig{
		DB: source, Authorize: func(*http.Request) (string, error) { return "receiver", nil },
		ArchiveDirectory: directory, Buffer: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	bootstrapServer := httptest.NewTLSServer(bootstrapSource)
	defer bootstrapServer.Close()
	bootstrapPath := filepath.Join(directory, "follower.meld2")
	bootstrap, err := Fetch(context.Background(), FetchConfig{URL: bootstrapServer.URL, HTTPClient: bootstrapServer.Client(), Destination: bootstrapPath})
	if err != nil {
		t.Fatal(err)
	}
	follower, err := meldbase.OpenV2Follower(bootstrapPath, meldbase.V2Options{RequireGraphAudit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	if got := follower.DB().Stats().CommitSequence; got != bootstrap.SnapshotToken {
		t.Fatalf("follower bootstrap token=%d want=%d", got, bootstrap.SnapshotToken)
	}

	tailHandler, err := replicationws.New(replicationws.Config{
		DB: source, Authorize: func(*http.Request) (string, error) { return "receiver", nil }, MaxFrameBytes: 1 << 20, Buffer: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	tailServer := httptest.NewTLSServer(tailHandler)
	defer tailServer.Close()
	receiveContext, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- replicationws.Receive(receiveContext, replicationws.ReceiverConfig{
			Follower:              follower,
			URL:                   "wss" + tailServer.URL[len("https"):],
			DialOptions:           &websocket.DialOptions{HTTPClient: tailServer.Client()},
			MaxFrameBytes:         1 << 20,
			SourceCheckpointToken: bootstrap.CheckpointToken,
			BootstrapToken:        bootstrap.SnapshotToken,
		})
	}()
	// The follower must ACK through the snapshot's source token before applying
	// later batches. Waiting briefly only gives the durable hello a chance to
	// reach the source; the eventual follower value is the actual proof.
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
			t.Fatalf("follower did not receive bootstrap tail: document=%v query=%v receive=%v", document, queryErr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("receiver close err=%v", err)
	}
}

func TestFetchRejectsInsecureOrMalformedBootstrapBeforePublication(t *testing.T) {
	if _, err := Fetch(context.Background(), FetchConfig{URL: "http://example.test/bootstrap", Destination: filepath.Join(t.TempDir(), "never.meld2")}); !errors.Is(err, ErrBootstrapHTTPSRequired) {
		t.Fatalf("HTTP fetch err=%v", err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set(headerBootstrapVersion, bootstrapVersion)
		writer.Header().Set(headerBytes, "4096")
		writer.Header().Set(headerPages, "1")
		writer.Header().Set(headerCommitSequence, "1")
		writer.Header().Set(headerMetaGeneration, "1")
		writer.Header().Set(headerDatabaseID, "00000000000000000000000000000000")
		writer.Header().Set(headerSHA256, "0000000000000000000000000000000000000000000000000000000000000000")
		writer.Header().Set(headerCheckpointToken, "1")
		writer.Header().Set(headerSnapshotToken, "1")
		_, _ = writer.Write(make([]byte, 1))
	}))
	defer server.Close()
	destination := filepath.Join(t.TempDir(), "never.meld2")
	if _, err := Fetch(context.Background(), FetchConfig{URL: server.URL, HTTPClient: server.Client(), Destination: destination}); !errors.Is(err, ErrInvalidBootstrapResponse) {
		t.Fatalf("malformed fetch err=%v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("malformed response published destination: %v", err)
	}
	unsafeClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} // #nosec G402 -- proves Fetch rejects an unauthenticated peer.
	defer unsafeClient.CloseIdleConnections()
	unsafeDestination := filepath.Join(t.TempDir(), "unsafe-tls.meld2")
	if _, err := Fetch(context.Background(), FetchConfig{URL: server.URL, HTTPClient: unsafeClient, Destination: unsafeDestination}); !errors.Is(err, ErrBootstrapHTTPSRequired) {
		t.Fatalf("unsafe TLS fetch err=%v", err)
	}
	if _, err := os.Stat(unsafeDestination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsafe TLS response published destination: %v", err)
	}
}

func TestSourceRejectsPlainHTTPAndConcurrentConsumer(t *testing.T) {
	directory := t.TempDir()
	db, err := meldbase.OpenV2(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler, err := NewSource(SourceConfig{DB: db, Authorize: func(*http.Request) (string, error) { return "replica-a", nil }, ArchiveDirectory: directory, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	plain := httptest.NewServer(handler)
	defer plain.Close()
	response, err := plain.Client().Get(plain.URL)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusBadRequest {
		response.Body.Close()
		t.Fatalf("plain status=%d", response.StatusCode)
	}
	response.Body.Close()
	obsolete := httptest.NewRequest(http.MethodGet, "https://example/replication", nil)
	obsolete.TLS = &tls.ConnectionState{Version: tls.VersionTLS11}
	obsoleteResponse := httptest.NewRecorder()
	handler.ServeHTTP(obsoleteResponse, obsolete)
	if obsoleteResponse.Code != http.StatusBadRequest {
		t.Fatalf("obsolete TLS status=%d", obsoleteResponse.Code)
	}
	lease, err := db.AcquireReplicationSourceLease("replica-a")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	tlsServer := httptest.NewTLSServer(handler)
	defer tlsServer.Close()
	response, err = tlsServer.Client().Get(tlsServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("concurrent source status=%d", response.StatusCode)
	}
}
