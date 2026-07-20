package leasehttp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"

	"github.com/crapthings/meldbase/integrations/primarylease"
)

func TestMemberServesStrictAuthenticatedLoadAndCAS(t *testing.T) {
	store := primarylease.NewMemoryStore()
	member, err := NewMember(MemberConfig{Store: store, Authorize: func(*http.Request) (string, error) { return "controller-a", nil }})
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{11: 1}
	endpoint := leasePath + hex.EncodeToString(databaseID[:])
	missing := memberRequest(http.MethodGet, endpoint, nil)
	response := httptest.NewRecorder()
	member.ServeHTTP(response, missing)
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d body=%s", response.Code, response.Body.String())
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	next := primarylease.LeaseRecord{DatabaseID: databaseID, Owner: "writer-a", Epoch: 1, CommitSequence: 2, NotAfter: now.Add(time.Minute)}
	body := wireCAS{Version: protocolVersion, Next: pointerRecord(encodeRecord(next))}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request := memberRequest(http.MethodPut, endpoint, encoded)
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	member.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("CAS status=%d body=%s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	member.ServeHTTP(response, memberRequest(http.MethodGet, endpoint, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("load status=%d body=%s", response.Code, response.Body.String())
	}
	var loaded wireLoad
	if err := json.Unmarshal(response.Body.Bytes(), &loaded); err != nil {
		t.Fatal(err)
	}
	record, err := decodeRecord(loaded.Record)
	if err != nil || loaded.Version != protocolVersion || record != next {
		t.Fatalf("load=%+v record=%+v err=%v", loaded, record, err)
	}
	request = memberRequest(http.MethodPut, endpoint, encoded)
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	member.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("duplicate create status=%d", response.Code)
	}
	malformed := memberRequest(http.MethodPut, endpoint, []byte(`{"v":1,"next":{},"extra":true}`))
	malformed.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	member.ServeHTTP(response, malformed)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("malformed status=%d", response.Code)
	}
}

func TestMemberRejectsOriginsUnauthenticatedAndUnsafePaths(t *testing.T) {
	member, err := NewMember(MemberConfig{Store: primarylease.NewMemoryStore(), Authorize: func(*http.Request) (string, error) { return "controller-a", nil }})
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{11: 2}
	endpoint := leasePath + hex.EncodeToString(databaseID[:])
	unauthenticated := httptest.NewRequest(http.MethodGet, endpoint, nil)
	response := httptest.NewRecorder()
	member.ServeHTTP(response, unauthenticated)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d", response.Code)
	}
	origin := memberRequest(http.MethodGet, endpoint, nil)
	origin.Header.Set("Origin", "https://browser.invalid")
	response = httptest.NewRecorder()
	member.ServeHTTP(response, origin)
	if response.Code != http.StatusForbidden {
		t.Fatalf("origin status=%d", response.Code)
	}
	unsafePath := memberRequest(http.MethodGet, path.Join(leasePath, "..", "other"), nil)
	response = httptest.NewRecorder()
	member.ServeHTTP(response, unsafePath)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unsafe path status=%d", response.Code)
	}
	denied, err := NewMember(MemberConfig{Store: primarylease.NewMemoryStore(), Authorize: func(*http.Request) (string, error) { return "", errors.New("denied") }})
	if err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	denied.ServeHTTP(response, memberRequest(http.MethodGet, endpoint, nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("denied status=%d", response.Code)
	}
}

func TestClientUsesVerifiedHTTPSAndExactWireContract(t *testing.T) {
	databaseID := [16]byte{11: 3}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	record := primarylease.LeaseRecord{DatabaseID: databaseID, Owner: "writer-a", Epoch: 1, CommitSequence: 5, NotAfter: now.Add(time.Minute)}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != leasePath+hex.EncodeToString(databaseID[:]) {
			t.Fatalf("path=%q", request.URL.Path)
		}
		switch request.Method {
		case http.MethodGet:
			writer.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(wireLoad{Version: protocolVersion, Record: encodeRecord(record)})
		case http.MethodPut:
			var body wireCAS
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.Version != protocolVersion || body.Next == nil {
				t.Fatalf("CAS body=%+v err=%v", body, err)
			}
			writer.WriteHeader(http.StatusNoContent)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	fingerprint := sha256.Sum256(server.Certificate().Raw)
	client, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprint: hex.EncodeToString(fingerprint[:]), HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	loaded, exists, err := client.LoadPrimaryLease(context.Background(), databaseID)
	if err != nil || !exists || loaded != record {
		t.Fatalf("load record=%+v exists=%t err=%v", loaded, exists, err)
	}
	next := record
	next.Epoch = 2
	if swapped, err := client.CompareAndSwapPrimaryLease(context.Background(), databaseID, &record, next); err != nil || !swapped {
		t.Fatalf("CAS swapped=%t err=%v", swapped, err)
	}
	wrongFingerprint := fingerprint
	wrongFingerprint[0] ^= 1
	wrongClient, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprint: hex.EncodeToString(wrongFingerprint[:]), HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := wrongClient.LoadPrimaryLease(context.Background(), databaseID); !errors.Is(err, ErrHTTPSRequired) {
		t.Fatalf("wrong server fingerprint err=%v", err)
	}
	if _, err := NewClient(ClientOptions{Endpoint: "http://member.invalid"}); !errors.Is(err, ErrHTTPSRequired) {
		t.Fatalf("insecure client err=%v", err)
	}
	if _, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprint: hex.EncodeToString(fingerprint[:]), ExpectedServerFingerprints: []string{hex.EncodeToString(fingerprint[:])}, HTTPClient: server.Client()}); !errors.Is(err, ErrHTTPSRequired) {
		t.Fatalf("mixed pin configuration err=%v", err)
	}
	if _, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprints: []string{hex.EncodeToString(fingerprint[:]), hex.EncodeToString(fingerprint[:])}, HTTPClient: server.Client()}); !errors.Is(err, ErrHTTPSRequired) {
		t.Fatalf("duplicate pin configuration err=%v", err)
	}
	if _, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprints: []string{"ABC"}, HTTPClient: server.Client()}); !errors.Is(err, ErrHTTPSRequired) {
		t.Fatalf("noncanonical pin configuration err=%v", err)
	}
}

func memberRequest(method, endpoint string, body []byte) *http.Request {
	request := httptest.NewRequest(method, endpoint, bytes.NewReader(body))
	request.TLS = &tls.ConnectionState{Version: tls.VersionTLS13, VerifiedChains: [][]*x509.Certificate{{{Raw: []byte("verified-peer")}}}}
	return request
}
