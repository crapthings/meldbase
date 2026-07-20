package authorityhttp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/crapthings/meldbase/core"
	"github.com/crapthings/meldbase/integrations/leasehttp"
	"github.com/crapthings/meldbase/integrations/primarylease"
)

func TestHandlerDerivesOwnerAndSeparatesRevokeAuthorization(t *testing.T) {
	store := primarylease.NewMemoryStore()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{Store: store, PrivateKey: privateKey, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(HandlerConfig{
		Authority: authority,
		NodeAuthorize: func(*http.Request) (string, error) {
			return "node-from-certificate", nil
		},
		OperatorAuthorize: func(*http.Request) (string, error) {
			return "operator-from-certificate", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{13: 1}
	grantBody, err := json.Marshal(wireGrantRequest{Version: protocolVersion, CommitSequence: 4})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, verifiedRequest(http.MethodPost, grantPath+hex.EncodeToString(databaseID[:]), grantBody))
	if response.Code != http.StatusOK {
		t.Fatalf("grant status=%d body=%s", response.Code, response.Body.String())
	}
	var granted wireGrantResponse
	if err := json.Unmarshal(response.Body.Bytes(), &granted); err != nil || granted.Version != protocolVersion || granted.Certificate == "" {
		t.Fatalf("grant response=%+v err=%v", granted, err)
	}
	record, exists, err := store.LoadPrimaryLease(context.Background(), databaseID)
	if err != nil || !exists || record.Owner != "node-from-certificate" || record.CommitSequence != 4 {
		t.Fatalf("server trusted request owner: record=%+v exists=%t err=%v", record, exists, err)
	}

	denied, err := NewHandler(HandlerConfig{
		Authority:     authority,
		NodeAuthorize: handler.nodeAuthorize,
		OperatorAuthorize: func(*http.Request) (string, error) {
			return "", errors.New("denied")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	revokeBody, err := json.Marshal(wireRevokeRequest{Version: protocolVersion, Epoch: record.Epoch})
	if err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	denied.ServeHTTP(response, verifiedRequest(http.MethodPost, revokePath+hex.EncodeToString(databaseID[:]), revokeBody))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("node authorization also revoked lease: status=%d", response.Code)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, verifiedRequest(http.MethodPost, revokePath+hex.EncodeToString(databaseID[:]), revokeBody))
	if response.Code != http.StatusNoContent {
		t.Fatalf("operator revoke status=%d body=%s", response.Code, response.Body.String())
	}
	record, exists, err = store.LoadPrimaryLease(context.Background(), databaseID)
	if err != nil || !exists || !record.Revoked || record.Owner != "" {
		t.Fatalf("revoke record=%+v exists=%t err=%v", record, exists, err)
	}

	response = httptest.NewRecorder()
	unsafe := verifiedRequest(http.MethodPost, grantPath+"%2e%2e/revoke/"+hex.EncodeToString(databaseID[:]), grantBody)
	handler.ServeHTTP(response, unsafe)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unsafe path status=%d", response.Code)
	}
	response = httptest.NewRecorder()
	badContentType := verifiedRequest(http.MethodPost, grantPath+hex.EncodeToString(databaseID[:]), grantBody)
	badContentType.Header.Set("Content-Type", "text/plain")
	handler.ServeHTTP(response, badContentType)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("content type status=%d", response.Code)
	}
}

func TestClientUsesVerifiedMTLSIdentityPinnedServerAndLeaseResponses(t *testing.T) {
	pki := newAuthorityPKI(t)
	_, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{
		Store:         primarylease.NewMemoryStore(),
		PrivateKey:    signingKey,
		LeaseDuration: 5 * time.Second,
		Clock:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	nodeAFingerprint := sha256.Sum256(pki.nodeA.Leaf.Raw)
	nodeBFingerprint := sha256.Sum256(pki.nodeB.Leaf.Raw)
	operatorFingerprint := sha256.Sum256(pki.operator.Leaf.Raw)
	nodeAuthorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{
		hex.EncodeToString(nodeAFingerprint[:]): "node-a",
		hex.EncodeToString(nodeBFingerprint[:]): "node-b",
	}})
	if err != nil {
		t.Fatal(err)
	}
	operatorAuthorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{hex.EncodeToString(operatorFingerprint[:]): "operator-a"}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(HandlerConfig{Authority: authority, NodeAuthorize: nodeAuthorize, OperatorAuthorize: operatorAuthorize})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pki.server}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots}
	server.StartTLS()
	defer server.Close()
	serverFingerprint := sha256.Sum256(pki.server.Leaf.Raw)
	nodeAHTTP := pki.httpClient(&pki.nodeA)
	defer nodeAHTTP.CloseIdleConnections()
	nodeA, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprint: hex.EncodeToString(serverFingerprint[:]), HTTPClient: nodeAHTTP})
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{13: 2}
	grant, err := nodeA.Grant(context.Background(), databaseID, 9)
	if err != nil || grant.Certificate == "" {
		t.Fatalf("node grant=%+v err=%v", grant, err)
	}
	certificate, err := primarylease.Parse(grant.Certificate, signingKey.Public().(ed25519.PublicKey))
	if err != nil || certificate.Owner != "node-a" || certificate.DatabaseID != databaseID || certificate.CommitSequence != 9 {
		t.Fatalf("grant certificate=%+v err=%v", certificate, err)
	}
	nodeBHTTP := pki.httpClient(&pki.nodeB)
	defer nodeBHTTP.CloseIdleConnections()
	nodeB, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprints: []string{hex.EncodeToString(serverFingerprint[:])}, HTTPClient: nodeBHTTP})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodeB.Grant(context.Background(), databaseID, 9); !errors.Is(err, primarylease.ErrLeaseActive) {
		t.Fatalf("competing node grant err=%v", err)
	} else if retryAt, ok := RetryAfter(err); !ok || !retryAt.Equal(now.Add(6*time.Second)) {
		t.Fatalf("lease active retry=%v ok=%t", retryAt, ok)
	}
	if err := nodeA.Revoke(context.Background(), databaseID, certificate.Epoch); err == nil {
		t.Fatal("node identity revoked a lease")
	}
	operatorHTTP := pki.httpClient(&pki.operator)
	defer operatorHTTP.CloseIdleConnections()
	operator, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprints: []string{hex.EncodeToString(serverFingerprint[:])}, HTTPClient: operatorHTTP})
	if err != nil {
		t.Fatal(err)
	}
	if err := operator.Revoke(context.Background(), databaseID, certificate.Epoch); err != nil {
		t.Fatalf("operator revoke err=%v", err)
	}
	wrongFingerprint := serverFingerprint
	wrongFingerprint[0] ^= 1
	wrong, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprint: hex.EncodeToString(wrongFingerprint[:]), HTTPClient: operatorHTTP})
	if err != nil {
		t.Fatal(err)
	}
	operatorHTTP.CloseIdleConnections()
	if _, err := wrong.Grant(context.Background(), databaseID, 9); !errors.Is(err, ErrHTTPSRequired) {
		t.Fatalf("wrong server pin err=%v", err)
	}
}

func TestPrimaryRuntimeRenewsThroughPinnedMTLSAuthority(t *testing.T) {
	pki := newAuthorityPKI(t)
	publicKey, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{Store: primarylease.NewMemoryStore(), PrivateKey: signingKey, LeaseDuration: 5 * time.Second, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	nodeFingerprint := sha256.Sum256(pki.nodeA.Leaf.Raw)
	operatorFingerprint := sha256.Sum256(pki.operator.Leaf.Raw)
	nodeAuthorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{hex.EncodeToString(nodeFingerprint[:]): "node-a"}})
	if err != nil {
		t.Fatal(err)
	}
	operatorAuthorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{hex.EncodeToString(operatorFingerprint[:]): "operator-a"}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(HandlerConfig{Authority: authority, NodeAuthorize: nodeAuthorize, OperatorAuthorize: operatorAuthorize})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pki.server}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots}
	server.StartTLS()
	defer server.Close()
	serverFingerprint := sha256.Sum256(pki.server.Leaf.Raw)
	httpClient := pki.httpClient(&pki.nodeA)
	defer httpClient.CloseIdleConnections()
	client, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprints: []string{hex.EncodeToString(serverFingerprint[:])}, HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := primarylease.OpenPrimary(filepath.Join(t.TempDir(), "primary.meld2"), primarylease.PrimaryOptions{
		PublicKey: publicKey, GuardOptions: primarylease.GuardOptions{Owner: "node-a", Clock: func() time.Time { return now }}, RenewalClient: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if _, err := runtime.DB.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); !errors.Is(err, meldbase.ErrPrimaryWriteFence) {
		t.Fatalf("unrenewed primary write err=%v", err)
	}
	if err := runtime.Renew(context.Background()); err != nil {
		t.Fatalf("mTLS renewal err=%v", err)
	}
	if _, err := runtime.DB.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(2)}); err != nil {
		t.Fatalf("mTLS-renewed primary write err=%v", err)
	}
	status := runtime.Guard.LeaseStatus()
	if !status.Installed || status.CommitSequence != 0 || status.Epoch != 1 {
		t.Fatalf("runtime lease status=%+v", status)
	}
}

func TestPrimaryRuntimeUsesThreeMemberMTLSQuorumAuthority(t *testing.T) {
	pki := newAuthorityPKI(t)
	publicKey, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	controllerFingerprint := sha256.Sum256(pki.operator.Leaf.Raw)
	memberAuthorize, err := leasehttp.NewMTLSAuthorizer(leasehttp.MTLSConfig{PeerConsumers: map[string]string{hex.EncodeToString(controllerFingerprint[:]): "controller-a"}})
	if err != nil {
		t.Fatal(err)
	}
	controllerHTTP := pki.httpClient(&pki.operator)
	defer controllerHTTP.CloseIdleConnections()
	replicas := make([]primarylease.QuorumReplica, 0, 3)
	stores := make([]*primarylease.FileStore, 0, 3)
	servers := make([]*httptest.Server, 0, 3)
	for index := 0; index < 3; index++ {
		store, err := primarylease.NewFileStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		member, err := leasehttp.NewMember(leasehttp.MemberConfig{Store: store, Authorize: memberAuthorize})
		if err != nil {
			t.Fatal(err)
		}
		certificate := newAuthorityLeaf(t, pki.ca, pki.caKey, int64(100+index), x509.ExtKeyUsageServerAuth)
		server := httptest.NewUnstartedServer(member)
		server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots}
		server.StartTLS()
		servers = append(servers, server)
		fingerprint := sha256.Sum256(certificate.Leaf.Raw)
		client, err := leasehttp.NewClient(leasehttp.ClientOptions{Endpoint: server.URL, ExpectedServerFingerprints: []string{hex.EncodeToString(fingerprint[:])}, HTTPClient: controllerHTTP})
		if err != nil {
			t.Fatal(err)
		}
		replicas = append(replicas, primarylease.QuorumReplica{MemberID: "member-" + string(rune('a'+index)), Store: client})
		stores = append(stores, store)
	}
	for _, server := range servers {
		defer server.Close()
	}
	quorum, err := primarylease.NewQuorumStore(replicas)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{Store: quorum, PrivateKey: signingKey, LeaseDuration: 5 * time.Second, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	nodeFingerprint := sha256.Sum256(pki.nodeA.Leaf.Raw)
	operatorFingerprint := sha256.Sum256(pki.operator.Leaf.Raw)
	nodeAuthorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{hex.EncodeToString(nodeFingerprint[:]): "node-a"}})
	if err != nil {
		t.Fatal(err)
	}
	operatorAuthorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{hex.EncodeToString(operatorFingerprint[:]): "operator-a"}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(HandlerConfig{Authority: authority, NodeAuthorize: nodeAuthorize, OperatorAuthorize: operatorAuthorize})
	if err != nil {
		t.Fatal(err)
	}
	authorityServer := httptest.NewUnstartedServer(handler)
	authorityServer.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pki.server}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots}
	authorityServer.StartTLS()
	defer authorityServer.Close()
	authorityFingerprint := sha256.Sum256(pki.server.Leaf.Raw)
	nodeHTTP := pki.httpClient(&pki.nodeA)
	defer nodeHTTP.CloseIdleConnections()
	authorityClient, err := NewClient(ClientOptions{Endpoint: authorityServer.URL, ExpectedServerFingerprints: []string{hex.EncodeToString(authorityFingerprint[:])}, HTTPClient: nodeHTTP})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := primarylease.OpenPrimary(filepath.Join(t.TempDir(), "quorum-primary.meld2"), primarylease.PrimaryOptions{
		PublicKey: publicKey, GuardOptions: primarylease.GuardOptions{Owner: "node-a", Clock: func() time.Time { return now }}, RenewalClient: authorityClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if err := runtime.Renew(context.Background()); err != nil {
		t.Fatalf("quorum-backed renewal err=%v", err)
	}
	if _, err := runtime.DB.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); err != nil {
		t.Fatalf("quorum-backed primary write err=%v", err)
	}
	databaseID := runtime.DB.DatabaseID()
	persisted := 0
	for index, store := range stores {
		record, exists, err := store.LoadPrimaryLease(context.Background(), databaseID)
		if err != nil {
			t.Fatalf("member %d record=%+v exists=%t err=%v", index, record, exists, err)
		}
		if exists && record.Owner == "node-a" && record.Epoch == 1 && record.CommitSequence == 0 {
			persisted++
		}
	}
	if persisted < 2 {
		t.Fatalf("persisted quorum=%d, want at least 2", persisted)
	}
	if stats := authority.Stats(); stats.Granted != 1 || stats.StoreFailures != 0 {
		t.Fatalf("authority stats=%+v", stats)
	}
	if stats := quorum.Stats(); stats.CompareAndSwaps == 0 || stats.Loads == 0 || stats.EndpointFailures != 0 || stats.QuorumFailures != 0 {
		t.Fatalf("quorum stats=%+v", stats)
	}
}

func verifiedRequest(method, endpoint string, body []byte) *http.Request {
	request := httptest.NewRequest(method, endpoint, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.TLS = &tls.ConnectionState{Version: tls.VersionTLS13, VerifiedChains: [][]*x509.Certificate{{{Raw: []byte("verified-peer")}}}}
	return request
}

type authorityPKI struct {
	roots    *x509.CertPool
	ca       *x509.Certificate
	caKey    ed25519.PrivateKey
	server   tls.Certificate
	nodeA    tls.Certificate
	nodeB    tls.Certificate
	operator tls.Certificate
}

func newAuthorityPKI(t *testing.T) authorityPKI {
	t.Helper()
	now := time.Now().Add(-time.Minute)
	caKey := newAuthorityKey(t)
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Meldbase authority test CA"}, NotBefore: now, NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	server := newAuthorityLeaf(t, ca, caKey, 2, x509.ExtKeyUsageServerAuth)
	nodeA := newAuthorityLeaf(t, ca, caKey, 3, x509.ExtKeyUsageClientAuth)
	nodeB := newAuthorityLeaf(t, ca, caKey, 4, x509.ExtKeyUsageClientAuth)
	operator := newAuthorityLeaf(t, ca, caKey, 5, x509.ExtKeyUsageClientAuth)
	return authorityPKI{roots: roots, ca: ca, caKey: caKey, server: server, nodeA: nodeA, nodeB: nodeB, operator: operator}
}

func newAuthorityLeaf(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey, serial int64, usage x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	key := newAuthorityKey(t)
	now := time.Now().Add(-time.Minute)
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "127.0.0.1"}, NotBefore: now, NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
	if usage == x509.ExtKeyUsageServerAuth {
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func newAuthorityKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func (pki authorityPKI) httpClient(certificate *tls.Certificate) *http.Client {
	config := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pki.roots, Certificates: []tls.Certificate{*certificate}}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: config}}
}
