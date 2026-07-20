package leasehttp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crapthings/meldbase/integrations/primarylease"
)

func TestClientRequiresActualVerifiedMTLSMember(t *testing.T) {
	pki := newLeaseTestPKI(t)
	fingerprint := sha256.Sum256(pki.clientLeaf.Raw)
	authorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{hex.EncodeToString(fingerprint[:]): "controller-a"}})
	if err != nil {
		t.Fatal(err)
	}
	member, err := NewMember(MemberConfig{Store: primarylease.NewMemoryStore(), Authorize: authorize})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(member)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pki.server}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots}
	server.StartTLS()
	defer server.Close()

	withoutCertificate := pki.httpClient(nil)
	defer withoutCertificate.CloseIdleConnections()
	serverFingerprint := sha256.Sum256(pki.server.Leaf.Raw)
	client, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprint: hex.EncodeToString(serverFingerprint[:]), HTTPClient: withoutCertificate})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.LoadPrimaryLease(context.Background(), [16]byte{12: 1}); err == nil {
		t.Fatal("member accepted a client without a verified certificate")
	}

	withCertificate := pki.httpClient(&pki.client)
	defer withCertificate.CloseIdleConnections()
	client, err = NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprint: hex.EncodeToString(serverFingerprint[:]), HTTPClient: withCertificate})
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{12: 1}
	if _, exists, err := client.LoadPrimaryLease(context.Background(), databaseID); err != nil || exists {
		t.Fatalf("verified initial load exists=%t err=%v", exists, err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	next := primarylease.LeaseRecord{DatabaseID: databaseID, Owner: "writer-a", Epoch: 1, NotAfter: now.Add(time.Minute)}
	if swapped, err := client.CompareAndSwapPrimaryLease(context.Background(), databaseID, nil, next); err != nil || !swapped {
		t.Fatalf("verified CAS swapped=%t err=%v", swapped, err)
	}
}

func TestThreeVerifiedHTTPSMembersBackAuthorityQuorum(t *testing.T) {
	pki := newLeaseTestPKI(t)
	clientFingerprint := sha256.Sum256(pki.clientLeaf.Raw)
	authorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{hex.EncodeToString(clientFingerprint[:]): "controller-a"}})
	if err != nil {
		t.Fatal(err)
	}
	memberClient := pki.httpClient(&pki.client)
	defer memberClient.CloseIdleConnections()
	replicas := make([]primarylease.QuorumReplica, 0, 3)
	servers := make([]*httptest.Server, 0, 3)
	for index := 0; index < 3; index++ {
		store, err := primarylease.NewFileStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		member, err := NewMember(MemberConfig{Store: store, Authorize: authorize})
		if err != nil {
			t.Fatal(err)
		}
		serverCertificate, leaf := newLeaseTestServerLeafWithSerial(t, pki.ca, pki.caKey, int64(10+index))
		server := httptest.NewUnstartedServer(member)
		server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCertificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots}
		server.StartTLS()
		servers = append(servers, server)
		fingerprint := sha256.Sum256(leaf.Raw)
		client, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprint: hex.EncodeToString(fingerprint[:]), HTTPClient: memberClient})
		if err != nil {
			t.Fatal(err)
		}
		replicas = append(replicas, primarylease.QuorumReplica{MemberID: "member-" + string(rune('a'+index)), Store: client})
	}
	for _, server := range servers {
		defer server.Close()
	}
	quorum, err := primarylease.NewQuorumStore(replicas)
	if err != nil {
		t.Fatal(err)
	}
	_, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	authority, err := primarylease.NewAuthority(primarylease.AuthorityOptions{Store: quorum, PrivateKey: signingKey, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	databaseID := [16]byte{12: 2}
	grant, err := authority.Grant(context.Background(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: "writer-a", CommitSequence: 3})
	if err != nil || grant.Certificate == "" || grant.Record.Epoch != 1 {
		t.Fatalf("network quorum grant=%+v err=%v", grant, err)
	}
	if record, exists, err := quorum.LoadPrimaryLease(context.Background(), databaseID); err != nil || !exists || record != grant.Record {
		t.Fatalf("network quorum record=%+v exists=%t err=%v", record, exists, err)
	}
}

func TestClientAcceptsOnlyConfiguredLeavesDuringRotation(t *testing.T) {
	pki := newLeaseTestPKI(t)
	clientFingerprint := sha256.Sum256(pki.clientLeaf.Raw)
	authorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{hex.EncodeToString(clientFingerprint[:]): "controller-a"}})
	if err != nil {
		t.Fatal(err)
	}
	member, err := NewMember(MemberConfig{Store: primarylease.NewMemoryStore(), Authorize: authorize})
	if err != nil {
		t.Fatal(err)
	}
	oldCertificate := pki.server
	newCertificate, newLeaf := newLeaseTestServerLeafWithSerial(t, pki.ca, pki.caKey, 99)
	var useNew atomic.Bool
	oldTLS := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{oldCertificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots}
	newTLS := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{newCertificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots}
	server := httptest.NewUnstartedServer(member)
	server.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{oldCertificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pki.roots,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			if useNew.Load() {
				return newTLS, nil
			}
			return oldTLS, nil
		},
	}
	server.StartTLS()
	defer server.Close()

	oldFingerprint := sha256.Sum256(oldCertificate.Leaf.Raw)
	newFingerprint := sha256.Sum256(newLeaf.Raw)
	httpClient := pki.httpClient(&pki.client)
	defer httpClient.CloseIdleConnections()
	client, err := NewClient(ClientOptions{
		Endpoint: server.URL,
		ExpectedServerFingerprints: []string{
			hex.EncodeToString(newFingerprint[:]),
			hex.EncodeToString(oldFingerprint[:]),
		},
		HTTPClient: httpClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	identities := client.PrimaryLeaseMemberIdentities()
	if len(identities) != 2 || identities[0] >= identities[1] {
		t.Fatalf("rotation identities=%q", identities)
	}
	databaseID := [16]byte{12: 3}
	if _, exists, err := client.LoadPrimaryLease(context.Background(), databaseID); err != nil || exists {
		t.Fatalf("old leaf request exists=%t err=%v", exists, err)
	}
	useNew.Store(true)
	httpClient.CloseIdleConnections()
	if _, exists, err := client.LoadPrimaryLease(context.Background(), databaseID); err != nil || exists {
		t.Fatalf("new leaf request exists=%t err=%v", exists, err)
	}
	oldOnly, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprint: hex.EncodeToString(oldFingerprint[:]), HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	httpClient.CloseIdleConnections()
	if _, _, err := oldOnly.LoadPrimaryLease(context.Background(), databaseID); !errors.Is(err, ErrHTTPSRequired) {
		t.Fatalf("old-only client accepted unpinned rotated leaf: %v", err)
	}
	alias, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprints: []string{hex.EncodeToString(newFingerprint[:])}, HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	third, err := NewClient(ClientOptions{Endpoint: server.URL, ExpectedServerFingerprints: []string{hex.EncodeToString(oldFingerprint[:])}, HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := primarylease.NewQuorumStore([]primarylease.QuorumReplica{{MemberID: "member-a", Store: client}, {MemberID: "member-b", Store: alias}, {MemberID: "member-c", Store: third}}); err == nil {
		t.Fatal("overlapping rotation pins formed several quorum votes")
	}
}

type leaseTestPKI struct {
	roots      *x509.CertPool
	ca         *x509.Certificate
	caKey      ed25519.PrivateKey
	server     tls.Certificate
	client     tls.Certificate
	clientLeaf *x509.Certificate
}

func newLeaseTestPKI(t *testing.T) leaseTestPKI {
	t.Helper()
	now := time.Now().Add(-time.Minute)
	caKey := newLeaseTestKey(t)
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Meldbase lease test CA"}, NotBefore: now, NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
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
	server, _ := newLeaseTestServerLeaf(t, ca, caKey)
	client, clientLeaf := newLeaseTestClientLeaf(t, ca, caKey)
	return leaseTestPKI{roots: roots, ca: ca, caKey: caKey, server: server, client: client, clientLeaf: clientLeaf}
}

func newLeaseTestServerLeaf(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey) (tls.Certificate, *x509.Certificate) {
	return newLeaseTestServerLeafWithSerial(t, ca, caKey, 2)
}

func newLeaseTestServerLeafWithSerial(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey, serial int64) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	key := newLeaseTestKey(t)
	now := time.Now().Add(-time.Minute)
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "127.0.0.1"}, NotBefore: now, NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	return signLeaseTestLeaf(t, template, ca, caKey, key)
}

func newLeaseTestClientLeaf(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	key := newLeaseTestKey(t)
	now := time.Now().Add(-time.Minute)
	template := &x509.Certificate{SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "client"}, NotBefore: now, NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	return signLeaseTestLeaf(t, template, ca, caKey, key)
}

func signLeaseTestLeaf(t *testing.T, template, ca *x509.Certificate, caKey, key ed25519.PrivateKey) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, leaf
}

func newLeaseTestKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func (pki leaseTestPKI) httpClient(client *tls.Certificate) *http.Client {
	config := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pki.roots}
	if client != nil {
		config.Certificates = []tls.Certificate{*client}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: config}}
}
