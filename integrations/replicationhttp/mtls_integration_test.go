package replicationhttp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
)

func TestFetchRequiresActualVerifiedMTLSClientCertificate(t *testing.T) {
	directory := t.TempDir()
	db, err := meldbase.OpenV2(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); err != nil {
		t.Fatal(err)
	}
	pki := newReplicationTestPKI(t)
	fingerprint := sha256.Sum256(pki.clientLeaf.Raw)
	authorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{hex.EncodeToString(fingerprint[:]): "verified-peer"}})
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSource(SourceConfig{DB: db, Authorize: authorize, ArchiveDirectory: directory, Buffer: 2})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(source)
	server.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{pki.server},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pki.roots,
	}
	server.StartTLS()
	defer server.Close()

	withoutCertificate := pki.httpClient(nil)
	defer withoutCertificate.CloseIdleConnections()
	missingDestination := filepath.Join(directory, "missing-client-cert.meld2")
	if _, err := Fetch(context.Background(), FetchConfig{URL: server.URL, HTTPClient: withoutCertificate, Destination: missingDestination}); err == nil {
		t.Fatal("bootstrap accepted a client without a verified certificate")
	}
	if _, err := os.Stat(missingDestination); !os.IsNotExist(err) {
		t.Fatalf("missing-client-cert destination=%v", err)
	}

	withCertificate := pki.httpClient(&pki.client)
	defer withCertificate.CloseIdleConnections()
	destination := filepath.Join(directory, "verified-client-cert.meld2")
	bootstrap, err := Fetch(context.Background(), FetchConfig{URL: server.URL, HTTPClient: withCertificate, Destination: destination})
	if err != nil {
		t.Fatal(err)
	}
	if bootstrap.Backup.Bytes == 0 || bootstrap.SnapshotToken != bootstrap.Backup.CommitSequence {
		t.Fatalf("verified mTLS bootstrap=%+v", bootstrap)
	}
}

type replicationTestPKI struct {
	roots      *x509.CertPool
	server     tls.Certificate
	client     tls.Certificate
	clientLeaf *x509.Certificate
}

func newReplicationTestPKI(t *testing.T) replicationTestPKI {
	t.Helper()
	now := time.Now().Add(-time.Minute)
	caKey := newReplicationTestKey(t)
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Meldbase replication test CA"},
		NotBefore: now, NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
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
	server, _ := newReplicationTestServerLeaf(t, ca, caKey)
	client, clientLeaf := newReplicationTestLeaf(t, ca, caKey, 3, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	return replicationTestPKI{roots: roots, server: server, client: client, clientLeaf: clientLeaf}
}

func newReplicationTestServerLeaf(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	key := newReplicationTestKey(t)
	now := time.Now().Add(-time.Minute)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "127.0.0.1"}, NotBefore: now, NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	return signReplicationTestLeaf(t, template, ca, caKey, key)
}

func newReplicationTestLeaf(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey, serial int64, name string, usages []x509.ExtKeyUsage) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	key := newReplicationTestKey(t)
	now := time.Now().Add(-time.Minute)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, NotBefore: now, NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usages,
	}
	return signReplicationTestLeaf(t, template, ca, caKey, key)
}

func signReplicationTestLeaf(t *testing.T, template, ca *x509.Certificate, caKey, key ed25519.PrivateKey) (tls.Certificate, *x509.Certificate) {
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

func newReplicationTestKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func (pki replicationTestPKI) httpClient(client *tls.Certificate) *http.Client {
	config := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pki.roots}
	if client != nil {
		config.Certificates = []tls.Certificate{*client}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: config}}
}
