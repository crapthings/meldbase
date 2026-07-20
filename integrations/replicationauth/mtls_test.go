package replicationauth

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net/http/httptest"
	"testing"
)

func TestMTLSAuthorizerBindsVerifiedLeafFingerprint(t *testing.T) {
	raw := []byte("verified-leaf-certificate")
	fingerprint := sha256.Sum256(raw)
	authorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{hex.EncodeToString(fingerprint[:]): "replica-a"}})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "https://example/replication", nil)
	request.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{Raw: raw}}}}
	consumer, err := authorize(request)
	if err != nil || consumer != "replica-a" {
		t.Fatalf("verified consumer=%q err=%v", consumer, err)
	}
	request.TLS.VerifiedChains[0][0].Raw = []byte("different")
	if consumer, err := authorize(request); err == nil || consumer != "" {
		t.Fatalf("unrecognized certificate consumer=%q err=%v", consumer, err)
	}
}

func TestMTLSAuthorizerRejectsUnsafeConfigurationAndUnverifiedRequest(t *testing.T) {
	if _, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{"ABC": "peer"}}); err == nil {
		t.Fatal("accepted malformed fingerprint")
	}
	if _, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{"0000000000000000000000000000000000000000000000000000000000000000": "has space"}}); err == nil {
		t.Fatal("accepted unsafe consumer name")
	}
	authorize, err := NewMTLSAuthorizer(MTLSConfig{PeerConsumers: map[string]string{"0000000000000000000000000000000000000000000000000000000000000000": "peer"}})
	if err != nil {
		t.Fatal(err)
	}
	if consumer, err := authorize(httptest.NewRequest("GET", "https://example/replication", nil)); err == nil || consumer != "" {
		t.Fatalf("unverified request consumer=%q err=%v", consumer, err)
	}
}

func TestRequireVerifiedTLSRejectsUnauthenticatedOrObsoletePeer(t *testing.T) {
	for _, state := range []*tls.ConnectionState{
		nil,
		{Version: tls.VersionTLS11},
		{Version: tls.VersionTLS13},
	} {
		if err := RequireVerifiedTLS(state); !errors.Is(err, ErrTLSUnverified) {
			t.Fatalf("state=%#v err=%v", state, err)
		}
	}
	if err := RequireVerifiedTLS(&tls.ConnectionState{Version: tls.VersionTLS13, VerifiedChains: [][]*x509.Certificate{{{Raw: []byte("verified")}}}}); err != nil {
		t.Fatal(err)
	}
}
