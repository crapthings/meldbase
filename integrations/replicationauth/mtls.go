// Package replicationauth verifies peer identities for server-to-server replication.
package replicationauth

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"net/http"
)

// Authorize verifies a replication peer and returns its server-assigned durable consumer name.
//
// Transport adapters must never take that name from an
// untrusted frame, URL, or request header.
type Authorize func(*http.Request) (consumerName string, err error)

// MTLSConfig maps verified client certificate fingerprints to durable consumer names.
//
// Fingerprints are lowercase SHA-256 hashes of certificate.Raw. They avoid ambiguous subject/SAN matching and make rotation an
// explicit configuration change.
type MTLSConfig struct {
	PeerConsumers map[string]string
}

// ErrTLSUnverified reports a connection without TLS 1.2 or later and a verified peer chain.
var ErrTLSUnverified = errors.New("replication peer TLS connection is not verified")

// RequireVerifiedTLS checks for TLS 1.2 or later and a verified peer certificate chain.
//
// Server-side adapters retain an Authorize callback because deployments may use this package's strict mTLS
// authorizer or an equivalent private identity system.
func RequireVerifiedTLS(state *tls.ConnectionState) error {
	if state == nil || state.Version < tls.VersionTLS12 || len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return ErrTLSUnverified
	}
	return nil
}

// NewMTLSAuthorizer creates a peer authorizer using verified client certificate fingerprints.
//
// The enclosing net/http server must require and verify client certificates.
// This function does not configure TLS: callers must set ClientAuth and
// trusted roots before a request reaches a replication handler.
func NewMTLSAuthorizer(config MTLSConfig) (Authorize, error) {
	if len(config.PeerConsumers) == 0 || len(config.PeerConsumers) > 1024 {
		return nil, errors.New("mTLS replication authorizer requires between one and 1024 peers")
	}
	peers := make(map[[sha256.Size]byte]string, len(config.PeerConsumers))
	for encoded, consumer := range config.PeerConsumers {
		if !validConsumerName(consumer) {
			return nil, errors.New("mTLS replication consumer name is invalid")
		}
		if len(encoded) != sha256.Size*2 {
			return nil, errors.New("mTLS replication fingerprint is invalid")
		}
		decoded, err := hex.DecodeString(encoded)
		if err != nil || hex.EncodeToString(decoded) != encoded {
			return nil, errors.New("mTLS replication fingerprint must be lowercase SHA-256 hex")
		}
		var fingerprint [sha256.Size]byte
		copy(fingerprint[:], decoded)
		if _, duplicate := peers[fingerprint]; duplicate {
			return nil, errors.New("mTLS replication fingerprint is duplicated")
		}
		peers[fingerprint] = consumer
	}
	return func(request *http.Request) (string, error) {
		if request == nil || request.TLS == nil || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.VerifiedChains[0]) == 0 {
			return "", errors.New("replication peer certificate is not verified")
		}
		leaf := request.TLS.VerifiedChains[0][0]
		if leaf == nil || len(leaf.Raw) == 0 {
			return "", errors.New("replication peer certificate is invalid")
		}
		fingerprint := sha256.Sum256(leaf.Raw)
		consumer, exists := peers[fingerprint]
		if !exists {
			return "", errors.New("replication peer certificate is not authorized")
		}
		return consumer, nil
	}, nil
}

// validConsumerName accepts bounded names containing only letters, digits, underscores, and hyphens.
func validConsumerName(name string) bool {
	if len(name) == 0 || len(name) > 128 {
		return false
	}
	for index := range len(name) {
		value := name[index]
		if !((value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9') || value == '_' || value == '-') {
			return false
		}
	}
	return true
}
