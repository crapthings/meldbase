package replicationws

import "github.com/crapthings/meldbase/integrations/replicationauth"

// MTLSConfig maps verified client certificate fingerprints to durable consumer names.
//
// Fingerprints are lowercase SHA-256 hashes of certificate.Raw. They avoid
// ambiguous subject/SAN matching and let
// operators rotate identities with an explicit configuration change.
type MTLSConfig = replicationauth.MTLSConfig

// NewMTLSAuthorizer creates a peer authorizer using verified client certificate fingerprints.
//
// The enclosing net/http server must require and verify client certificates.
// This helper does
// not configure TLS itself: callers must set tls.Config.ClientAuth and trusted
// roots on the server before requests reach the handler.
func NewMTLSAuthorizer(config MTLSConfig) (Authorize, error) {
	return replicationauth.NewMTLSAuthorizer(config)
}
