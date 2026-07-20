package replicationws

import "github.com/crapthings/meldbase/integrations/replicationauth"

// MTLSConfig maps the SHA-256 fingerprint of an already verified peer leaf
// certificate (lowercase hex over certificate.Raw) to its stable durable
// consumer name. A fingerprint avoids ambiguous subject/SAN matching and lets
// operators rotate identities with an explicit configuration change.
type MTLSConfig = replicationauth.MTLSConfig

// NewMTLSAuthorizer returns an Authorize callback for a net/http server whose
// TLS configuration requires and verifies client certificates. This helper does
// not configure TLS itself: callers must set tls.Config.ClientAuth and trusted
// roots on the server before requests reach the handler.
func NewMTLSAuthorizer(config MTLSConfig) (Authorize, error) {
	return replicationauth.NewMTLSAuthorizer(config)
}
