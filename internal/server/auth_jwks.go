package server

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// RS256JWKSAuthenticatorConfig configures verification against an OIDC-style
// JSON Web Key Set. Issuer and audience are required so a token minted for a
// different API cannot become a Meldbase credential.
type RS256JWKSAuthenticatorConfig struct {
	JWKSURL        string
	Issuer         string
	Audience       string
	WorkspaceClaim string
	HTTPClient     *http.Client
	Clock          func() time.Time
	CacheTTL       time.Duration
}

type RS256JWKSAuthenticator struct {
	url            string
	issuer         string
	audience       string
	workspaceClaim string
	client         *http.Client
	now            func() time.Time
	cacheTTL       time.Duration

	mu      sync.Mutex
	keys    map[string]*rsa.PublicKey
	expires time.Time
}

func NewRS256JWKSAuthenticator(config RS256JWKSAuthenticatorConfig) (*RS256JWKSAuthenticator, error) {
	parsed, err := url.Parse(config.JWKSURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return nil, errors.New("JWT JWKS URL must be an absolute HTTPS URL without credentials or fragment")
	}
	if config.Issuer == "" || config.Audience == "" {
		return nil, errors.New("JWT JWKS authentication requires issuer and audience")
	}
	if config.WorkspaceClaim == "" {
		config.WorkspaceClaim = "workspace_id"
	}
	if !validWorkspaceClaim(config.WorkspaceClaim) {
		return nil, errors.New("JWT workspace claim must be a non-reserved simple claim name")
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = 5 * time.Minute
	}
	if config.CacheTTL < time.Second || config.CacheTTL > time.Hour {
		return nil, errors.New("JWT JWKS cache TTL must be between one second and one hour")
	}
	return &RS256JWKSAuthenticator{
		url: config.JWKSURL, issuer: config.Issuer, audience: config.Audience, workspaceClaim: config.WorkspaceClaim,
		client: config.HTTPClient, now: config.Clock, cacheTTL: config.CacheTTL, keys: make(map[string]*rsa.PublicKey),
	}, nil
}

func (a *RS256JWKSAuthenticator) AuthenticateHTTP(request *http.Request) (Actor, error) {
	token, ok := bearerToken(request.Header.Get("Authorization"))
	if !ok || len(token) > 8192 {
		return Actor{}, ErrUnauthenticated
	}
	parts := splitJWT(token)
	if parts == nil {
		return Actor{}, ErrUnauthenticated
	}
	header, ok := decodeJWTPart(parts[0], 1024)
	algorithm, keyID, headerOK := decodeJWTHeader(header)
	if !ok || !headerOK || algorithm != "RS256" || keyID == "" {
		return Actor{}, ErrUnauthenticated
	}
	payload, ok := decodeJWTPart(parts[1], 6144)
	signature, signatureOK := decodeJWTPart(parts[2], 1024)
	if !ok || !signatureOK {
		return Actor{}, ErrUnauthenticated
	}
	key, err := a.keyFor(request.Context(), keyID)
	if err != nil {
		return Actor{}, ErrUnauthenticated
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return Actor{}, ErrUnauthenticated
	}
	claims, ok := decodeJWTClaims(payload)
	if !ok || !validJWTClaims(claims, a.issuer, a.audience, a.now) {
		return Actor{}, ErrUnauthenticated
	}
	actorID, actorIDOK := claimString(claims, "sub")
	tenantID, tenantIDOK := claimString(claims, a.workspaceClaim)
	if !actorIDOK || !tenantIDOK || !validActorPart(actorID) || !validActorPart(tenantID) {
		return Actor{}, ErrUnauthenticated
	}
	return Actor{ID: actorID, TenantID: tenantID}, nil
}

func splitJWT(token string) []string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil
	}
	return parts
}

func (a *RS256JWKSAuthenticator) keyFor(ctx context.Context, keyID string) (*rsa.PublicKey, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.now().Before(a.expires) {
		if key := a.keys[keyID]; key != nil {
			return key, nil
		}
	}
	if err := a.refreshLocked(ctx); err != nil {
		return nil, err
	}
	key := a.keys[keyID]
	if key == nil {
		return nil, errors.New("JWT key ID is unknown")
	}
	return key, nil
}

func (a *RS256JWKSAuthenticator) refreshLocked(ctx context.Context) error {
	requestContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, a.url, nil)
	if err != nil {
		return err
	}
	response, err := a.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.Request == nil || response.Request.URL == nil || response.Request.URL.Scheme != "https" || response.Request.URL.Host == "" {
		return errors.New("JWT JWKS endpoint redirected outside HTTPS")
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("JWT JWKS endpoint returned non-200 status")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20+1))
	if err != nil || len(body) > 1<<20 {
		return errors.New("JWT JWKS response is invalid or too large")
	}
	var document struct {
		Keys []struct {
			KeyID string `json:"kid"`
			Type  string `json:"kty"`
			Use   string `json:"use"`
			Alg   string `json:"alg"`
			N     string `json:"n"`
			E     string `json:"e"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(body, &document); err != nil || len(document.Keys) == 0 || len(document.Keys) > 128 {
		return errors.New("JWT JWKS document is invalid")
	}
	keys := make(map[string]*rsa.PublicKey, len(document.Keys))
	for _, encoded := range document.Keys {
		if encoded.KeyID == "" || len(encoded.KeyID) > 256 || encoded.Type != "RSA" || (encoded.Use != "" && encoded.Use != "sig") || (encoded.Alg != "" && encoded.Alg != "RS256") {
			continue
		}
		key, ok := decodeRSAJWK(encoded.N, encoded.E)
		if !ok || keys[encoded.KeyID] != nil {
			continue
		}
		keys[encoded.KeyID] = key
	}
	if len(keys) == 0 {
		return errors.New("JWT JWKS has no supported signing keys")
	}
	a.keys, a.expires = keys, a.now().Add(a.cacheTTL)
	return nil
}

func decodeRSAJWK(encodedN, encodedE string) (*rsa.PublicKey, bool) {
	n, err := base64.RawURLEncoding.DecodeString(encodedN)
	if err != nil || len(n) == 0 || base64.RawURLEncoding.EncodeToString(n) != encodedN {
		return nil, false
	}
	e, err := base64.RawURLEncoding.DecodeString(encodedE)
	if err != nil || len(e) == 0 || len(e) > 4 || base64.RawURLEncoding.EncodeToString(e) != encodedE {
		return nil, false
	}
	exponent := 0
	for _, byteValue := range e {
		exponent = exponent<<8 | int(byteValue)
	}
	modulus := new(big.Int).SetBytes(n)
	if modulus.BitLen() < 2048 || exponent < 3 || exponent%2 == 0 {
		return nil, false
	}
	return &rsa.PublicKey{N: modulus, E: exponent}, true
}
