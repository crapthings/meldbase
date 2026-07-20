package server

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRS256JWKSAuthenticatorVerifiesOIDCTokenAndCachesKeySet(t *testing.T) {
	private, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	jwks := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kid": "key-1", "kty": "RSA", "alg": "RS256", "use": "sig",
			"n": base64.RawURLEncoding.EncodeToString(private.PublicKey.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString([]byte{1, 0, 1}),
		}}})
	}))
	defer jwks.Close()
	now := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	authenticator, err := NewRS256JWKSAuthenticator(RS256JWKSAuthenticatorConfig{
		JWKSURL: jwks.URL, Issuer: "https://identity.example.test", Audience: "meldbase-api",
		HTTPClient: jwks.Client(), Clock: func() time.Time { return now }, CacheTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	token := signedRS256JWT(t, private, "key-1", map[string]any{
		"iss": "https://identity.example.test", "aud": "meldbase-api", "sub": "user-1",
		"workspace_id": "team-a", "exp": now.Add(time.Minute).Unix(),
	})
	for range 2 {
		request, _ := http.NewRequest(http.MethodGet, "https://example.test", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		principal, authenticateErr := authenticator.AuthenticateHTTP(request)
		if authenticateErr != nil || principal != (Principal{Subject: "user-1", Tenant: "team-a"}) {
			t.Fatalf("principal=%+v error=%v", principal, authenticateErr)
		}
	}
	if requests.Load() != 1 {
		t.Fatalf("JWKS requests=%d, want 1", requests.Load())
	}
}

func TestRS256JWKSAuthenticatorRequiresHTTPSIssuerAndAudience(t *testing.T) {
	if _, err := NewRS256JWKSAuthenticator(RS256JWKSAuthenticatorConfig{JWKSURL: "http://identity.example.test/keys", Issuer: "issuer", Audience: "audience"}); err == nil {
		t.Fatal("HTTP JWKS URL accepted")
	}
	if _, err := NewRS256JWKSAuthenticator(RS256JWKSAuthenticatorConfig{JWKSURL: "https://identity.example.test/keys", Audience: "audience"}); err == nil {
		t.Fatal("missing issuer accepted")
	}
}

func signedRS256JWT(t *testing.T, private *rsa.PrivateKey, keyID string, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": keyID, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, private, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}
