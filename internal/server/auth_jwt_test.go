package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestHS256JWTAuthenticatorVerifiesSignedActiveWorkspace(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	authenticator, err := NewHS256JWTAuthenticator(HS256JWTAuthenticatorConfig{
		Secret: secret, Issuer: "https://identity.example.test", Audience: "meldbase-api",
		WorkspaceClaim: "workspace_id", Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	token := signedHS256JWT(t, secret, map[string]any{
		"iss": "https://identity.example.test", "aud": []string{"other", "meldbase-api"},
		"sub": "user-1", "workspace_id": "team-a", "exp": now.Add(time.Minute).Unix(),
	})
	request, _ := http.NewRequest(http.MethodGet, "https://example.test", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	principal, err := authenticator.AuthenticateHTTP(request)
	if err != nil {
		t.Fatal(err)
	}
	if principal != (Principal{Subject: "user-1", Tenant: "team-a"}) {
		t.Fatalf("principal=%+v", principal)
	}
}

func TestHS256JWTAuthenticatorRejectsInvalidBearerClaimsAndSignature(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	authenticator, err := NewHS256JWTAuthenticator(HS256JWTAuthenticatorConfig{Secret: secret, Audience: "meldbase-api", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	validClaims := map[string]any{"sub": "user-1", "workspace_id": "team-a", "aud": "meldbase-api", "exp": now.Add(time.Minute).Unix()}
	tests := []struct {
		name   string
		header string
	}{
		{name: "missing bearer", header: "Basic credentials"},
		{name: "expired", header: "Bearer " + signedHS256JWT(t, secret, map[string]any{"sub": "user-1", "workspace_id": "team-a", "aud": "meldbase-api", "exp": now.Add(-time.Second).Unix()})},
		{name: "missing workspace", header: "Bearer " + signedHS256JWT(t, secret, map[string]any{"sub": "user-1", "aud": "meldbase-api", "exp": now.Add(time.Minute).Unix()})},
		{name: "wrong audience", header: "Bearer " + signedHS256JWT(t, secret, map[string]any{"sub": "user-1", "workspace_id": "team-a", "aud": "other", "exp": now.Add(time.Minute).Unix()})},
		{name: "wrong signature", header: "Bearer " + signedHS256JWT(t, []byte("fedcba9876543210fedcba9876543210"), validClaims)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, _ := http.NewRequest(http.MethodGet, "https://example.test", nil)
			request.Header.Set("Authorization", test.header)
			if _, err := authenticator.AuthenticateHTTP(request); err != ErrUnauthenticated {
				t.Fatalf("error=%v, want unauthenticated", err)
			}
		})
	}
}

func signedHS256JWT(t *testing.T, secret []byte, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(unsigned))
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
