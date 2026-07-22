package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// HS256JWTAuthenticatorConfig configures a locally verified JWT issuer. It is
// useful when an identity service signs short-lived access tokens with a shared
// secret. OIDC/JWKS verification can use the same Actor contract later;
// callers never supply a workspace separately from the signed token.
type HS256JWTAuthenticatorConfig struct {
	Secret         []byte
	Issuer         string
	Audience       string
	WorkspaceClaim string
	Clock          func() time.Time
}

// HS256JWTAuthenticator verifies a bounded Bearer JWT and maps its `sub` and
// active workspace claim to the server Actor.
type HS256JWTAuthenticator struct {
	secret         []byte
	issuer         string
	audience       string
	workspaceClaim string
	now            func() time.Time
}

func NewHS256JWTAuthenticator(config HS256JWTAuthenticatorConfig) (*HS256JWTAuthenticator, error) {
	if len(config.Secret) < 32 {
		return nil, errors.New("JWT HS256 secret must contain at least 32 bytes")
	}
	if config.Issuer == "" || config.Audience == "" {
		return nil, errors.New("JWT HS256 authentication requires issuer and audience")
	}
	if config.WorkspaceClaim == "" {
		config.WorkspaceClaim = "workspace_id"
	}
	if !validWorkspaceClaim(config.WorkspaceClaim) {
		return nil, errors.New("JWT workspace claim must be a non-reserved simple claim name")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &HS256JWTAuthenticator{
		secret: append([]byte(nil), config.Secret...), issuer: config.Issuer,
		audience: config.Audience, workspaceClaim: config.WorkspaceClaim, now: config.Clock,
	}, nil
}

func (a *HS256JWTAuthenticator) AuthenticateHTTP(request *http.Request) (Actor, error) {
	token, ok := bearerToken(request.Header.Get("Authorization"))
	if !ok {
		return Actor{}, ErrUnauthenticated
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || len(token) > 8192 {
		return Actor{}, ErrUnauthenticated
	}
	header, ok := decodeJWTPart(parts[0], 1024)
	algorithm, _, headerOK := decodeJWTHeader(header)
	if !ok || !headerOK || algorithm != "HS256" {
		return Actor{}, ErrUnauthenticated
	}
	signature, ok := decodeJWTPart(parts[2], sha256.Size)
	if !ok || len(signature) != sha256.Size {
		return Actor{}, ErrUnauthenticated
	}
	mac := hmac.New(sha256.New, a.secret)
	_, _ = mac.Write([]byte(parts[0]))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write([]byte(parts[1]))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return Actor{}, ErrUnauthenticated
	}
	payload, ok := decodeJWTPart(parts[1], 6144)
	if !ok {
		return Actor{}, ErrUnauthenticated
	}
	claims, ok := decodeJWTClaims(payload)
	if !ok || !validJWTClaims(claims, a.issuer, a.audience, a.now) {
		return Actor{}, ErrUnauthenticated
	}
	actorID, actorIDOK := claimString(claims, "sub")
	workspaceID, workspaceIDOK := claimString(claims, a.workspaceClaim)
	if !actorIDOK || !workspaceIDOK || !validActorPart(actorID) || !validActorPart(workspaceID) {
		return Actor{}, ErrUnauthenticated
	}
	return Actor{ID: actorID, WorkspaceID: workspaceID}, nil
}

func bearerToken(header string) (string, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || len(parts[1]) == 0 {
		return "", false
	}
	return parts[1], true
}

func decodeJWTPart(encoded string, max int) ([]byte, bool) {
	if encoded == "" || len(encoded) > max*2 || strings.Contains(encoded, "=") {
		return nil, false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(decoded) == 0 || len(decoded) > max || base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		return nil, false
	}
	return decoded, true
}

func decodeJWTHeader(raw []byte) (algorithm, keyID string, ok bool) {
	var header map[string]json.RawMessage
	if err := json.Unmarshal(raw, &header); err != nil {
		return "", "", false
	}
	algorithm, ok = claimString(header, "alg")
	if !ok || algorithm == "" {
		return "", "", false
	}
	keyID, _ = claimString(header, "kid")
	return algorithm, keyID, len(keyID) <= 256
}

func decodeJWTClaims(raw []byte) (map[string]json.RawMessage, bool) {
	var claims map[string]json.RawMessage
	if err := json.Unmarshal(raw, &claims); err != nil || claims == nil {
		return nil, false
	}
	return claims, true
}

func validJWTClaims(claims map[string]json.RawMessage, expectedIssuer, expectedAudience string, now func() time.Time) bool {
	if expectedIssuer != "" {
		issuer, ok := claimString(claims, "iss")
		if !ok || issuer != expectedIssuer {
			return false
		}
	}
	if expectedAudience != "" && !claimAudienceContains(claims, expectedAudience) {
		return false
	}
	expires, ok := claimUnix(claims, "exp")
	if !ok || expires <= now().Unix() {
		return false
	}
	if notBefore, present := claimUnix(claims, "nbf"); present && notBefore > now().Unix() {
		return false
	}
	return true
}

func claimString(claims map[string]json.RawMessage, name string) (string, bool) {
	raw, found := claims[name]
	if !found {
		return "", false
	}
	var value string
	return value, json.Unmarshal(raw, &value) == nil
}

func claimUnix(claims map[string]json.RawMessage, name string) (int64, bool) {
	raw, found := claims[name]
	if !found {
		return 0, false
	}
	var value json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return 0, false
	}
	parsed, err := strconv.ParseInt(string(value), 10, 64)
	return parsed, err == nil
}

func claimAudienceContains(claims map[string]json.RawMessage, expected string) bool {
	raw, found := claims["aud"]
	if !found {
		return false
	}
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return single == expected
	}
	var many []string
	if json.Unmarshal(raw, &many) != nil {
		return false
	}
	for _, value := range many {
		if value == expected {
			return true
		}
	}
	return false
}

func validActorPart(value string) bool {
	return value != "" && len(value) <= 512 && utf8.ValidString(value)
}

func validWorkspaceClaim(value string) bool {
	if !workspaceIdentifier.MatchString(value) {
		return false
	}
	switch value {
	case "sub", "iss", "aud", "exp", "nbf", "iat", "jti":
		return false
	default:
		return true
	}
}
