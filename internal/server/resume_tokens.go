package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/crapthings/meldbase"
)

// errInvalidResumeToken reports a malformed, expired, or mismatched subscription resume token.
var errInvalidResumeToken = errors.New("meldbase server: invalid resume token")

// resumeTokenService signs and verifies expiring subscription positions with a shared secret.
type resumeTokenService struct {
	key []byte
	ttl time.Duration
	now func() time.Time
}

// resumeClaims binds a saved position to a database, actor, workspace, collection, query, and policy version.
type resumeClaims struct {
	Version       int    `json:"v"`
	Database      string `json:"db"`
	ID            string `json:"sub"`
	WorkspaceID   string `json:"workspace"`
	Collection    string `json:"collection"`
	QueryHash     string `json:"query"`
	PolicyVersion string `json:"policy"`
	Position      uint64 `json:"position"`
	Expires       int64  `json:"expires"`
}

// newResumeTokenService copies the signing key and configures the resume token lifetime.
func newResumeTokenService(key []byte, ttl time.Duration) *resumeTokenService {
	return &resumeTokenService{key: append([]byte(nil), key...), ttl: ttl, now: time.Now}
}

// issue signs an expiring resume token bound to the supplied subscription identity and position.
func (s *resumeTokenService) issue(database [16]byte, actor Actor, collection string, query meldbase.QuerySpec, policyVersion string, position uint64) (string, error) {
	queryBytes, err := meldbase.MarshalQuerySpecJSON(query)
	if err != nil || policyVersion == "" || len(policyVersion) > 128 || len(actor.ID) > 512 || len(actor.WorkspaceID) > 512 {
		return "", errInvalidResumeToken
	}
	queryHash := sha256.Sum256(queryBytes)
	claims := resumeClaims{
		Version: 1, Database: hex.EncodeToString(database[:]), ID: actor.ID, WorkspaceID: actor.WorkspaceID,
		Collection: collection, QueryHash: hex.EncodeToString(queryHash[:]), PolicyVersion: policyVersion,
		Position: position, Expires: s.now().Add(s.ttl).Unix(),
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signature := s.sign(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// validate checks a resume token's signature, expiration, and subscription binding before returning its position.
func (s *resumeTokenService) validate(token string, database [16]byte, actor Actor, collection string, query meldbase.QuerySpec, policyVersion string) (uint64, error) {
	if len(token) == 0 || len(token) > 4096 || strings.Count(token, ".") != 1 {
		return 0, errInvalidResumeToken
	}
	parts := strings.SplitN(token, ".", 2)
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(payload) > 3072 || base64.RawURLEncoding.EncodeToString(payload) != parts[0] {
		return 0, errInvalidResumeToken
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || base64.RawURLEncoding.EncodeToString(signature) != parts[1] || !hmac.Equal(signature, s.sign(payload)) {
		return 0, errInvalidResumeToken
	}
	var claims resumeClaims
	if err := decodeStrict(payload, &claims); err != nil {
		return 0, errInvalidResumeToken
	}
	queryBytes, err := meldbase.MarshalQuerySpecJSON(query)
	if err != nil {
		return 0, errInvalidResumeToken
	}
	queryHash := sha256.Sum256(queryBytes)
	if claims.Version != 1 || claims.Database != hex.EncodeToString(database[:]) || claims.ID != actor.ID ||
		claims.WorkspaceID != actor.WorkspaceID || claims.Collection != collection || claims.QueryHash != hex.EncodeToString(queryHash[:]) ||
		claims.PolicyVersion != policyVersion || claims.Expires <= s.now().Unix() {
		return 0, errInvalidResumeToken
	}
	return claims.Position, nil
}

// sign computes the HMAC-SHA256 signature of a resume token payload.
func (s *resumeTokenService) sign(payload []byte) []byte {
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}
