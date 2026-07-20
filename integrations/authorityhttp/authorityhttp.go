// Package authorityhttp exposes a narrow, mTLS-authenticated primary-lease
// Authority control endpoint. It is intentionally separate from the public
// database HTTP/RPC surface and from the quorum-member LeaseStore endpoint.
package authorityhttp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/crapthings/meldbase/integrations/primarylease"
	"github.com/crapthings/meldbase/integrations/replicationauth"
)

const (
	protocolVersion   uint32 = 1
	grantPath                = "/v1/authority/grant/"
	revokePath               = "/v1/authority/revoke/"
	maximumBodyBytes  int64  = 1024
	maximumServerPins        = 4
)

var (
	// ErrHTTPSRequired means the client did not obtain a verified, pinned HTTPS
	// response. It is fatal for a caller: treating it as a temporary controller
	// outage could hide an identity configuration error.
	ErrHTTPSRequired = errors.New("meldbase authority HTTP: verified pinned HTTPS is required")
	// ErrProtocol means an authenticated peer violated this strict control-plane
	// protocol. It is fatal for a caller and never permits a blind retry.
	ErrProtocol = errors.New("meldbase authority HTTP: invalid protocol response")
)

type fatalError struct{ err error }

func (err fatalError) Error() string { return err.err.Error() }
func (err fatalError) Unwrap() error { return err.err }

func fatal(err error) error {
	if err == nil {
		return nil
	}
	return fatalError{err: err}
}

// Authorize returns an identity derived from a verified server-to-server peer
// certificate. It must never take that identity from an HTTP field.
type Authorize = replicationauth.Authorize
type MTLSConfig = replicationauth.MTLSConfig

func NewMTLSAuthorizer(config MTLSConfig) (Authorize, error) {
	return replicationauth.NewMTLSAuthorizer(config)
}

// HandlerConfig configures the private Authority API. NodeAuthorize maps each
// verified node certificate to the immutable lease Owner used by Grant. Revoke
// requires a separately configured operator authorizer; granting node identity
// alone never conveys the ability to fence another primary.
type HandlerConfig struct {
	Authority         *primarylease.Authority
	NodeAuthorize     Authorize
	OperatorAuthorize Authorize
}

// Handler serves one controller issuer. It does not expose promotion because
// promotion must execute a deployment-specific, local history-readiness proof.
type Handler struct {
	authority         *primarylease.Authority
	nodeAuthorize     Authorize
	operatorAuthorize Authorize
}

func NewHandler(config HandlerConfig) (*Handler, error) {
	if config.Authority == nil || config.NodeAuthorize == nil || config.OperatorAuthorize == nil {
		return nil, errors.New("meldbase authority HTTP: incomplete handler configuration")
	}
	return &Handler{authority: config.Authority, nodeAuthorize: config.NodeAuthorize, operatorAuthorize: config.OperatorAuthorize}, nil
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	if handler == nil || handler.authority == nil || handler.nodeAuthorize == nil || handler.operatorAuthorize == nil {
		writeError(writer, http.StatusServiceUnavailable, "unavailable", 0)
		return
	}
	if request == nil || request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		writeError(writer, http.StatusMethodNotAllowed, "method_not_allowed", 0)
		return
	}
	if request.Header.Get("Origin") != "" {
		writeError(writer, http.StatusForbidden, "origin_forbidden", 0)
		return
	}
	if request.TLS == nil || request.TLS.Version < tls.VersionTLS12 || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.VerifiedChains[0]) == 0 {
		writeError(writer, http.StatusUnauthorized, "authentication_failed", 0)
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(writer, http.StatusUnsupportedMediaType, "invalid_content_type", 0)
		return
	}
	if databaseID, ok := databaseIDFromPath(request.URL, grantPath); ok {
		handler.grant(writer, request, databaseID)
		return
	}
	if databaseID, ok := databaseIDFromPath(request.URL, revokePath); ok {
		handler.revoke(writer, request, databaseID)
		return
	}
	writeError(writer, http.StatusNotFound, "not_found", 0)
}

func (handler *Handler) grant(writer http.ResponseWriter, request *http.Request, databaseID [16]byte) {
	owner, err := handler.nodeAuthorize(request)
	if err != nil || owner == "" {
		writeError(writer, http.StatusUnauthorized, "authentication_failed", 0)
		return
	}
	var body wireGrantRequest
	if !decodeBody(request.Body, &body) || body.Version != protocolVersion {
		writeError(writer, http.StatusBadRequest, "invalid_request", 0)
		return
	}
	grant, err := handler.authority.Grant(request.Context(), primarylease.GrantRequest{DatabaseID: databaseID, Owner: owner, CommitSequence: body.CommitSequence})
	if err == nil {
		writeJSON(writer, http.StatusOK, wireGrantResponse{Version: protocolVersion, Certificate: grant.Certificate})
		return
	}
	switch {
	case errors.Is(err, primarylease.ErrLeaseActive):
		writeError(writer, http.StatusConflict, "lease_active", grant.RetryAfter.UTC().UnixMilli())
	case errors.Is(err, primarylease.ErrLeaseSequence):
		writeError(writer, http.StatusConflict, "sequence_rejected", 0)
	case errors.Is(err, primarylease.ErrCertificate):
		writeError(writer, http.StatusBadRequest, "invalid_request", 0)
	default:
		writeError(writer, http.StatusServiceUnavailable, "unavailable", 0)
	}
}

func (handler *Handler) revoke(writer http.ResponseWriter, request *http.Request, databaseID [16]byte) {
	if identity, err := handler.operatorAuthorize(request); err != nil || identity == "" {
		writeError(writer, http.StatusUnauthorized, "authentication_failed", 0)
		return
	}
	var body wireRevokeRequest
	if !decodeBody(request.Body, &body) || body.Version != protocolVersion || body.Epoch == 0 {
		writeError(writer, http.StatusBadRequest, "invalid_request", 0)
		return
	}
	if _, err := handler.authority.Revoke(request.Context(), databaseID, body.Epoch); err != nil {
		if errors.Is(err, primarylease.ErrCertificate) {
			writeError(writer, http.StatusBadRequest, "invalid_request", 0)
			return
		}
		writeError(writer, http.StatusServiceUnavailable, "unavailable", 0)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

// ClientOptions configures a private Authority API client. HTTPClient carries
// the caller's mTLS certificate and trusted roots. Exactly one of the legacy
// single pin or the rotation-aware pin list must be supplied.
type ClientOptions struct {
	Endpoint                   string
	ExpectedServerFingerprint  string
	ExpectedServerFingerprints []string
	HTTPClient                 *http.Client
}

// Client is a node or operator control-plane client. It cannot impersonate an
// owner because the handler derives Owner from its verified mTLS identity.
type Client struct {
	endpoint     string
	fingerprints map[[sha256.Size]byte]struct{}
	client       *http.Client
}

func NewClient(options ClientOptions) (*Client, error) {
	endpoint, err := parseEndpoint(options.Endpoint)
	if err != nil {
		return nil, err
	}
	fingerprints, err := configuredFingerprints(options)
	if err != nil {
		return nil, err
	}
	client := options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{endpoint: endpoint, fingerprints: fingerprints, client: &copy}, nil
}

// Grant requests a primary certificate for the caller's configured mTLS node
// identity. RetryAfter is set only when ErrLeaseActive is returned.
func (client *Client) Grant(ctx context.Context, databaseID [16]byte, commitSequence uint64) (primarylease.Grant, error) {
	if client == nil || client.client == nil || ctx == nil || databaseID == [16]byte{} {
		return primarylease.Grant{}, fatal(ErrProtocol)
	}
	body, err := json.Marshal(wireGrantRequest{Version: protocolVersion, CommitSequence: commitSequence})
	if err != nil {
		return primarylease.Grant{}, fatal(ErrProtocol)
	}
	response, err := client.request(ctx, grantPath, databaseID, body)
	if err != nil {
		return primarylease.Grant{}, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusOK {
		var wire wireGrantResponse
		if !decodeResponse(response.Body, &wire) || wire.Version != protocolVersion || wire.Certificate == "" {
			return primarylease.Grant{}, fatal(ErrProtocol)
		}
		return primarylease.Grant{Certificate: wire.Certificate}, nil
	}
	return primarylease.Grant{}, client.grantError(response)
}

// Revoke requests a controller-side epoch advancement using the caller's
// separately authorized operator mTLS identity.
func (client *Client) Revoke(ctx context.Context, databaseID [16]byte, epoch uint64) error {
	if client == nil || client.client == nil || ctx == nil || databaseID == [16]byte{} || epoch == 0 {
		return fatal(ErrProtocol)
	}
	body, err := json.Marshal(wireRevokeRequest{Version: protocolVersion, Epoch: epoch})
	if err != nil {
		return fatal(ErrProtocol)
	}
	response, err := client.request(ctx, revokePath, databaseID, body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return nil
	}
	return client.statusError(response)
}

func (client *Client) request(ctx context.Context, prefix string, databaseID [16]byte, body []byte) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint+prefix+hex.EncodeToString(databaseID[:]), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.client.Do(request)
	if err != nil {
		return nil, err
	}
	if err := replicationauth.RequireVerifiedTLS(response.TLS); err != nil {
		response.Body.Close()
		return nil, fatal(fmt.Errorf("%w: %v", ErrHTTPSRequired, err))
	}
	leaf := response.TLS.VerifiedChains[0][0]
	if leaf == nil {
		response.Body.Close()
		return nil, fatal(ErrHTTPSRequired)
	}
	if _, expected := client.fingerprints[sha256.Sum256(leaf.Raw)]; !expected {
		response.Body.Close()
		return nil, fatal(ErrHTTPSRequired)
	}
	return response, nil
}

func (client *Client) grantError(response *http.Response) error {
	var wire wireError
	if !decodeResponse(response.Body, &wire) || wire.Version != protocolVersion {
		return fatal(ErrProtocol)
	}
	switch response.StatusCode {
	case http.StatusConflict:
		switch wire.Error {
		case "lease_active":
			if wire.RetryAfterMS <= 0 {
				return fatal(ErrProtocol)
			}
			return errors.Join(primarylease.ErrLeaseActive, retryAfterError{at: time.UnixMilli(wire.RetryAfterMS).UTC()})
		case "sequence_rejected":
			return primarylease.ErrLeaseSequence
		default:
			return fatal(ErrProtocol)
		}
	default:
		return client.statusErrorDecoded(response.StatusCode, wire.Error)
	}
}

func (client *Client) statusError(response *http.Response) error {
	var wire wireError
	if !decodeResponse(response.Body, &wire) || wire.Version != protocolVersion {
		return fatal(ErrProtocol)
	}
	return client.statusErrorDecoded(response.StatusCode, wire.Error)
}

func (client *Client) statusErrorDecoded(status int, code string) error {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return fatal(errors.New("meldbase authority HTTP: authentication failed"))
	case http.StatusBadRequest, http.StatusUnsupportedMediaType, http.StatusMethodNotAllowed, http.StatusNotFound:
		return fatal(ErrProtocol)
	case http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusBadGateway:
		if code != "unavailable" {
			return fatal(ErrProtocol)
		}
		return errors.New("meldbase authority HTTP: controller unavailable")
	default:
		return fatal(ErrProtocol)
	}
}

// RetryAfter extracts the retry point returned with ErrLeaseActive. The value
// is intentionally absent for all other failures and has no owner/state data.
func RetryAfter(err error) (time.Time, bool) {
	var retry retryAfterError
	if !errors.As(err, &retry) {
		return time.Time{}, false
	}
	return retry.at, true
}

type retryAfterError struct{ at time.Time }

func (err retryAfterError) Error() string {
	return "meldbase authority HTTP: retry after lease handoff"
}

type wireGrantRequest struct {
	Version        uint32 `json:"v"`
	CommitSequence uint64 `json:"commitSequence"`
}

type wireRevokeRequest struct {
	Version uint32 `json:"v"`
	Epoch   uint64 `json:"epoch"`
}

type wireGrantResponse struct {
	Version     uint32 `json:"v"`
	Certificate string `json:"certificate"`
}

type wireError struct {
	Version      uint32 `json:"v"`
	Error        string `json:"error"`
	RetryAfterMS int64  `json:"retryAfterMs,omitempty"`
}

func parseEndpoint(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", ErrHTTPSRequired
	}
	return strings.TrimSuffix(parsed.String(), "/"), nil
}

func configuredFingerprints(options ClientOptions) (map[[sha256.Size]byte]struct{}, error) {
	if options.ExpectedServerFingerprint != "" && len(options.ExpectedServerFingerprints) != 0 {
		return nil, ErrHTTPSRequired
	}
	pins := options.ExpectedServerFingerprints
	if options.ExpectedServerFingerprint != "" {
		pins = []string{options.ExpectedServerFingerprint}
	}
	if len(pins) == 0 || len(pins) > maximumServerPins {
		return nil, ErrHTTPSRequired
	}
	fingerprints := make(map[[sha256.Size]byte]struct{}, len(pins))
	for _, pin := range pins {
		if len(pin) != sha256.Size*2 || pin != strings.ToLower(pin) {
			return nil, ErrHTTPSRequired
		}
		decoded, err := hex.DecodeString(pin)
		if err != nil || len(decoded) != sha256.Size {
			return nil, ErrHTTPSRequired
		}
		var fingerprint [sha256.Size]byte
		copy(fingerprint[:], decoded)
		if _, duplicate := fingerprints[fingerprint]; duplicate {
			return nil, ErrHTTPSRequired
		}
		fingerprints[fingerprint] = struct{}{}
	}
	return fingerprints, nil
}

func databaseIDFromPath(endpoint *url.URL, prefix string) ([16]byte, bool) {
	var databaseID [16]byte
	if endpoint == nil || endpoint.RawQuery != "" || endpoint.EscapedPath() != endpoint.Path || !strings.HasPrefix(endpoint.Path, prefix) {
		return databaseID, false
	}
	value := strings.TrimPrefix(endpoint.Path, prefix)
	if len(value) != 32 || strings.Contains(value, "/") || value != strings.ToLower(value) {
		return databaseID, false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(databaseID) {
		return databaseID, false
	}
	copy(databaseID[:], decoded)
	return databaseID, databaseID != [16]byte{}
}

func decodeBody(reader io.Reader, target any) bool {
	return decodeJSON(io.LimitReader(reader, maximumBodyBytes+1), target)
}

func decodeResponse(reader io.Reader, target any) bool {
	return decodeJSON(io.LimitReader(reader, maximumBodyBytes+1), target)
}

func decodeJSON(reader io.Reader, target any) bool {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return false
	}
	var trailing any
	return errors.Is(decoder.Decode(&trailing), io.EOF)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, code string, retryAfterMS int64) {
	writeJSON(writer, status, wireError{Version: protocolVersion, Error: code, RetryAfterMS: retryAfterMS})
}
