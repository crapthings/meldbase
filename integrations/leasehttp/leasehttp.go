// Package leasehttp provides authenticated HTTPS access to one controller member's lease records.
//
// It uses strict JSON and mTLS. Certificate signing, Authority.Grant, and leader
// election remain outside the handler.
package leasehttp

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
	"sort"
	"strings"
	"time"

	"github.com/crapthings/meldbase/integrations/primarylease"
	"github.com/crapthings/meldbase/integrations/replicationauth"
)

const (
	// protocolVersion identifies the lease member JSON protocol.
	protocolVersion uint32 = 1
	// leasePath prefixes routes for database lease records.
	leasePath = "/v1/lease/"
	// maximumBodyBytes bounds the JSON data read from requests and responses.
	maximumBodyBytes int64 = 4096
	// maximumServerPins limits certificate fingerprints accepted for one member.
	maximumServerPins = 4
)

var (
	// ErrHTTPSRequired reports invalid HTTPS configuration or an untrusted server connection.
	ErrHTTPSRequired = errors.New("meldbase lease HTTP: verified HTTPS is required")
	// ErrProtocol reports invalid lease protocol data or responses.
	ErrProtocol = errors.New("meldbase lease HTTP: invalid protocol response")
)

// fatalQuorumError marks an error that a quorum must not ignore.
type fatalQuorumError struct{ err error }

// Error returns the wrapped error message.
func (err fatalQuorumError) Error() string { return err.err.Error() }

// Unwrap exposes the underlying error.
func (err fatalQuorumError) Unwrap() error { return err.err }

// PrimaryLeaseQuorumFatal tells the quorum to stop on this error.
func (fatalQuorumError) PrimaryLeaseQuorumFatal() bool { return true }

// fatal wraps a nonnil error so it stops a quorum operation.
func fatal(err error) error {
	if err == nil {
		return nil
	}
	return fatalQuorumError{err: err}
}

// Authorize maps a verified peer certificate to a trusted controller identity.
//
// The identity must not come from a lease frame or URL.
type Authorize = replicationauth.Authorize

// MTLSConfig maps verified peer certificate fingerprints to trusted identities.
type MTLSConfig = replicationauth.MTLSConfig

// NewMTLSAuthorizer creates an authorizer for verified and configured client certificates.
func NewMTLSAuthorizer(config MTLSConfig) (Authorize, error) {
	return replicationauth.NewMTLSAuthorizer(config)
}

// MemberConfig sets one controller member's lease store and peer authorization rules.
//
// The enclosing server
// must require and verify client certificates; Authorize maps the verified leaf
// certificate to a configured controller identity.
type MemberConfig struct {
	Store     primarylease.LeaseStore
	Authorize Authorize
}

// Member serves authenticated lease record reads and conditional writes over HTTP.
type Member struct {
	store     primarylease.LeaseStore
	authorize Authorize
}

// NewMember creates a lease member handler with a store and peer authorizer.
func NewMember(config MemberConfig) (*Member, error) {
	if config.Store == nil || config.Authorize == nil {
		return nil, errors.New("meldbase lease HTTP: incomplete member configuration")
	}
	return &Member{store: config.Store, authorize: config.Authorize}, nil
}

// ServeHTTP authenticates a peer and routes its lease read or conditional write.
func (member *Member) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	if member == nil || member.store == nil || member.authorize == nil {
		writeError(writer, http.StatusServiceUnavailable, "unavailable")
		return
	}
	if request == nil || (request.Method != http.MethodGet && request.Method != http.MethodPut) {
		writer.Header().Set("Allow", "GET, PUT")
		writeError(writer, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if request.Header.Get("Origin") != "" {
		writeError(writer, http.StatusForbidden, "origin_forbidden")
		return
	}
	if request.TLS == nil || request.TLS.Version < tls.VersionTLS12 || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.VerifiedChains[0]) == 0 {
		writeError(writer, http.StatusUnauthorized, "authentication_failed")
		return
	}
	if _, err := member.authorize(request); err != nil {
		writeError(writer, http.StatusUnauthorized, "authentication_failed")
		return
	}
	databaseID, ok := databaseIDFromPath(request.URL)
	if !ok {
		writeError(writer, http.StatusNotFound, "not_found")
		return
	}
	if request.Method == http.MethodGet {
		member.load(writer, request, databaseID)
		return
	}
	member.compareAndSwap(writer, request, databaseID)
}

// load returns a stored lease as JSON or reports its absence or a store failure.
func (member *Member) load(writer http.ResponseWriter, request *http.Request, databaseID [16]byte) {
	record, exists, err := member.store.LoadPrimaryLease(request.Context(), databaseID)
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "unavailable")
		return
	}
	if !exists {
		writeError(writer, http.StatusNotFound, "not_found")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(wireLoad{Version: protocolVersion, Record: encodeRecord(record)})
}

// compareAndSwap validates a conditional lease write and reports whether the store accepted it.
func (member *Member) compareAndSwap(writer http.ResponseWriter, request *http.Request, databaseID [16]byte) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(writer, http.StatusUnsupportedMediaType, "invalid_content_type")
		return
	}
	decoder := json.NewDecoder(io.LimitReader(request.Body, maximumBodyBytes+1))
	decoder.DisallowUnknownFields()
	var wire wireCAS
	if err := decoder.Decode(&wire); err != nil || !decoderEOF(decoder) || wire.Version != protocolVersion || wire.Next == nil {
		writeError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	next, err := decodeRecord(*wire.Next)
	if err != nil || primarylease.ValidateLeaseRecord(next, databaseID) != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	var previous *primarylease.LeaseRecord
	if wire.Previous != nil {
		value, err := decodeRecord(*wire.Previous)
		if err != nil || primarylease.ValidateLeaseRecord(value, databaseID) != nil {
			writeError(writer, http.StatusBadRequest, "invalid_request")
			return
		}
		previous = &value
	}
	swapped, err := member.store.CompareAndSwapPrimaryLease(request.Context(), databaseID, previous, next)
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "unavailable")
		return
	}
	if !swapped {
		writeError(writer, http.StatusConflict, "conflict")
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

// ClientOptions sets one controller member's endpoint, expected certificates, and HTTP client.
//
// HTTPClient should carry the caller's mTLS certificate and trusted
// server roots. The endpoint cannot include a path, query, userinfo or
// fragment, preventing URL-controlled routing ambiguity.
type ClientOptions struct {
	Endpoint string
	// ExpectedServerFingerprint is the legacy single-leaf configuration. New
	// deployments should use ExpectedServerFingerprints so an old and a new
	// leaf can overlap during a controlled rotation. The two fields are mutually
	// exclusive.
	ExpectedServerFingerprint string
	// ExpectedServerFingerprints is one to four lowercase SHA-256 server leaf
	// fingerprints. Every configured leaf identifies the same static quorum
	// member; QuorumStore rejects an overlap with another replica's pin set.
	ExpectedServerFingerprints []string
	HTTPClient                 *http.Client
}

// Client reads and conditionally updates one controller member's lease records over verified HTTPS.
//
// It implements primarylease.LeaseStore. QuorumStore handles majority decisions
// across several clients.
type Client struct {
	endpoint     string
	fingerprints map[[sha256.Size]byte]struct{}
	identities   []string
	client       *http.Client
}

// NewClient creates a client with validated server pins and HTTP redirects disabled.
func NewClient(options ClientOptions) (*Client, error) {
	parsed, err := url.Parse(options.Endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, ErrHTTPSRequired
	}
	fingerprints, identities, err := configuredFingerprints(options)
	if err != nil {
		return nil, err
	}
	client := options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{endpoint: strings.TrimSuffix(parsed.String(), "/"), fingerprints: fingerprints, identities: identities, client: &copy}, nil
}

// configuredFingerprints validates server pins and returns their sorted member identities.
func configuredFingerprints(options ClientOptions) (map[[sha256.Size]byte]struct{}, []string, error) {
	if options.ExpectedServerFingerprint != "" && len(options.ExpectedServerFingerprints) != 0 {
		return nil, nil, ErrHTTPSRequired
	}
	pins := options.ExpectedServerFingerprints
	if options.ExpectedServerFingerprint != "" {
		pins = []string{options.ExpectedServerFingerprint}
	}
	if len(pins) == 0 || len(pins) > maximumServerPins {
		return nil, nil, ErrHTTPSRequired
	}
	fingerprints := make(map[[sha256.Size]byte]struct{}, len(pins))
	identities := make([]string, 0, len(pins))
	for _, pin := range pins {
		if len(pin) != sha256.Size*2 || pin != strings.ToLower(pin) {
			return nil, nil, ErrHTTPSRequired
		}
		decoded, err := hex.DecodeString(pin)
		if err != nil || len(decoded) != sha256.Size {
			return nil, nil, ErrHTTPSRequired
		}
		var fingerprint [sha256.Size]byte
		copy(fingerprint[:], decoded)
		if _, duplicate := fingerprints[fingerprint]; duplicate {
			return nil, nil, ErrHTTPSRequired
		}
		fingerprints[fingerprint] = struct{}{}
		identities = append(identities, pin)
	}
	sort.Strings(identities)
	return fingerprints, identities, nil
}

// LoadPrimaryLease fetches and validates a member's lease record.
func (client *Client) LoadPrimaryLease(ctx context.Context, databaseID [16]byte) (primarylease.LeaseRecord, bool, error) {
	if client == nil || client.client == nil || ctx == nil || databaseID == [16]byte{} {
		return primarylease.LeaseRecord{}, false, fatal(ErrProtocol)
	}
	response, err := client.request(ctx, http.MethodGet, databaseID, nil)
	if err != nil {
		return primarylease.LeaseRecord{}, false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return primarylease.LeaseRecord{}, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return primarylease.LeaseRecord{}, false, client.statusError(response)
	}
	var wire wireLoad
	if err := decodeBoundedJSON(response.Body, &wire); err != nil || wire.Version != protocolVersion {
		return primarylease.LeaseRecord{}, false, fatal(ErrProtocol)
	}
	record, err := decodeRecord(wire.Record)
	if err != nil || primarylease.ValidateLeaseRecord(record, databaseID) != nil {
		return primarylease.LeaseRecord{}, false, fatal(ErrProtocol)
	}
	return record, true, nil
}

// CompareAndSwapPrimaryLease asks a member to replace a lease matching the expected state.
func (client *Client) CompareAndSwapPrimaryLease(ctx context.Context, databaseID [16]byte, previous *primarylease.LeaseRecord, next primarylease.LeaseRecord) (bool, error) {
	if client == nil || client.client == nil || ctx == nil || databaseID == [16]byte{} || primarylease.ValidateLeaseRecord(next, databaseID) != nil || (previous != nil && primarylease.ValidateLeaseRecord(*previous, databaseID) != nil) {
		return false, fatal(ErrProtocol)
	}
	wire := wireCAS{Version: protocolVersion, Next: pointerRecord(encodeRecord(next))}
	if previous != nil {
		value := encodeRecord(*previous)
		wire.Previous = &value
	}
	body, err := json.Marshal(wire)
	if err != nil {
		return false, fatal(ErrProtocol)
	}
	response, err := client.request(ctx, http.MethodPut, databaseID, body)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusNoContent:
		return true, nil
	case http.StatusConflict:
		return false, nil
	default:
		return false, client.statusError(response)
	}
}

// request sends a lease operation and checks the server's verified certificate against configured pins.
func (client *Client) request(ctx context.Context, method string, databaseID [16]byte, body []byte) (*http.Response, error) {
	path := leasePath + hex.EncodeToString(databaseID[:])
	request, err := http.NewRequestWithContext(ctx, method, client.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if method == http.MethodPut {
		request.Header.Set("Content-Type", "application/json")
	}
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

// PrimaryLeaseMemberIdentity retains the legacy single-identity method.
//
// New
// callers must use PrimaryLeaseMemberIdentities so certificate rotation cannot
// hide an overlapping member identity.
func (client *Client) PrimaryLeaseMemberIdentity() string {
	if client == nil || len(client.identities) == 0 {
		return ""
	}
	return client.identities[0]
}

// PrimaryLeaseMemberIdentities exposes every expected, verified server leaf
// fingerprint to primarylease.QuorumStore.
//
// The returned copy is canonical and
// cannot mutate the client's identity configuration.
func (client *Client) PrimaryLeaseMemberIdentities() []string {
	if client == nil || len(client.identities) == 0 {
		return nil
	}
	return append([]string(nil), client.identities...)
}

// statusError classifies HTTP failures as temporary member outages or fatal quorum errors.
func (client *Client) statusError(response *http.Response) error {
	if response == nil {
		return fatal(ErrProtocol)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maximumBodyBytes+1))
	switch response.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return fatal(errors.New("meldbase lease HTTP: authentication failed"))
	case http.StatusBadRequest, http.StatusUnsupportedMediaType, http.StatusMethodNotAllowed:
		return fatal(ErrProtocol)
	case http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusBadGateway:
		return errors.New("meldbase lease HTTP: member unavailable")
	default:
		return fatal(fmt.Errorf("meldbase lease HTTP: unexpected status %d", response.StatusCode))
	}
}

// wireRecord represents a lease record in JSON.
type wireRecord struct {
	DatabaseID     string `json:"databaseId"`
	Owner          string `json:"owner"`
	Epoch          uint64 `json:"epoch"`
	CommitSequence uint64 `json:"commitSequence"`
	NotAfterMS     int64  `json:"notAfterMs"`
	Revoked        bool   `json:"revoked"`
}

// wireLoad carries a protocol version and a lease read result.
type wireLoad struct {
	Version uint32     `json:"v"`
	Record  wireRecord `json:"record"`
}

// wireCAS carries the expected and replacement lease records for a conditional write.
type wireCAS struct {
	Version  uint32      `json:"v"`
	Previous *wireRecord `json:"previous"`
	Next     *wireRecord `json:"next"`
}

// encodeRecord converts a lease into its JSON fields.
func encodeRecord(record primarylease.LeaseRecord) wireRecord {
	return wireRecord{DatabaseID: hex.EncodeToString(record.DatabaseID[:]), Owner: record.Owner, Epoch: record.Epoch, CommitSequence: record.CommitSequence, NotAfterMS: record.NotAfter.UTC().UnixMilli(), Revoked: record.Revoked}
}

// decodeRecord decodes the database identity and converts JSON fields into a lease.
func decodeRecord(wire wireRecord) (primarylease.LeaseRecord, error) {
	if len(wire.DatabaseID) != 32 || wire.DatabaseID != strings.ToLower(wire.DatabaseID) {
		return primarylease.LeaseRecord{}, ErrProtocol
	}
	decoded, err := hex.DecodeString(wire.DatabaseID)
	if err != nil || len(decoded) != 16 {
		return primarylease.LeaseRecord{}, ErrProtocol
	}
	var record primarylease.LeaseRecord
	copy(record.DatabaseID[:], decoded)
	record.Owner, record.Epoch, record.CommitSequence = wire.Owner, wire.Epoch, wire.CommitSequence
	record.NotAfter, record.Revoked = time.UnixMilli(wire.NotAfterMS).UTC(), wire.Revoked
	return record, nil
}

// databaseIDFromPath extracts a nonzero database identity from an unescaped lease route.
func databaseIDFromPath(endpoint *url.URL) ([16]byte, bool) {
	var databaseID [16]byte
	if endpoint == nil || endpoint.RawQuery != "" || endpoint.EscapedPath() != endpoint.Path || !strings.HasPrefix(endpoint.Path, leasePath) {
		return databaseID, false
	}
	value := strings.TrimPrefix(endpoint.Path, leasePath)
	if len(value) != 32 || strings.Contains(value, "/") || value != strings.ToLower(value) {
		return databaseID, false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 16 {
		return databaseID, false
	}
	copy(databaseID[:], decoded)
	return databaseID, databaseID != [16]byte{}
}

// pointerRecord returns a pointer to a copy of a wire record.
func pointerRecord(record wireRecord) *wireRecord { return &record }

// decoderEOF checks that no JSON value follows the decoded message.
func decoderEOF(decoder *json.Decoder) bool {
	var trailing any
	return errors.Is(decoder.Decode(&trailing), io.EOF)
}

// decodeBoundedJSON reads one JSON message with a read limit and rejects unknown fields.
func decodeBoundedJSON(reader io.Reader, target any) error {
	decoder := json.NewDecoder(io.LimitReader(reader, maximumBodyBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil || !decoderEOF(decoder) {
		return ErrProtocol
	}
	return nil
}

// writeError sends a JSON error code with the supplied HTTP status.
func writeError(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, `{"error":"`+code+`"}`+"\n")
}

// Verify at compile time that Client implements the lease store interface.
var _ primarylease.LeaseStore = (*Client)(nil)
