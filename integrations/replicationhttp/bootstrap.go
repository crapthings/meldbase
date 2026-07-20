// Package replicationhttp transports a verified V2 bootstrap over HTTPS.
//
// It is intentionally only the bootstrap half of replication. After an
// import, callers attach the returned tokens to the WebSocket change-feed
// receiver. The authenticated durable-consumer name is server controlled;
// clients never select it in an HTTP header or URL.
package replicationhttp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/crapthings/meldbase/core"
	"github.com/crapthings/meldbase/integrations/replicationauth"
)

const bootstrapVersion = "1"

const (
	headerBootstrapVersion = "X-Meldbase-Bootstrap-Version"
	headerBytes            = "X-Meldbase-Bytes"
	headerPages            = "X-Meldbase-Pages"
	headerCommitSequence   = "X-Meldbase-Commit-Sequence"
	headerMetaGeneration   = "X-Meldbase-Meta-Generation"
	headerDatabaseID       = "X-Meldbase-Database-ID"
	headerSHA256           = "X-Meldbase-SHA256"
	headerCheckpointToken  = "X-Meldbase-Checkpoint-Token"
	headerSnapshotToken    = "X-Meldbase-Snapshot-Token"
)

var (
	ErrInvalidBootstrapResponse = errors.New("meldbase replication bootstrap: invalid response")
	ErrBootstrapHTTPSRequired   = errors.New("meldbase replication bootstrap: HTTPS is required")
)

// Authorize authenticates an HTTPS request and returns a stable,
// server-controlled durable consumer name. Typical deployments map a verified
// mTLS client certificate fingerprint to that name. It must not derive the
// name from a user-controlled request value.
type Authorize = replicationauth.Authorize

// MTLSConfig is shared with the WebSocket replication adapter, so bootstrap
// and tail transport cannot accidentally use different peer-name mappings.
type MTLSConfig = replicationauth.MTLSConfig

// NewMTLSAuthorizer returns the strict verified-leaf-fingerprint authorizer
// used by the WebSocket adapter too. The enclosing http.Server must still
// require and verify client certificates in its tls.Config.
func NewMTLSAuthorizer(config MTLSConfig) (Authorize, error) {
	return replicationauth.NewMTLSAuthorizer(config)
}

// SourceConfig configures an HTTPS handler. TLS and client-certificate
// verification belong to the enclosing http.Server; Authorize enforces its
// application-level identity mapping.
type SourceConfig struct {
	DB               *meldbase.DB
	Authorize        Authorize
	ArchiveDirectory string
	Buffer           int
}

// Source serves one exact, verified V2 artifact and establishes the source
// durable checkpoint before it is made available to the receiver.
type Source struct {
	db               *meldbase.DB
	authorize        Authorize
	archiveDirectory string
	buffer           int
}

// NewSource validates configuration and returns an HTTP handler source.
func NewSource(config SourceConfig) (*Source, error) {
	if config.DB == nil || config.Authorize == nil || config.ArchiveDirectory == "" {
		return nil, errors.New("meldbase replication bootstrap: incomplete source configuration")
	}
	if config.Buffer <= 0 || config.Buffer > 1024 {
		return nil, errors.New("meldbase replication bootstrap: invalid source buffer")
	}
	directory, err := filepath.Abs(filepath.Clean(config.ArchiveDirectory))
	if err != nil {
		return nil, err
	}
	return &Source{db: config.DB, authorize: config.Authorize, archiveDirectory: directory, buffer: config.Buffer}, nil
}

// ServeHTTP accepts only an authenticated GET with no browser Origin. The
// source lease covers both bootstrap and a subsequent replicationws source
// session, preventing two transports from racing one durable checkpoint.
func (source *Source) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if source == nil || source.db == nil || source.authorize == nil {
		http.Error(writer, "replication bootstrap unavailable", http.StatusServiceUnavailable)
		return
	}
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if request.Header.Get("Origin") != "" {
		http.Error(writer, "browser origins are not allowed", http.StatusForbidden)
		return
	}
	if request.TLS == nil || request.TLS.Version < tls.VersionTLS12 {
		http.Error(writer, "TLS 1.2 or later is required", http.StatusBadRequest)
		return
	}
	consumerName, err := source.authorize(request)
	if err != nil {
		http.Error(writer, "replication authentication failed", http.StatusUnauthorized)
		return
	}
	lease, err := source.db.AcquireReplicationSourceLease(consumerName)
	if err != nil {
		if errors.Is(err, meldbase.ErrReplicaSourceActive) {
			http.Error(writer, "replication source already active", http.StatusConflict)
			return
		}
		http.Error(writer, "replication bootstrap unavailable", http.StatusServiceUnavailable)
		return
	}
	defer lease.Release()

	directory, err := os.MkdirTemp(source.archiveDirectory, ".meldbase-bootstrap-")
	if err != nil {
		http.Error(writer, "replication bootstrap unavailable", http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(directory)
	artifact := filepath.Join(directory, "bootstrap.meld2")
	bootstrap, subscription, err := source.db.BeginV2Archive(request.Context(), consumerName, artifact, source.buffer)
	if err != nil {
		if errors.Is(err, meldbase.ErrDurableConsumerExists) {
			http.Error(writer, "replication bootstrap already exists; resume the durable feed or choose a new replica identity", http.StatusConflict)
			return
		}
		http.Error(writer, "replication bootstrap unavailable", http.StatusInternalServerError)
		return
	}
	// Keep the consumer handle open until the entire response is copied. Its
	// durable checkpoint deliberately remains on any later transport failure so
	// an operator can inspect or resume the retained history.
	defer subscription.Close()

	file, err := os.Open(artifact)
	if err != nil {
		http.Error(writer, "replication bootstrap unavailable", http.StatusInternalServerError)
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() < 0 || uint64(info.Size()) != bootstrap.Backup.Bytes {
		http.Error(writer, "replication bootstrap unavailable", http.StatusInternalServerError)
		return
	}
	writeBootstrapHeaders(writer.Header(), bootstrap)
	writer.WriteHeader(http.StatusOK)
	_, _ = io.Copy(writer, file)
}

func writeBootstrapHeaders(header http.Header, bootstrap meldbase.ArchiveV2Bootstrap) {
	backup := bootstrap.Backup
	header.Set("Content-Type", "application/octet-stream")
	header.Set("Content-Length", strconv.FormatUint(backup.Bytes, 10))
	header.Set("Cache-Control", "no-store")
	header.Set(headerBootstrapVersion, bootstrapVersion)
	header.Set(headerBytes, strconv.FormatUint(backup.Bytes, 10))
	header.Set(headerPages, strconv.FormatUint(backup.Pages, 10))
	header.Set(headerCommitSequence, strconv.FormatUint(backup.CommitSequence, 10))
	header.Set(headerMetaGeneration, strconv.FormatUint(backup.MetaGeneration, 10))
	header.Set(headerDatabaseID, backup.DatabaseIDHex)
	header.Set(headerSHA256, backup.SHA256)
	header.Set(headerCheckpointToken, strconv.FormatUint(bootstrap.CheckpointToken, 10))
	header.Set(headerSnapshotToken, strconv.FormatUint(bootstrap.SnapshotToken, 10))
}

// FetchConfig configures a receiving bootstrap. HTTPClient must be configured
// with the deployment's mTLS client certificate and trusted server roots.
type FetchConfig struct {
	URL         string
	HTTPClient  *http.Client
	Destination string
	MaxBytes    uint64
}

// Bootstrap is the verified artifact receipt and the durable-feed bridge
// tokens that must be supplied to replicationws.ReceiverConfig.
type Bootstrap struct {
	Backup          meldbase.BackupV2Result
	CheckpointToken uint64
	SnapshotToken   uint64
}

// Fetch downloads one bootstrap over a non-redirected HTTPS connection and
// imports it atomically. It requires an actual TLS response even when callers
// provide a custom HTTP transport, preventing a transport shim from silently
// downgrading the security contract.
func Fetch(ctx context.Context, config FetchConfig) (Bootstrap, error) {
	endpoint, err := url.Parse(config.URL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil {
		return Bootstrap{}, ErrBootstrapHTTPSRequired
	}
	if config.Destination == "" {
		return Bootstrap{}, errors.New("meldbase replication bootstrap: empty destination")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return Bootstrap{}, err
	}
	client := cloneNoRedirectClient(config.HTTPClient)
	response, err := client.Do(request)
	if err != nil {
		return Bootstrap{}, err
	}
	defer response.Body.Close()
	if err := replicationauth.RequireVerifiedTLS(response.TLS); err != nil {
		return Bootstrap{}, fmt.Errorf("%w: %v", ErrBootstrapHTTPSRequired, err)
	}
	if response.StatusCode != http.StatusOK {
		return Bootstrap{}, fmt.Errorf("meldbase replication bootstrap: source returned HTTP %d", response.StatusCode)
	}
	bootstrap, err := parseBootstrapResponse(response)
	if err != nil {
		return Bootstrap{}, err
	}
	if _, err := meldbase.ImportV2PhysicalBackup(ctx, response.Body, config.Destination, bootstrap.Backup, meldbase.PhysicalBackupImportOptions{MaxBytes: config.MaxBytes}); err != nil {
		return Bootstrap{}, err
	}
	return bootstrap, nil
}

func cloneNoRedirectClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copy
}

func parseBootstrapResponse(response *http.Response) (Bootstrap, error) {
	if response == nil || response.ContentLength < 0 {
		return Bootstrap{}, ErrInvalidBootstrapResponse
	}
	if value, err := requiredHeader(response.Header, headerBootstrapVersion); err != nil || value != bootstrapVersion {
		return Bootstrap{}, ErrInvalidBootstrapResponse
	}
	bytes, err := requiredUintHeader(response.Header, headerBytes)
	if err != nil || response.ContentLength < 0 || uint64(response.ContentLength) != bytes {
		return Bootstrap{}, ErrInvalidBootstrapResponse
	}
	pages, err := requiredUintHeader(response.Header, headerPages)
	if err != nil {
		return Bootstrap{}, ErrInvalidBootstrapResponse
	}
	commitSequence, err := requiredUintHeader(response.Header, headerCommitSequence)
	if err != nil {
		return Bootstrap{}, ErrInvalidBootstrapResponse
	}
	metaGeneration, err := requiredUintHeader(response.Header, headerMetaGeneration)
	if err != nil {
		return Bootstrap{}, ErrInvalidBootstrapResponse
	}
	checkpoint, err := requiredUintHeader(response.Header, headerCheckpointToken)
	if err != nil {
		return Bootstrap{}, ErrInvalidBootstrapResponse
	}
	snapshot, err := requiredUintHeader(response.Header, headerSnapshotToken)
	if err != nil || checkpoint > snapshot || snapshot != commitSequence {
		return Bootstrap{}, ErrInvalidBootstrapResponse
	}
	databaseID, err := requiredHeader(response.Header, headerDatabaseID)
	if err != nil {
		return Bootstrap{}, ErrInvalidBootstrapResponse
	}
	digest, err := requiredHeader(response.Header, headerSHA256)
	if err != nil {
		return Bootstrap{}, ErrInvalidBootstrapResponse
	}
	return Bootstrap{Backup: meldbase.BackupV2Result{Bytes: bytes, Pages: pages, CommitSequence: commitSequence, MetaGeneration: metaGeneration, DatabaseIDHex: databaseID, SHA256: digest}, CheckpointToken: checkpoint, SnapshotToken: snapshot}, nil
}

func requiredHeader(header http.Header, key string) (string, error) {
	values := header.Values(key)
	if len(values) != 1 || values[0] == "" || strings.Contains(values[0], ",") {
		return "", ErrInvalidBootstrapResponse
	}
	return values[0], nil
}

func requiredUintHeader(header http.Header, key string) (uint64, error) {
	value, err := requiredHeader(header, key)
	if err != nil || value == "0" || (len(value) > 1 && value[0] == '0') {
		return 0, ErrInvalidBootstrapResponse
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || strconv.FormatUint(parsed, 10) != value {
		return 0, ErrInvalidBootstrapResponse
	}
	return parsed, nil
}
