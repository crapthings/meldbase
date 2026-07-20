package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/crapthings/meldbase/integrations/anchorhttp"
)

const (
	anchorTLSFileMaximum = 1 << 20
	anchorKeyFileMaximum = 1024
)

type anchorKeyFlags []string

func (values *anchorKeyFlags) String() string { return strings.Join(*values, ",") }
func (values *anchorKeyFlags) Set(value string) error {
	if value == "" {
		return errors.New("anchor key specification is empty")
	}
	*values = append(*values, value)
	return nil
}

type anchorServeOptions struct {
	address         string
	directory       string
	clusterID       string
	members         []string
	memberID        string
	keys            map[string][]byte
	certificate     tls.Certificate
	clientCAs       *x509.CertPool
	maxClockSkew    time.Duration
	shutdownTimeout time.Duration
	ready           chan<- net.Addr
}

func runAnchorServe(args []string, stdout, stderr io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runAnchorServeContext(ctx, args, stdout, stderr)
}

func runAnchorServeContext(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("anchor-serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	address := flags.String("addr", ":8443", "TLS listen address")
	directory := flags.String("dir", "", "private persistent anchor directory")
	clusterID := flags.String("cluster", "", "stable static cluster identifier")
	membersRaw := flags.String("members", "", "comma-separated complete static member IDs")
	memberID := flags.String("member", "", "this node's member ID")
	certificatePath := flags.String("tls-cert", "", "server certificate PEM file")
	privateKeyPath := flags.String("tls-key", "", "private server key PEM file; mode 0600 or stricter")
	clientCAPath := flags.String("client-ca", "", "PEM CA bundle used to require and verify client certificates")
	maxClockSkew := flags.Duration("max-clock-skew", 30*time.Second, "maximum accepted signed-request clock skew")
	shutdownTimeout := flags.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown deadline")
	var keyFiles anchorKeyFlags
	flags.Var(&keyFiles, "key", "repeatable HMAC key as key-id=/private/base64-file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if ctx == nil || *directory == "" || *clusterID == "" || *membersRaw == "" || *memberID == "" || *certificatePath == "" || *privateKeyPath == "" || *clientCAPath == "" || len(keyFiles) == 0 {
		return errors.New("anchor-serve requires --dir, --cluster, --members, --member, --tls-cert, --tls-key, --client-ca and at least one --key")
	}
	if flags.NArg() != 0 {
		return errors.New("anchor-serve does not accept positional arguments")
	}
	if *maxClockSkew < time.Second || *maxClockSkew > 10*time.Minute || *shutdownTimeout <= 0 || *shutdownTimeout > time.Minute {
		return errors.New("anchor-serve requires max clock skew in [1s,10m] and shutdown timeout in (0,1m]")
	}
	if err := validateAnchorDirectory(*directory); err != nil {
		return err
	}
	keys, err := loadAnchorKeys(keyFiles)
	if err != nil {
		return err
	}
	certificate, err := loadAnchorCertificate(*certificatePath, *privateKeyPath)
	if err != nil {
		return err
	}
	clientCAs, err := loadAnchorClientCAs(*clientCAPath)
	if err != nil {
		return err
	}
	options := anchorServeOptions{
		address: *address, directory: *directory, clusterID: *clusterID, members: splitCommaList(*membersRaw), memberID: *memberID,
		keys: keys, certificate: certificate, clientCAs: clientCAs, maxClockSkew: *maxClockSkew, shutdownTimeout: *shutdownTimeout,
	}
	return serveAnchor(ctx, options, stdout)
}

func serveAnchor(ctx context.Context, options anchorServeOptions, stdout io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if options.clientCAs == nil || len(options.certificate.Certificate) == 0 || options.shutdownTimeout <= 0 {
		return errors.New("anchor service requires a server certificate, client CA pool and positive shutdown timeout")
	}
	api, err := anchorhttp.NewHandler(anchorhttp.HandlerOptions{
		Directory: options.directory, ClusterID: options.clusterID, Members: options.members, MemberID: options.memberID,
		Keys: options.keys, MaxClockSkew: options.maxClockSkew,
	})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", options.address)
	if err != nil {
		return err
	}
	serviceHandler := &anchorServiceHandler{api: api}
	server := &http.Server{
		Handler: serviceHandler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{options.certificate},
			ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: options.clientCAs,
			NextProtos: []string{"h2", "http/1.1"},
		},
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.ServeTLS(listener, "", "") }()
	if options.ready != nil {
		options.ready <- listener.Addr()
	}
	fmt.Fprintf(stdout, "Meldbase anchor member %s listening on https://%s\n", options.memberID, listener.Addr())
	select {
	case serveErr := <-serveErrors:
		if errors.Is(serveErr, http.ErrServerClosed) {
			return nil
		}
		return serveErr
	case <-ctx.Done():
		serviceHandler.draining.Store(true)
		shutdown, cancel := context.WithTimeout(context.Background(), options.shutdownTimeout)
		shutdownErr := server.Shutdown(shutdown)
		cancel()
		serveErr := <-serveErrors
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(shutdownErr, serveErr)
	}
}

type anchorServiceHandler struct {
	api      http.Handler
	draining atomic.Bool
}

func (handler *anchorServiceHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	if request.Method == http.MethodGet && request.URL.RawQuery == "" && request.Body != nil && request.ContentLength == 0 {
		switch request.URL.Path {
		case "/livez":
			response.WriteHeader(http.StatusNoContent)
			return
		case "/readyz":
			if handler.draining.Load() {
				response.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			response.WriteHeader(http.StatusNoContent)
			return
		}
	}
	handler.api.ServeHTTP(response, request)
}

func validateAnchorDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(err, errors.New("anchor directory must be an existing real directory"))
	}
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("anchor directory must not grant permissions to group or other users")
	}
	return nil
}

func loadAnchorKeys(specifications []string) (map[string][]byte, error) {
	if len(specifications) < 1 || len(specifications) > 16 {
		return nil, errors.New("anchor-serve accepts between 1 and 16 HMAC keys")
	}
	keys := make(map[string][]byte, len(specifications))
	for _, specification := range specifications {
		keyID, path, ok := strings.Cut(specification, "=")
		if !ok || keyID == "" || path == "" {
			return nil, errors.New("each --key must be key-id=/private/base64-file")
		}
		if _, duplicate := keys[keyID]; duplicate {
			return nil, errors.New("duplicate anchor HMAC key ID")
		}
		raw, err := readAnchorFile(path, anchorKeyFileMaximum, true)
		if err != nil {
			return nil, err
		}
		encoded := strings.TrimSuffix(string(raw), "\n")
		if encoded == "" || strings.ContainsAny(encoded, "\r\n\t ") {
			return nil, errors.New("anchor HMAC key file must contain one strict base64 value with an optional final newline")
		}
		secret, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil || len(secret) < 32 || len(secret) > 128 {
			return nil, errors.New("anchor HMAC key must decode to between 32 and 128 bytes")
		}
		keys[keyID] = secret
	}
	return keys, nil
}

func loadAnchorCertificate(certificatePath, privateKeyPath string) (tls.Certificate, error) {
	return loadAnchorCertificateForUsage(certificatePath, privateKeyPath, x509.ExtKeyUsageServerAuth, "server")
}

func loadAnchorClientCAs(path string) (*x509.CertPool, error) {
	raw, err := readAnchorFile(path, anchorTLSFileMaximum, false)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, errors.New("anchor client CA file contains no certificates")
	}
	return pool, nil
}

func readAnchorFile(path string, maximum int64, private bool) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.Join(err, errors.New("anchor file must be a bounded non-empty regular file"))
	}
	if private && info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("private anchor files must not grant permissions to group or other users")
	}
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	openedInfo, err := file.Stat()
	if err != nil || !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) || (private && openedInfo.Mode().Perm()&0o077 != 0) {
		_ = file.Close()
		return nil, errors.New("anchor file changed while opening")
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maximum+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(raw) == 0 || int64(len(raw)) > maximum {
		return nil, errors.Join(readErr, closeErr, errors.New("failed to read bounded anchor file"))
	}
	return raw, nil
}
