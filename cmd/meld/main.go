package main

import (
	"context"
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
	"syscall"
	"time"

	"github.com/crapthings/meldbase"
	"github.com/crapthings/meldbase/admin"
	meldserver "github.com/crapthings/meldbase/server"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "meld:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: meld <demo|serve|inspect|verify|backup|durability-check|storage-soak|qualification-check|index-build>")
	}
	switch args[0] {
	case "demo":
		return runDemo(args[1:], stdout, stderr)
	case "serve":
		return runServe(args[1:], stdout, stderr)
	case "durability-check":
		return runDurabilityCheck(args[1:], stdout, stderr)
	case "storage-soak":
		return runStorageSoak(args[1:], stdout, stderr)
	case "qualification-check":
		return runQualificationCheck(args[1:], stdout, stderr)
	case "inspect":
		return runInspect(args[1:], stdout, stderr)
	case "backup":
		return runBackup(args[1:], stdout, stderr)
	case "verify":
		return runVerify(args[1:], stdout, stderr)
	case "index-build":
		return runIndexBuild(args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runDemo(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("db", "", "database path (must not already exist)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cleanup := func() {}
	if *path == "" {
		directory, err := os.MkdirTemp("", "meldbase-demo-")
		if err != nil {
			return err
		}
		cleanup = func() { _ = os.RemoveAll(directory) }
		*path = filepath.Join(directory, "demo.meld")
	} else if _, err := os.Stat(*path); err == nil {
		return errors.New("demo database already exists; choose a new --db path")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := meldbase.Open(*path)
	if err != nil {
		return err
	}
	users := db.Collection("users")
	first, err := users.InsertOne(ctx, meldbase.Document{
		"email": meldbase.String("ada@example.com"), "name": meldbase.String("Ada"), "active": meldbase.Bool(true),
	})
	if err != nil {
		_ = db.Close()
		return err
	}
	fmt.Fprintf(stdout, "Inserted user: %s\n", first)
	if err := users.CreateIndex(ctx, "users_email", []meldbase.IndexField{{Field: "email", Order: 1}}, meldbase.IndexOptions{Unique: true}); err != nil {
		_ = db.Close()
		return err
	}
	fmt.Fprintln(stdout, "Created index: users_email")
	if _, err := users.UpdateOne(ctx, meldbase.Filter{"_id": first}, meldbase.Update{"$set": map[string]any{"name": "Ada Lovelace"}}); err != nil {
		_ = db.Close()
		return err
	}
	found, err := users.Find(ctx, meldbase.Filter{"email": "ada@example.com"})
	if err != nil {
		_ = db.Close()
		return err
	}
	documents, err := found.All(ctx)
	if err != nil {
		_ = db.Close()
		return err
	}
	fmt.Fprintf(stdout, "Found users: %d\n", len(documents))
	query, err := meldbase.CompileQuery(meldbase.Filter{"active": true}, meldbase.QueryOptions{})
	if err != nil {
		_ = db.Close()
		return err
	}
	subscription, err := users.SubscribeQuery(ctx, query, 4)
	if err != nil {
		_ = db.Close()
		return err
	}
	fmt.Fprintln(stdout, "Subscribed: users_active")
	initial, err := nextSnapshot(ctx, subscription)
	if err != nil {
		subscription.Close()
		_ = db.Close()
		return err
	}
	fmt.Fprintf(stdout, "Received snapshot: %d document\n", len(initial.Documents))
	if _, err := users.InsertOne(ctx, meldbase.Document{
		"email": meldbase.String("grace@example.com"), "name": meldbase.String("Grace"), "active": meldbase.Bool(true),
	}); err != nil {
		subscription.Close()
		_ = db.Close()
		return err
	}
	next, err := nextSnapshot(ctx, subscription)
	subscription.Close()
	if err != nil {
		_ = db.Close()
		return err
	}
	fmt.Fprintf(stdout, "Received snapshot: %d documents\n", len(next.Documents))
	if err := db.Close(); err != nil {
		return err
	}
	reopened, err := meldbase.Open(*path)
	if err != nil {
		return err
	}
	reopenedCursor, err := reopened.Collection("users").Find(ctx, meldbase.Filter{"active": true})
	if err != nil {
		_ = reopened.Close()
		return err
	}
	reopenedDocuments, err := reopenedCursor.All(ctx)
	closeErr := reopened.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if len(reopenedDocuments) != 2 {
		return fmt.Errorf("recovery check found %d users, want 2", len(reopenedDocuments))
	}
	fmt.Fprintln(stdout, "Database reopened successfully")
	fmt.Fprintln(stdout, "Recovery check passed")
	return nil
}

func nextSnapshot(ctx context.Context, subscription *meldbase.QuerySubscription) (meldbase.QuerySnapshot, error) {
	select {
	case snapshot, ok := <-subscription.Snapshots:
		if !ok {
			return meldbase.QuerySnapshot{}, errors.New("subscription closed")
		}
		return snapshot, nil
	case err, ok := <-subscription.Errors:
		if ok && err != nil {
			return meldbase.QuerySnapshot{}, err
		}
		return meldbase.QuerySnapshot{}, errors.New("subscription stopped")
	case <-ctx.Done():
		return meldbase.QuerySnapshot{}, ctx.Err()
	}
}

func runServe(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("db", "", "database path")
	address := flags.String("addr", ":8080", "HTTP listen address")
	realtimeURL := flags.String("public-realtime-url", "", "public ws(s) URL ending in /v1/realtime")
	devNoAuth := flags.Bool("dev-no-auth", false, "explicitly allow all requests; development only")
	adminAddress := flags.String("admin-addr", "", "optional loopback address for the secured embedded admin dashboard")
	adminDiagnostics := flags.Bool("admin-diagnostics", false, "record bounded slow/failure diagnostics for the admin dashboard")
	adminDiagnosticsAll := flags.Bool("admin-diagnostics-all", false, "record every query/commit; short development sessions only")
	adminMetrics := flags.Bool("admin-metrics", false, "serve authenticated Prometheus metrics on the admin listener")
	workerAddress := flags.String("worker-addr", "", "optional loopback control address for server JavaScript workers")
	workerPublications := flags.String("worker-publications", "", "comma-separated collections whose query visibility is owned by the worker")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return errors.New("serve requires --db")
	}
	if !*devNoAuth {
		return errors.New("no production auth provider is configured; pass --dev-no-auth only for local development")
	}
	adminToken := ""
	if (*adminDiagnostics || *adminDiagnosticsAll || *adminMetrics) && *adminAddress == "" {
		return errors.New("--admin-diagnostics and --admin-metrics require --admin-addr")
	}
	if *adminDiagnosticsAll {
		*adminDiagnostics = true
	}
	if *adminAddress != "" {
		if !isLoopbackAddress(*adminAddress) {
			return errors.New("--admin-addr must be a loopback address; use the admin package behind TLS for remote access")
		}
		adminToken = os.Getenv("MELDBASE_ADMIN_TOKEN")
		if len(adminToken) < 32 {
			return errors.New("MELDBASE_ADMIN_TOKEN must contain at least 32 bytes when --admin-addr is set")
		}
	}
	workerToken := ""
	publicationCollections := splitCommaList(*workerPublications)
	if len(publicationCollections) > 0 && *workerAddress == "" {
		return errors.New("--worker-publications requires --worker-addr")
	}
	if *workerAddress != "" {
		if !isLoopbackAddress(*workerAddress) {
			return errors.New("--worker-addr must be a loopback address; mount WorkerHub behind TLS/mTLS for remote workers")
		}
		workerToken = os.Getenv("MELDBASE_WORKER_TOKEN")
		if len(workerToken) < 32 {
			return errors.New("MELDBASE_WORKER_TOKEN must contain at least 32 bytes when --worker-addr is set")
		}
	}
	if *realtimeURL == "" {
		*realtimeURL = defaultRealtimeURL(*address)
	}
	db, err := meldbase.Open(*path)
	if err != nil {
		return err
	}
	var workerHub *meldserver.WorkerHub
	var idempotency meldserver.RPCIdempotencyStore
	if *workerAddress != "" {
		workerAuthenticator, err := meldserver.NewWorkerTokenAuthenticator(workerToken)
		if err != nil {
			_ = db.Close()
			return err
		}
		policyGenerations, policyErr := meldserver.NewDurablePolicyGenerationStore(db)
		if policyErr != nil {
			_ = db.Close()
			return policyErr
		}
		workerHub, err = meldserver.NewWorkerHub(meldserver.WorkerHubConfig{
			Authenticator: workerAuthenticator, PublicationCollections: publicationCollections,
			PolicyGenerationStore: policyGenerations,
		})
		if err != nil {
			_ = db.Close()
			return err
		}
		idempotency, err = meldserver.NewDurableRPCIdempotencyStore(db)
		if err != nil {
			_ = db.Close()
			return err
		}
	}
	var queryPolicyResolver meldserver.QueryPolicyResolver
	if len(publicationCollections) > 0 {
		queryPolicyResolver = workerHub
	}
	handler, err := meldserver.New(meldserver.Config{
		DB: db, Authenticator: devAccess{}, Authorizer: devAccess{}, PublicRealtimeURL: *realtimeURL,
		OriginPatterns:     []string{"localhost:*", "127.0.0.1:*", "[::1]:*"},
		AllowedHTTPOrigins: []string{"http://localhost:5173", "http://127.0.0.1:5173"}, MaxBodyBytes: 1 << 20,
		RPCMethodResolver: workerHub, RPCTransactionalMethodResolver: workerHub, QueryPolicyResolver: queryPolicyResolver,
		RPCIdempotencyStore: idempotency, RPCAuthorizer: devAccess{},
	})
	if err != nil {
		_ = db.Close()
		return err
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		_ = db.Close()
		return err
	}
	httpServer := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	var workerServer *http.Server
	var workerListener net.Listener
	var workerServeError chan error
	var adminSampler *admin.Sampler
	var diagnostics *meldbase.Diagnostics
	var adminServer *http.Server
	var adminListener net.Listener
	var adminServeError chan error
	if *adminAddress != "" {
		if *adminDiagnostics {
			diagnostics, err = db.EnableDiagnostics(meldbase.DiagnosticsOptions{RecordAll: *adminDiagnosticsAll})
			if err != nil {
				_ = listener.Close()
				_ = db.Close()
				return err
			}
		}
		adminSampler, err = admin.NewSampler(db, admin.SamplerOptions{Server: handler})
		if err != nil {
			_ = diagnostics.Close()
			_ = listener.Close()
			_ = db.Close()
			return err
		}
		authorize, err := admin.NewBearerTokenAuthorizer(adminToken)
		if err != nil {
			_ = adminSampler.Close()
			_ = diagnostics.Close()
			_ = listener.Close()
			_ = db.Close()
			return err
		}
		adminHandler, err := admin.NewHandler(admin.HandlerOptions{
			Sampler: adminSampler, Authorize: authorize, ServeDashboard: true,
			ServeMetrics: *adminMetrics, Diagnostics: db,
		})
		if err != nil {
			_ = adminSampler.Close()
			_ = diagnostics.Close()
			_ = listener.Close()
			_ = db.Close()
			return err
		}
		adminListener, err = net.Listen("tcp", *adminAddress)
		if err != nil {
			_ = adminSampler.Close()
			_ = diagnostics.Close()
			_ = listener.Close()
			_ = db.Close()
			return err
		}
		adminServer = &http.Server{Handler: adminHandler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
		adminServeError = make(chan error, 1)
		go func() { adminServeError <- adminServer.Serve(adminListener) }()
		fmt.Fprintf(stdout, "Meldbase admin dashboard listening on http://%s\n", adminListener.Addr())
	}
	if workerHub != nil {
		workerListener, err = net.Listen("tcp", *workerAddress)
		if err != nil {
			if adminServer != nil {
				_ = adminServer.Close()
				_ = adminSampler.Close()
				_ = diagnostics.Close()
			}
			_ = listener.Close()
			_ = db.Close()
			return err
		}
		workerMux := http.NewServeMux()
		workerMux.Handle("GET /v1/workers", workerHub)
		workerServer = &http.Server{Handler: workerMux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
		workerServeError = make(chan error, 1)
		go func() { workerServeError <- workerServer.Serve(workerListener) }()
		fmt.Fprintf(stdout, "Meldbase worker control listening on ws://%s/v1/workers\n", workerListener.Addr())
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdown)
		if adminServer != nil {
			_ = adminServer.Shutdown(shutdown)
		}
		if workerServer != nil {
			_ = workerServer.Shutdown(shutdown)
		}
	}()
	fmt.Fprintf(stdout, "Meldbase listening on http://%s (development auth disabled)\n", listener.Addr())
	serveErr := httpServer.Serve(listener)
	var adminErr error
	if adminServer != nil {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = adminServer.Shutdown(shutdown)
		cancel()
		adminErr = <-adminServeError
		if errors.Is(adminErr, http.ErrServerClosed) {
			adminErr = nil
		}
		_ = adminSampler.Close()
		_ = diagnostics.Close()
	}
	var workerErr error
	if workerServer != nil {
		workerShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = workerServer.Shutdown(workerShutdown)
		cancel()
		workerErr = <-workerServeError
		if errors.Is(workerErr, http.ErrServerClosed) {
			workerErr = nil
		}
	}
	closeErr := db.Close()
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, adminErr, workerErr, closeErr)
}

func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func splitCommaList(raw string) []string {
	if raw == "" {
		return nil
	}
	values := strings.Split(raw, ",")
	for index := range values {
		values[index] = strings.TrimSpace(values[index])
	}
	return values
}

func defaultRealtimeURL(address string) string {
	host := address
	if strings.HasPrefix(host, ":") {
		host = "localhost" + host
	}
	if strings.HasPrefix(host, "0.0.0.0:") {
		host = "localhost:" + strings.TrimPrefix(host, "0.0.0.0:")
	}
	return "ws://" + host + "/v1/realtime"
}

type devAccess struct{}

func (devAccess) AuthenticateHTTP(*http.Request) (meldserver.Principal, error) {
	return meldserver.Principal{Subject: "development", Tenant: "development"}, nil
}
func (devAccess) AuthorizeQuery(context.Context, meldserver.Principal, string, meldbase.QuerySpec) (meldserver.QueryPolicy, error) {
	return meldserver.QueryPolicy{PolicyVersion: "development-v1", MaxResults: meldbase.DefaultQueryLimits.MaxLimit, AllowAllQueryPaths: true, AllowAllResultFields: true}, nil
}
func (devAccess) AuthorizeInsert(context.Context, meldserver.Principal, string, meldbase.Document) (meldserver.InsertPolicy, error) {
	return meldserver.InsertPolicy{AllowAllInputFields: true, AllowAllResultFields: true}, nil
}
func (devAccess) AuthorizeUpdate(context.Context, meldserver.Principal, string, meldbase.QuerySpec, meldbase.MutationSpec) (meldserver.UpdatePolicy, error) {
	return meldserver.UpdatePolicy{QueryPolicy: meldserver.QueryPolicy{PolicyVersion: "development-v1", AllowAllQueryPaths: true}, AllowAllUpdatePaths: true, MaxAffected: meldbase.DefaultQueryLimits.MaxLimit}, nil
}
func (devAccess) AuthorizeDelete(context.Context, meldserver.Principal, string, meldbase.QuerySpec) (meldserver.DeletePolicy, error) {
	return meldserver.DeletePolicy{QueryPolicy: meldserver.QueryPolicy{PolicyVersion: "development-v1", AllowAllQueryPaths: true}, MaxAffected: meldbase.DefaultQueryLimits.MaxLimit}, nil
}
func (devAccess) AuthorizeRPC(context.Context, meldserver.Principal, string) error { return nil }
