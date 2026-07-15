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
	meldserver "github.com/crapthings/meldbase/internal/server"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "meld:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: meld <demo|serve>")
	}
	switch args[0] {
	case "demo":
		return runDemo(args[1:], stdout, stderr)
	case "serve":
		return runServe(args[1:], stdout, stderr)
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
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return errors.New("serve requires --db")
	}
	if !*devNoAuth {
		return errors.New("no production auth provider is configured; pass --dev-no-auth only for local development")
	}
	if *realtimeURL == "" {
		*realtimeURL = defaultRealtimeURL(*address)
	}
	db, err := meldbase.Open(*path)
	if err != nil {
		return err
	}
	handler, err := meldserver.New(meldserver.Config{
		DB: db, Authenticator: devAccess{}, Authorizer: devAccess{}, PublicRealtimeURL: *realtimeURL,
		OriginPatterns:     []string{"localhost:*", "127.0.0.1:*", "[::1]:*"},
		AllowedHTTPOrigins: []string{"http://localhost:5173", "http://127.0.0.1:5173"}, MaxBodyBytes: 1 << 20,
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdown)
	}()
	fmt.Fprintf(stdout, "Meldbase listening on http://%s (development auth disabled)\n", listener.Addr())
	serveErr := httpServer.Serve(listener)
	closeErr := db.Close()
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return errors.Join(serveErr, closeErr)
	}
	return closeErr
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
