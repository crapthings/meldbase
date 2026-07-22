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

	"github.com/crapthings/meldbase/admin"
	"github.com/crapthings/meldbase/core"
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
		return errors.New("usage: meld <access-policy|init|demo|serve|anchor-serve|anchor-qualification|inspect|verify|backup|restore|export|import|durability-check|destructive-volume-check|destructive-process-check|destructive-enospc-check|destructive-power-prepare|destructive-power-controller-keygen|destructive-power-controller-run|destructive-qemu-reset|destructive-qemu-process-kill|destructive-power-recover|destructive-power-receipt-check|destructive-power-matrix-check|destructive-corruption-check|destructive-qemu-eio|destructive-qemu-volatile-loss|destructive-manifest-build|storage-soak|qualification-environment-capture|qualification-session-init|qualification-session-record|qualification-session-status|qualification-session-power-status|qualification-session-power-prepare|qualification-session-power-recover|qualification-session-seal|qualification-artifacts-index-build|qualification-artifacts-index-verify|qualification-check|qualification-packet-keygen|qualification-packet-verify|index-build>")
	}
	switch args[0] {
	case "access-policy":
		return runAccessPolicy(args[1:], stdout, stderr)
	case "init":
		return runInit(args[1:], stdout, stderr)
	case "demo":
		return runDemo(args[1:], stdout, stderr)
	case "serve":
		return runServe(args[1:], stdout, stderr)
	case "anchor-serve":
		return runAnchorServe(args[1:], stdout, stderr)
	case "anchor-qualification":
		return runAnchorQualification(args[1:], stdout, stderr)
	case "durability-check":
		return runDurabilityCheck(args[1:], stdout, stderr)
	case "destructive-process-check":
		return runDestructiveProcessCheck(args[1:], stdout, stderr)
	case "destructive-volume-check":
		return runDestructiveVolumeCheck(args[1:], stdout, stderr)
	case "destructive-enospc-check":
		return runDestructiveENOSPCCheck(args[1:], stdout, stderr)
	case "destructive-enospc-worker":
		return runDestructiveENOSPCWorker(args[1:], stderr)
	case "destructive-power-prepare":
		return runDestructivePowerPrepare(args[1:], stderr)
	case "destructive-power-controller-keygen":
		return runDestructivePowerControllerKeygen(args[1:], stdout, stderr)
	case "destructive-power-controller-run":
		return runDestructivePowerControllerRun(args[1:], stdout, stderr)
	case "destructive-qemu-reset":
		return runDestructiveQEMUReset(args[1:], stdout, stderr)
	case "destructive-qemu-process-kill":
		return runDestructiveQEMUProcessKill(args[1:], stdout, stderr)
	case "destructive-power-recover":
		return runDestructivePowerRecover(args[1:], stdout, stderr)
	case "destructive-power-receipt-check":
		return runDestructivePowerReceiptCheck(args[1:], stdout, stderr)
	case "destructive-power-matrix-check":
		return runDestructivePowerMatrixCheck(args[1:], stdout, stderr)
	case "destructive-manifest-build":
		return runDestructiveManifestBuild(args[1:], stdout, stderr)
	case "destructive-corruption-check":
		return runDestructiveCorruptionCheck(args[1:], stdout, stderr)
	case "destructive-corruption-receipt-check":
		return runDestructiveCorruptionReceiptCheck(args[1:], stdout, stderr)
	case "destructive-eio-seed":
		return runDestructiveEIOSeed(args[1:], stdout, stderr)
	case "destructive-eio-worker":
		return runDestructiveEIOWorker(args[1:], stdout, stderr)
	case "destructive-eio-result-check":
		return runDestructiveEIOResultCheck(args[1:], stdout, stderr)
	case "destructive-flush-eio-worker":
		return runDestructiveFlushEIOWorker(args[1:], stdout, stderr)
	case "destructive-flush-eio-recovery":
		return runDestructiveFlushEIORecovery(args[1:], stdout, stderr)
	case "destructive-flush-eio-recovery-plan":
		return runDestructiveFlushEIORecoveryPlan(args[1:], stdout, stderr)
	case "destructive-flush-eio-recovery-preflight":
		return runDestructiveFlushEIORecoveryPreflight(args[1:], stdout, stderr)
	case "destructive-flush-eio-result-check":
		return runDestructiveFlushEIOResultCheck(args[1:], stdout, stderr)
	case "destructive-qemu-eio":
		return runDestructiveQEMUEIO(args[1:], stdout, stderr)
	case "destructive-qemu-eio-proof-check":
		return runDestructiveQMPEIOProofCheck(args[1:], stdout, stderr)
	case "destructive-eio-bundle-check":
		return runDestructiveEIOBundleCheck(args[1:], stdout, stderr)
	case "destructive-qemu-flush-arm-probe":
		return runDestructiveQEMUFlushArmProbe(args[1:], stdout, stderr)
	case "destructive-qemu-flush-eio":
		return runDestructiveQEMUFlushEIO(args[1:], stdout, stderr)
	case "destructive-qemu-flush-eio-proof-check":
		return runDestructiveQMPFlushEIOProofCheck(args[1:], stdout, stderr)
	case "destructive-flush-eio-bundle-check":
		return runDestructiveFlushEIOBundleCheck(args[1:], stdout, stderr)
	case "destructive-volatile-loss-seed":
		return runDestructiveVolatileLossSeed(args[1:], stdout, stderr)
	case "destructive-volatile-loss-update":
		return runDestructiveVolatileLossUpdate(args[1:], stdout, stderr)
	case "destructive-volatile-loss-recover":
		return runDestructiveVolatileLossRecover(args[1:], stdout, stderr)
	case "destructive-volatile-loss-recovery-ready":
		return runDestructiveVolatileLossRecoveryReady(args[1:], stdout, stderr)
	case "destructive-qemu-volatile-loss":
		return runDestructiveQEMUVolatileLoss(args[1:], stdout, stderr)
	case "destructive-qemu-volatile-loss-proof-check":
		return runDestructiveQMPVolatileLossProofCheck(args[1:], stdout, stderr)
	case "destructive-volatile-loss-bundle-check":
		return runDestructiveVolatileLossBundleCheck(args[1:], stdout, stderr)
	case "destructive-process-worker":
		return runDestructiveProcessWorker(args[1:], stderr)
	case "storage-soak":
		return runStorageSoak(args[1:], stdout, stderr)
	case "qualification-check":
		return runQualificationCheck(args[1:], stdout, stderr)
	case "qualification-environment-capture":
		return runQualificationEnvironmentCapture(args[1:], stdout, stderr)
	case "qualification-session-init":
		return runQualificationSessionInit(args[1:], stdout, stderr)
	case "qualification-session-record":
		return runQualificationSessionRecord(args[1:], stdout, stderr)
	case "qualification-session-status":
		return runQualificationSessionStatus(args[1:], stdout, stderr)
	case "qualification-session-power-status":
		return runQualificationSessionPowerStatus(args[1:], stdout, stderr)
	case "qualification-session-power-prepare":
		return runQualificationSessionPowerPrepare(args[1:], stderr)
	case "qualification-session-power-recover":
		return runQualificationSessionPowerRecover(args[1:], stdout, stderr)
	case "qualification-session-seal":
		return runQualificationSessionSeal(args[1:], stdout, stderr)
	case "qualification-artifacts-index-build":
		return runQualificationArtifactsIndexBuild(args[1:], stdout, stderr)
	case "qualification-artifacts-index-verify":
		return runQualificationArtifactsIndexVerify(args[1:], stdout, stderr)
	case "qualification-packet-keygen":
		return runQualificationPacketKeygen(args[1:], stdout, stderr)
	case "qualification-packet-verify":
		return runQualificationPacketVerify(args[1:], stdout, stderr)
	case "inspect":
		return runInspect(args[1:], stdout, stderr)
	case "backup":
		return runBackup(args[1:], stdout, stderr)
	case "restore":
		return runRestore(args[1:], stdout, stderr)
	case "export":
		return runLogicalExport(args[1:], stdout, stderr)
	case "import":
		return runLogicalImport(args[1:], stdout, stderr)
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
	httpOrigins := flags.String("http-origins", "", "comma-separated exact browser HTTP(S) origins; defaults to local development origins")
	realtimeOriginPatterns := flags.String("realtime-origin-patterns", "", "comma-separated WebSocket Origin host or scheme+host patterns; defaults to local development origins")
	devNoAuth := flags.Bool("dev-no-auth", false, "explicitly allow all requests; development only")
	jwtHS256SecretFile := flags.String("jwt-hs256-secret-file", "", "private file containing a 32+ byte HS256 JWT secret")
	jwtJWKSURL := flags.String("jwt-jwks-url", "", "HTTPS OIDC JSON Web Key Set URL for RS256 JWT verification")
	jwtIssuer := flags.String("jwt-issuer", "", "required JWT issuer value")
	jwtAudience := flags.String("jwt-audience", "", "required JWT audience value")
	jwtWorkspaceClaim := flags.String("jwt-workspace-claim", "workspace_id", "JWT claim containing the active workspace ID")
	accessPolicyFile := flags.String("access-policy-file", "", "strict JSON collection access manifest")
	adminAddress := flags.String("admin-addr", "", "optional loopback address for the secured embedded admin dashboard")
	adminDiagnostics := flags.Bool("admin-diagnostics", false, "record bounded slow/failure diagnostics for the admin dashboard")
	adminDiagnosticsAll := flags.Bool("admin-diagnostics-all", false, "record every query/commit; short development sessions only")
	adminMetrics := flags.Bool("admin-metrics", false, "serve authenticated Prometheus metrics on the admin listener")
	workerAddress := flags.String("worker-addr", "", "optional loopback control address for server JavaScript workers")
	workerReadPolicies := flags.String("worker-read-policies", "", "comma-separated collections whose query visibility is owned by the worker")
	rollbackAnchorPath := flags.String("rollback-anchor", "", "independently trusted rollback-anchor file")
	rollbackAnchorInit := flags.Bool("rollback-anchor-init", false, "explicitly initialize an empty anchor from the current database")
	rollbackAnchorTimeout := flags.Duration("rollback-anchor-timeout", meldbase.DefaultRollbackAnchorOperationTimeout, "deadline for each rollback-anchor operation")
	rollbackAnchorCluster := flags.String("rollback-anchor-cluster", "", "remote anchor static cluster ID")
	rollbackAnchorName := flags.String("rollback-anchor-name", "", "remote anchor resource name")
	rollbackAnchorKeyID := flags.String("rollback-anchor-key-id", "", "remote anchor HMAC key ID")
	rollbackAnchorKeyFile := flags.String("rollback-anchor-key-file", "", "private base64 remote anchor HMAC key file")
	rollbackAnchorCA := flags.String("rollback-anchor-ca", "", "remote anchor server CA PEM")
	rollbackAnchorClientCert := flags.String("rollback-anchor-client-cert", "", "remote anchor mTLS client certificate PEM")
	rollbackAnchorClientKey := flags.String("rollback-anchor-client-key", "", "private remote anchor mTLS client key PEM")
	var rollbackAnchorReplicas anchorReplicaFlags
	flags.Var(&rollbackAnchorReplicas, "rollback-anchor-replica", "repeatable remote member-id=https://endpoint")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return errors.New("serve requires --db")
	}
	remoteAnchor := remoteAnchorConfig{
		clusterID: *rollbackAnchorCluster, replicaSpecs: rollbackAnchorReplicas, anchorName: *rollbackAnchorName,
		keyID: *rollbackAnchorKeyID, keyFile: *rollbackAnchorKeyFile, serverCAFile: *rollbackAnchorCA,
		clientCertFile: *rollbackAnchorClientCert, clientKeyFile: *rollbackAnchorClientKey, operationTimeout: *rollbackAnchorTimeout,
	}
	if *rollbackAnchorPath != "" && remoteAnchor.enabled() {
		return errors.New("local --rollback-anchor and remote rollback-anchor flags are mutually exclusive")
	}
	if *rollbackAnchorInit && *rollbackAnchorPath == "" && !remoteAnchor.enabled() {
		return errors.New("--rollback-anchor-init requires --rollback-anchor or a complete remote rollback anchor")
	}
	if (*rollbackAnchorPath != "" || remoteAnchor.enabled()) && *rollbackAnchorTimeout <= 0 {
		return errors.New("--rollback-anchor-timeout must be positive")
	}
	if *devNoAuth && (*jwtHS256SecretFile != "" || *jwtJWKSURL != "") {
		return errors.New("--dev-no-auth and production JWT authentication flags are mutually exclusive")
	}
	if !*devNoAuth && (*jwtHS256SecretFile == "" && *jwtJWKSURL == "") {
		return errors.New("configure --jwt-hs256-secret-file or --jwt-jwks-url for production auth, or pass --dev-no-auth only for local development")
	}
	if *jwtHS256SecretFile != "" && *jwtJWKSURL != "" {
		return errors.New("--jwt-hs256-secret-file and --jwt-jwks-url are mutually exclusive")
	}
	if !*devNoAuth && (*jwtIssuer == "" || *jwtAudience == "") {
		return errors.New("--jwt-issuer and --jwt-audience are required with production JWT auth")
	}
	if *devNoAuth && *accessPolicyFile != "" {
		return errors.New("--access-policy-file requires production JWT authentication")
	}
	if !*devNoAuth && *accessPolicyFile == "" {
		return errors.New("--access-policy-file is required with production JWT auth")
	}
	var workspaceAccessConfig meldserver.WorkspaceAuthorizerConfig
	if !*devNoAuth {
		manifestBytes, readErr := os.ReadFile(*accessPolicyFile)
		if readErr != nil {
			return fmt.Errorf("read collection access manifest: %w", readErr)
		}
		manifest, parseErr := meldserver.ParseCollectionAccessManifestJSON(manifestBytes)
		if parseErr != nil {
			return fmt.Errorf("parse collection access manifest: %w", parseErr)
		}
		workspaceAccessConfig, parseErr = manifest.WorkspaceAuthorizerConfig()
		if parseErr != nil {
			return fmt.Errorf("validate collection access manifest: %w", parseErr)
		}
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
	readPolicyCollections := splitCommaList(*workerReadPolicies)
	if len(readPolicyCollections) > 0 && *workerAddress == "" {
		return errors.New("--worker-read-policies requires --worker-addr")
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
	deriveRealtimeURLFromListener := *realtimeURL == "" && usesEphemeralPort(*address)
	if *realtimeURL == "" && !deriveRealtimeURLFromListener {
		*realtimeURL = defaultRealtimeURL(*address)
	}
	configuredHTTPOrigins := configuredOriginList(*httpOrigins, defaultHTTPOrigins())
	configuredRealtimeOriginPatterns := configuredOriginList(*realtimeOriginPatterns, defaultRealtimeOriginPatterns())
	openOptions := meldbase.OpenOptions{}
	var anchorTransport *http.Transport
	if *rollbackAnchorPath != "" {
		anchor, err := meldbase.NewFileRollbackAnchorStore(*rollbackAnchorPath)
		if err != nil {
			return err
		}
		openOptions.RollbackProtection = meldbase.RollbackProtection{
			AnchorStore: anchor, InitializeAnchor: *rollbackAnchorInit, OperationTimeout: *rollbackAnchorTimeout,
		}
	} else if remoteAnchor.enabled() {
		anchor, transport, err := newRemoteAnchorStore(remoteAnchor)
		if err != nil {
			return err
		}
		anchorTransport = transport
		defer anchorTransport.CloseIdleConnections()
		openOptions.RollbackProtection = meldbase.RollbackProtection{
			AnchorStore: anchor, InitializeAnchor: *rollbackAnchorInit, OperationTimeout: *rollbackAnchorTimeout,
		}
	}
	db, err := meldbase.OpenWithOptions(*path, openOptions)
	if err != nil {
		return err
	}
	var authenticator meldserver.Authenticator
	var authorizer meldserver.Authorizer
	var rpcAuthorizer meldserver.RPCAuthorizer
	if *devNoAuth {
		access := devAccess{}
		authenticator, authorizer, rpcAuthorizer = access, access, access
	} else {
		if *jwtHS256SecretFile != "" {
			secret, readErr := os.ReadFile(*jwtHS256SecretFile)
			if readErr != nil {
				_ = db.Close()
				return fmt.Errorf("read JWT HS256 secret: %w", readErr)
			}
			authenticator, err = meldserver.NewHS256JWTAuthenticator(meldserver.HS256JWTAuthenticatorConfig{
				Secret: secret, Issuer: *jwtIssuer, Audience: *jwtAudience, WorkspaceClaim: *jwtWorkspaceClaim,
			})
		} else {
			authenticator, err = meldserver.NewRS256JWKSAuthenticator(meldserver.RS256JWKSAuthenticatorConfig{
				JWKSURL: *jwtJWKSURL, Issuer: *jwtIssuer, Audience: *jwtAudience, WorkspaceClaim: *jwtWorkspaceClaim,
			})
		}
		if err != nil {
			_ = db.Close()
			return err
		}
		workspaceAccess, policyErr := meldserver.NewWorkspaceAuthorizer(workspaceAccessConfig)
		if policyErr != nil {
			_ = db.Close()
			return policyErr
		}
		authorizer, rpcAuthorizer = workspaceAccess, workspaceAccess
	}
	var workerHub *meldserver.WorkerHub
	var idempotency meldserver.RPCIdempotencyStore
	var rpcMethodResolver meldserver.RPCMethodResolver
	var rpcTransactionalMethodResolver meldserver.RPCTransactionalMethodResolver
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
			Authenticator: workerAuthenticator, ReadPolicyCollections: readPolicyCollections,
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
		rpcMethodResolver = workerHub
		rpcTransactionalMethodResolver = workerHub
	}
	var queryPolicyResolver meldserver.QueryPolicyResolver
	if len(readPolicyCollections) > 0 {
		queryPolicyResolver = workerHub
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		_ = db.Close()
		return err
	}
	if deriveRealtimeURLFromListener {
		*realtimeURL = defaultRealtimeURL(listener.Addr().String())
	}
	handler, err := meldserver.New(meldserver.Config{
		DB: db, Authenticator: authenticator, Authorizer: authorizer, PublicRealtimeURL: *realtimeURL,
		OriginPatterns:     configuredRealtimeOriginPatterns,
		AllowedHTTPOrigins: configuredHTTPOrigins, MaxBodyBytes: 1 << 20,
		RPCMethodResolver: rpcMethodResolver, RPCTransactionalMethodResolver: rpcTransactionalMethodResolver, QueryPolicyResolver: queryPolicyResolver,
		RPCIdempotencyStore: idempotency, RPCAuthorizer: rpcAuthorizer,
	})
	if err != nil {
		_ = listener.Close()
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
			ServeMetrics: *adminMetrics, Diagnostics: db, IndexCatalog: db,
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
	if *devNoAuth {
		fmt.Fprintf(stdout, "Meldbase listening on http://%s (UNSAFE development auth disabled)\n", listener.Addr())
	} else {
		fmt.Fprintf(stdout, "Meldbase listening on http://%s (JWT workspace isolation enabled)\n", listener.Addr())
	}
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

func configuredOriginList(raw string, defaults []string) []string {
	configured := splitCommaList(raw)
	if len(configured) != 0 {
		return configured
	}
	return append([]string(nil), defaults...)
}

func defaultHTTPOrigins() []string {
	return []string{"http://localhost:5173", "http://127.0.0.1:5173", "http://[::1]:5173"}
}

func defaultRealtimeOriginPatterns() []string {
	// path.Match treats '[' as syntax. A literal IPv6 host bracket must therefore
	// use its escaped character-class form.
	return []string{"localhost:*", "127.0.0.1:*", "[[]::1]:*"}
}

func defaultRealtimeURL(address string) string {
	host := address
	if strings.HasPrefix(host, ":") {
		host = "localhost" + host
	}
	if strings.HasPrefix(host, "0.0.0.0:") {
		host = "localhost:" + strings.TrimPrefix(host, "0.0.0.0:")
	}
	if strings.HasPrefix(host, "[::]:") {
		host = "localhost:" + strings.TrimPrefix(host, "[::]:")
	}
	return "ws://" + host + "/v1/realtime"
}

func usesEphemeralPort(address string) bool {
	_, port, err := net.SplitHostPort(address)
	return err == nil && port == "0"
}

type devAccess struct{}

func (devAccess) AuthenticateHTTP(*http.Request) (meldserver.Actor, error) {
	return meldserver.Actor{ID: "development", WorkspaceID: "development"}, nil
}
func (devAccess) AuthorizeQuery(context.Context, meldserver.Actor, string, meldbase.QuerySpec) (meldserver.QueryPolicy, error) {
	return meldserver.QueryPolicy{PolicyVersion: "development-v1", MaxResults: meldbase.DefaultQueryLimits.MaxLimit, AllowAllQueryPaths: true, AllowAllAggregateFields: true, AllowAllResultFields: true}, nil
}
func (devAccess) AuthorizeInsert(context.Context, meldserver.Actor, string, meldbase.Document) (meldserver.InsertPolicy, error) {
	return meldserver.InsertPolicy{AllowAllInputFields: true, AllowAllResultFields: true}, nil
}
func (devAccess) AuthorizeUpdate(context.Context, meldserver.Actor, string, meldbase.QuerySpec, meldbase.MutationSpec) (meldserver.UpdatePolicy, error) {
	return meldserver.UpdatePolicy{QueryPolicy: meldserver.QueryPolicy{PolicyVersion: "development-v1", AllowAllQueryPaths: true}, AllowAllUpdatePaths: true, MaxAffected: meldbase.DefaultQueryLimits.MaxLimit}, nil
}
func (devAccess) AuthorizeDelete(context.Context, meldserver.Actor, string, meldbase.QuerySpec) (meldserver.DeletePolicy, error) {
	return meldserver.DeletePolicy{QueryPolicy: meldserver.QueryPolicy{PolicyVersion: "development-v1", AllowAllQueryPaths: true}, MaxAffected: meldbase.DefaultQueryLimits.MaxLimit}, nil
}
func (devAccess) AuthorizeRPC(context.Context, meldserver.Actor, string) error { return nil }
