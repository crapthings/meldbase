package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	meldserver "github.com/crapthings/meldbase/server"
)

const defaultCollectionNames = "projects,tasks,comments"

// runInit creates one self-contained, local single-node bundle. It creates a
// new directory only: a previously created bundle is never modified or reused.
func runInit(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", "", "new directory for the local single-node bundle")
	issuer := flags.String("jwt-issuer", "meldbase-local", "JWT issuer expected by the database")
	audience := flags.String("jwt-audience", "meldbase-api", "JWT audience expected by the database")
	collections := flags.String("collections", defaultCollectionNames, "comma-separated collaborative collections to seed into the generated access policy")
	workspaceField := flags.String("workspace-field", "workspaceId", "server-owned document field containing the workspace ID")
	address := flags.String("addr", "127.0.0.1:8080", "loopback HTTP listen address")
	adminAddress := flags.String("admin-addr", "127.0.0.1:9091", "loopback embedded admin dashboard address")
	publicRealtimeURL := flags.String("public-realtime-url", "", "optional public ws(s) URL ending in /v1/realtime")
	httpOrigins := flags.String("http-origins", strings.Join(defaultHTTPOrigins(), ","), "comma-separated exact browser HTTP(S) origins")
	realtimeOriginPatterns := flags.String("realtime-origin-patterns", strings.Join(defaultRealtimeOriginPatterns(), ","), "comma-separated WebSocket Origin host or scheme+host patterns")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *directory == "" {
		return errors.New("init requires --dir")
	}
	if err := validateInitValue("jwt-issuer", *issuer); err != nil {
		return err
	}
	if err := validateInitValue("jwt-audience", *audience); err != nil {
		return err
	}
	if err := validateInitValue("collections", *collections); err != nil {
		return err
	}
	if err := validateInitValue("workspace-field", *workspaceField); err != nil {
		return err
	}
	if err := validateInitValue("http-origins", *httpOrigins); err != nil {
		return err
	}
	if err := validateInitValue("realtime-origin-patterns", *realtimeOriginPatterns); err != nil {
		return err
	}
	collectionAccess := make([]meldserver.CollectionAccess, 0, len(splitCommaList(*collections)))
	for _, collection := range splitCommaList(*collections) {
		collectionAccess = append(collectionAccess, meldserver.CollectionAccess{Collection: collection, Mode: meldserver.CollectionAccessCollaborative})
	}
	if _, err := meldserver.NewWorkspaceAuthorizer(meldserver.WorkspaceAuthorizerConfig{
		CollectionAccess: collectionAccess, WorkspaceField: *workspaceField,
	}); err != nil {
		return fmt.Errorf("invalid default collection access policy: %w", err)
	}
	if !isLoopbackAddress(*address) {
		return errors.New("--addr must be a loopback address")
	}
	if !isLoopbackAddress(*adminAddress) {
		return errors.New("--admin-addr must be a loopback address")
	}

	root, err := filepath.Abs(*directory)
	if err != nil {
		return fmt.Errorf("resolve --dir: %w", err)
	}
	root = filepath.Clean(root)
	if err := os.Mkdir(root, 0o750); err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("init directory already exists: %s", root)
		}
		return fmt.Errorf("create init directory: %w", err)
	}

	dataDirectory := filepath.Join(root, "data")
	backupDirectory := filepath.Join(root, "backups")
	rehearsalDirectory := filepath.Join(root, "rehearsals")
	configDirectory := filepath.Join(root, "config")
	secretDirectory := filepath.Join(root, "secrets")
	for _, path := range []string{dataDirectory, backupDirectory, rehearsalDirectory} {
		if err := os.Mkdir(path, 0o750); err != nil {
			return fmt.Errorf("create %s: %w", path, err)
		}
	}
	for _, path := range []string{configDirectory, secretDirectory} {
		if err := os.Mkdir(path, 0o700); err != nil {
			return fmt.Errorf("create %s: %w", path, err)
		}
	}

	jwtSecret, err := randomURLToken(48)
	if err != nil {
		return fmt.Errorf("generate JWT secret: %w", err)
	}
	adminToken, err := randomURLToken(48)
	if err != nil {
		return fmt.Errorf("generate admin token: %w", err)
	}
	jwtSecretPath := filepath.Join(secretDirectory, "jwt-hs256.secret")
	if err := writeNewPrivateFile(jwtSecretPath, []byte(jwtSecret+"\n"), 0o600); err != nil {
		return fmt.Errorf("write JWT secret: %w", err)
	}
	accessPolicyPath := filepath.Join(configDirectory, "access-policy.json")
	accessPolicy, err := json.MarshalIndent(meldserver.CollectionAccessManifest{
		SchemaURL: meldserver.CollectionAccessManifestSchemaURL,
		Version:   meldserver.CollectionAccessManifestVersion, WorkspaceField: *workspaceField, Collections: collectionAccess,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode collection access policy: %w", err)
	}
	if err := writeNewPrivateFile(accessPolicyPath, append(accessPolicy, '\n'), 0o600); err != nil {
		return fmt.Errorf("write collection access policy: %w", err)
	}

	configPath := filepath.Join(configDirectory, "meldbase.env")
	config := singleNodeEnv(map[string]string{
		"MELDBASE_DB":                       filepath.Join(dataDirectory, "app.meld"),
		"MELDBASE_ADDR":                     *address,
		"MELDBASE_ADMIN_ADDR":               *adminAddress,
		"MELDBASE_ADMIN_TOKEN":              adminToken,
		"MELDBASE_JWT_HS256_SECRET_FILE":    jwtSecretPath,
		"MELDBASE_JWT_ISSUER":               *issuer,
		"MELDBASE_JWT_AUDIENCE":             *audience,
		"MELDBASE_ACCESS_POLICY_FILE":       accessPolicyPath,
		"MELDBASE_PUBLIC_REALTIME_URL":      *publicRealtimeURL,
		"MELDBASE_HTTP_ORIGINS":             *httpOrigins,
		"MELDBASE_REALTIME_ORIGIN_PATTERNS": *realtimeOriginPatterns,
	})
	if err := writeNewPrivateFile(configPath, []byte(config), 0o600); err != nil {
		return fmt.Errorf("write local configuration: %w", err)
	}

	startPath := filepath.Join(root, "start.sh")
	if err := writeNewPrivateFile(startPath, []byte(singleNodeStartScript()), 0o750); err != nil {
		return fmt.Errorf("write local launcher: %w", err)
	}

	fmt.Fprintf(stdout, "Initialized local single-node bundle: %s\n", root)
	fmt.Fprintf(stdout, "Start it with: %s\n", startPath)
	fmt.Fprintf(stdout, "Admin dashboard: http://%s/\n", *adminAddress)
	fmt.Fprintf(stdout, "Admin token: %s (kept private; it is not printed)\n", configPath)
	fmt.Fprintln(stdout, "The bundle expects JWTs signed with secrets/jwt-hs256.secret and containing sub (actor ID), workspace_id (actor tenant ID), exp, iss, and aud.")
	return nil
}

func validateInitValue(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("--%s must not be empty", name)
	}
	if strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("--%s must not contain a line break", name)
	}
	return nil
}

func randomURLToken(size int) (string, error) {
	if size <= 0 {
		return "", errors.New("random token size must be positive")
	}
	bytes := make([]byte, size)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

func writeNewPrivateFile(path string, content []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(content)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func singleNodeEnv(values map[string]string) string {
	keys := []string{
		"MELDBASE_DB", "MELDBASE_ADDR", "MELDBASE_ADMIN_ADDR", "MELDBASE_ADMIN_TOKEN",
		"MELDBASE_JWT_HS256_SECRET_FILE", "MELDBASE_JWT_ISSUER", "MELDBASE_JWT_AUDIENCE",
		"MELDBASE_ACCESS_POLICY_FILE", "MELDBASE_PUBLIC_REALTIME_URL", "MELDBASE_HTTP_ORIGINS",
		"MELDBASE_REALTIME_ORIGIN_PATTERNS",
	}
	var output strings.Builder
	output.WriteString("# Generated by `meld init`; contains the local admin token. Keep mode 0600.\n")
	for _, key := range keys {
		fmt.Fprintf(&output, "%s=%s\n", key, shellQuote(values[key]))
	}
	return output.String()
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func singleNodeStartScript() string {
	return `#!/bin/sh
set -eu

bundle_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
set -a
. "$bundle_dir/config/meldbase.env"
set +a

set -- \
  --db "$MELDBASE_DB" \
  --addr "$MELDBASE_ADDR" \
  --jwt-hs256-secret-file "$MELDBASE_JWT_HS256_SECRET_FILE" \
  --jwt-issuer "$MELDBASE_JWT_ISSUER" \
  --jwt-audience "$MELDBASE_JWT_AUDIENCE" \
  --access-policy-file "$MELDBASE_ACCESS_POLICY_FILE" \
  --http-origins "$MELDBASE_HTTP_ORIGINS" \
  --realtime-origin-patterns "$MELDBASE_REALTIME_ORIGIN_PATTERNS" \
  --admin-addr "$MELDBASE_ADMIN_ADDR" \
  --admin-diagnostics \
  --admin-metrics
if [ -n "$MELDBASE_PUBLIC_REALTIME_URL" ]; then
  set -- "$@" --public-realtime-url "$MELDBASE_PUBLIC_REALTIME_URL"
fi

exec "${MELDBASE_BIN:-meld}" serve "$@"
`
}
