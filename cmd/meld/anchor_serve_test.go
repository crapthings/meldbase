package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
	"github.com/crapthings/meldbase/integrations/anchorhttp"
)

type anchorTestPKI struct {
	server       tls.Certificate
	client       tls.Certificate
	rootPool     *x509.CertPool
	serverPEM    []byte
	keyPEM       []byte
	clientPEM    []byte
	clientKeyPEM []byte
	caPEM        []byte
}

func TestAnchorServeRequiresPrivateFilesAndCompleteConfiguration(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := validateAnchorDirectory(directory); err == nil {
		t.Fatal("shared anchor directory accepted")
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := validateAnchorDirectory(directory); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "anchor.key")
	secret := []byte("0123456789abcdef0123456789abcdef")
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(secret)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	keys, err := loadAnchorKeys([]string{"primary=" + keyPath})
	if err != nil || string(keys["primary"]) != string(secret) {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	if err := os.Chmod(keyPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAnchorKeys([]string{"primary=" + keyPath}); err == nil {
		t.Fatal("shared HMAC key accepted")
	}
	pki := newAnchorTestPKI(t)
	tlsDirectory := t.TempDir()
	certificatePath := filepath.Join(tlsDirectory, "server.crt")
	privateKeyPath := filepath.Join(tlsDirectory, "server.key")
	clientCAPath := filepath.Join(tlsDirectory, "client-ca.crt")
	if err := os.WriteFile(certificatePath, pki.serverPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(privateKeyPath, pki.keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(clientCAPath, pki.caPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAnchorCertificate(certificatePath, privateKeyPath); err != nil {
		t.Fatal(err)
	}
	if pool, err := loadAnchorClientCAs(clientCAPath); err != nil || pool == nil {
		t.Fatalf("client CA pool=%v err=%v", pool, err)
	}
	if err := os.Chmod(privateKeyPath, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAnchorCertificate(certificatePath, privateKeyPath); err == nil {
		t.Fatal("shared TLS private key accepted")
	}
	if err := runAnchorServeContext(context.Background(), nil, ioDiscard{}, ioDiscard{}); err == nil {
		t.Fatal("incomplete anchor service configuration accepted")
	}
}

func TestAnchorServeMTLSQuorumPartitionAndGracefulRejoin(t *testing.T) {
	pki := newAnchorTestPKI(t)
	members := []string{"member-a", "member-b", "member-c"}
	secret := []byte("0123456789abcdef0123456789abcdef")
	type runningNode struct {
		cancel  context.CancelFunc
		done    chan error
		address string
		url     string
	}
	directories := make([]string, len(members))
	for index := range directories {
		directories[index] = t.TempDir()
		if err := os.Chmod(directories[index], 0o700); err != nil {
			t.Fatal(err)
		}
	}
	start := func(index int, listenAddress string) runningNode {
		ctx, cancel := context.WithCancel(context.Background())
		ready := make(chan net.Addr, 1)
		done := make(chan error, 1)
		options := anchorServeOptions{
			address: listenAddress, directory: directories[index], clusterID: "qualification-cluster", members: members, memberID: members[index],
			keys: map[string][]byte{"primary": secret}, certificate: pki.server, clientCAs: pki.rootPool,
			maxClockSkew: 30 * time.Second, shutdownTimeout: 3 * time.Second, ready: ready,
		}
		go func() { done <- serveAnchor(ctx, options, ioDiscard{}) }()
		select {
		case address := <-ready:
			return runningNode{cancel: cancel, done: done, address: address.String(), url: "https://" + address.String()}
		case err := <-done:
			t.Fatalf("anchor member failed before ready: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("anchor member did not become ready")
		}
		return runningNode{}
	}
	stop := func(node runningNode) {
		node.cancel()
		select {
		case err := <-node.done:
			if err != nil {
				t.Fatalf("graceful anchor shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("anchor shutdown timed out")
		}
	}
	nodes := []runningNode{start(0, "127.0.0.1:0"), start(1, "127.0.0.1:0"), start(2, "127.0.0.1:0")}
	t.Cleanup(func() {
		for _, node := range nodes {
			if node.cancel != nil {
				node.cancel()
			}
		}
	})
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pki.rootPool, Certificates: []tls.Certificate{pki.client}}}
	client := &http.Client{Transport: transport, Timeout: time.Second}
	defer transport.CloseIdleConnections()
	response, err := client.Get(nodes[0].url + "/readyz")
	if err != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("ready probe status=%v err=%v", statusCode(response), err)
	}
	_ = response.Body.Close()
	withoutClientCertificate := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pki.rootPool}}, Timeout: time.Second}
	if _, err := withoutClientCertificate.Get(nodes[0].url + "/livez"); err == nil {
		t.Fatal("TLS client without certificate reached anchor service")
	}
	tls12Only := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MaxVersion: tls.VersionTLS12, RootCAs: pki.rootPool, Certificates: []tls.Certificate{pki.client}}}, Timeout: time.Second}
	if _, err := tls12Only.Get(nodes[0].url + "/livez"); err == nil {
		t.Fatal("TLS 1.2 client reached TLS 1.3 anchor service")
	}
	clientFiles := t.TempDir()
	clientCertificatePath := filepath.Join(clientFiles, "client.crt")
	clientKeyPath := filepath.Join(clientFiles, "client.key")
	serverCAPath := filepath.Join(clientFiles, "server-ca.crt")
	hmacKeyPath := filepath.Join(clientFiles, "anchor.key")
	for path, file := range map[string]struct {
		raw  []byte
		mode os.FileMode
	}{
		clientCertificatePath: {pki.clientPEM, 0o644}, clientKeyPath: {pki.clientKeyPEM, 0o600},
		serverCAPath: {pki.caPEM, 0o644}, hmacKeyPath: {[]byte(base64.StdEncoding.EncodeToString(secret) + "\n"), 0o600},
	} {
		if err := os.WriteFile(path, file.raw, file.mode); err != nil {
			t.Fatal(err)
		}
	}
	newStore := func(current []runningNode) (*anchorhttp.QuorumStore, *http.Transport) {
		replicas := make([]string, len(current))
		for index := range current {
			replicas[index] = members[index] + "=" + current[index].url
		}
		store, anchorTransport, err := newRemoteAnchorStore(remoteAnchorConfig{
			clusterID: "qualification-cluster", replicaSpecs: replicas, anchorName: "orders", keyID: "primary", keyFile: hmacKeyPath,
			serverCAFile: serverCAPath, clientCertFile: clientCertificatePath, clientKeyFile: clientKeyPath, operationTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		return store, anchorTransport
	}
	store, anchorTransport := newStore(nodes)
	defer func() { anchorTransport.CloseIdleConnections() }()
	if anchorTransport.Proxy != nil || anchorTransport.TLSClientConfig.MinVersion != tls.VersionTLS13 || len(anchorTransport.TLSClientConfig.Certificates) != 1 {
		t.Fatal("remote anchor transport did not preserve strict direct mTLS configuration")
	}
	databasePath := filepath.Join(t.TempDir(), "remote-protected.meld")
	_, receiptPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	receiptKeyPath := filepath.Join(clientFiles, "qualification-signing.key")
	receiptPublicPath := filepath.Join(clientFiles, "qualification-verification.pub")
	if err := writeAnchorQualificationKey(receiptKeyPath, receiptPrivateKey, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeAnchorQualificationKey(receiptPublicPath, receiptPrivateKey.Public().(ed25519.PublicKey), 0o644); err != nil {
		t.Fatal(err)
	}
	receiptDirectory := t.TempDir()
	probe := func(phase, previous string) string {
		t.Helper()
		path := filepath.Join(receiptDirectory, phase+".json")
		arguments := []string{
			"probe", "--phase", phase, "--db", databasePath, "--out", path, "--signing-key", receiptKeyPath, "--timeout", "1s",
			"--external-evidence-sha256", strings.Repeat(string(rune('1'+anchorQualificationPhaseIndex(phase))), 64),
			"--cluster", "qualification-cluster", "--anchor-name", "orders", "--key-id", "primary", "--key-file", hmacKeyPath,
			"--ca", serverCAPath, "--client-cert", clientCertificatePath, "--client-key", clientKeyPath,
		}
		if previous != "" {
			arguments = append(arguments, "--previous", previous)
		}
		for index, node := range nodes {
			arguments = append(arguments, "--replica", members[index]+"="+node.url)
		}
		if err := runAnchorQualification(arguments, ioDiscard{}, ioDiscard{}); err != nil {
			t.Fatalf("qualification phase %s: %v", phase, err)
		}
		return path
	}
	db, err := meldbase.OpenWithOptions(databasePath, meldbase.OpenOptions{RollbackProtection: meldbase.RollbackProtection{
		AnchorStore: store, InitializeAnchor: true, OperationTimeout: time.Second,
	}})
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); err != nil {
		t.Fatal(err)
	}
	staleDatabase, err := os.ReadFile(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	healthyReceipt := probe("healthy", "")
	db, err = meldbase.OpenWithOptions(databasePath, meldbase.OpenOptions{RollbackProtection: meldbase.RollbackProtection{AnchorStore: store, OperationTimeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	items = db.Collection("items")
	stop(nodes[2])
	nodes[2].cancel = nil
	if _, err := items.InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(2)}); err != nil {
		t.Fatalf("one stopped member broke database commit quorum: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	degradedReceipt := probe("degraded", healthyReceipt)
	db, err = meldbase.OpenWithOptions(databasePath, meldbase.OpenOptions{RollbackProtection: meldbase.RollbackProtection{AnchorStore: store, OperationTimeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	items = db.Collection("items")
	stop(nodes[1])
	nodes[1].cancel = nil
	if _, err := items.InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(3)}); !errors.Is(err, meldbase.ErrDurability) {
		t.Fatalf("minority database write error=%v", err)
	}
	if !db.Stats().WritesDisabled {
		t.Fatal("lost anchor quorum did not fail-stop database writes")
	}
	if err := db.Close(); err != nil && !errors.Is(err, meldbase.ErrDurability) {
		t.Fatalf("fail-stopped database close error=%v", err)
	}
	minorityReceipt := probe("minority", degradedReceipt)
	nodeOneAddress, nodeTwoAddress := nodes[1].address, nodes[2].address
	nodes[1] = start(1, nodeOneAddress)
	nodes[2] = start(2, nodeTwoAddress)
	recoveredReceipt := probe("recovered", minorityReceipt)
	db, err = meldbase.OpenWithOptions(databasePath, meldbase.OpenOptions{RollbackProtection: meldbase.RollbackProtection{
		AnchorStore: store, OperationTimeout: time.Second,
	}})
	if err != nil {
		t.Fatalf("rejoined member did not restore database quorum: %v", err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(4)}); err != nil {
		t.Fatalf("database did not resume commits after quorum recovery: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(databasePath, staleDatabase, 0o600); err != nil {
		t.Fatal(err)
	}
	rollbackReceipt := probe("rollback-rejected", recoveredReceipt)
	verifyArguments := []string{"verify", "--public-key", receiptPublicPath, "--require-complete"}
	for _, receipt := range []string{healthyReceipt, degradedReceipt, minorityReceipt, recoveredReceipt, rollbackReceipt} {
		verifyArguments = append(verifyArguments, "--receipt", receipt)
	}
	if err := runAnchorQualification(verifyArguments, ioDiscard{}, ioDiscard{}); err != nil {
		t.Fatalf("complete real-network qualification chain: %v", err)
	}

	// Run two independent mTLS agents concurrently against the real TCP quorum,
	// preserve their signed fragments, then bind the exact history to a fresh
	// full-membership observation and verify the resulting receipt offline.
	retained, exists, err := store.Load(context.Background())
	if err != nil || !exists {
		t.Fatalf("history qualification initial load=%+v exists=%t err=%v", retained, exists, err)
	}
	historyRunID, err := newAnchorQualificationRunID()
	if err != nil {
		t.Fatal(err)
	}
	controllerCertificate, err := x509.ParseCertificate(pki.client.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	type runningAgent struct {
		cancel    context.CancelFunc
		done      chan error
		url       string
		publicKey ed25519.PublicKey
		transport *http.Transport
	}
	startAgent := func(agentID string) runningAgent {
		t.Helper()
		agentStore, agentTransport := newStore(nodes)
		publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		ready := make(chan net.Addr, 1)
		done := make(chan error, 1)
		agentStateDirectory := t.TempDir()
		if err := os.Chmod(agentStateDirectory, 0o700); err != nil {
			t.Fatal(err)
		}
		go func() {
			done <- serveAnchorHistoryAgent(ctx, anchorHistoryAgentOptions{
				address: "127.0.0.1:0", agentID: agentID, stateDirectory: agentStateDirectory, controllerSPKISHA256: anchorHistoryCertificateSPKI(controllerCertificate),
				sourceRevision: qualificationTestRevision, buildRevision: qualificationTestRevision, goos: "linux", goarch: "amd64", goVersion: "go1.25.0",
				certificate: pki.server, controllerCAs: pki.rootPool, signingKey: privateKey, executor: agentStore,
				operationTimeout: time.Second, shutdownTimeout: 3 * time.Second, ready: ready,
			}, ioDiscard{})
		}()
		select {
		case address := <-ready:
			return runningAgent{cancel: cancel, done: done, url: "https://" + address.String(), publicKey: publicKey, transport: agentTransport}
		case err := <-done:
			t.Fatalf("history agent failed before ready: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("history agent did not become ready")
		}
		return runningAgent{}
	}
	agents := []runningAgent{startAgent("writer-a"), startAgent("writer-b")}
	defer func() {
		for _, agent := range agents {
			agent.cancel()
			<-agent.done
			agent.transport.CloseIdleConnections()
		}
	}()
	targetA := anchorHistoryWireValueFromAnchor(meldbase.RollbackAnchor{
		DatabaseID: retained.DatabaseID, MinimumCommitSequence: retained.MinimumCommitSequence + 1, MinimumGeneration: retained.MinimumGeneration + 100,
	}, true)
	targetB := anchorHistoryWireValueFromAnchor(meldbase.RollbackAnchor{
		DatabaseID: retained.DatabaseID, MinimumCommitSequence: retained.MinimumCommitSequence + 2, MinimumGeneration: retained.MinimumGeneration + 99,
	}, true)
	plan := anchorHistoryRunPlan{
		SchemaVersion: anchorHistoryRunPlanSchema, RunID: historyRunID, SourceRevision: qualificationTestRevision,
		Agents: []anchorHistoryRunPlanAgent{
			{AgentID: "writer-a", Endpoint: agents[0].url, SigningPublicKey: base64PublicKey(agents[0].publicKey)},
			{AgentID: "writer-b", Endpoint: agents[1].url, SigningPublicKey: base64PublicKey(agents[1].publicKey)},
		},
		Waves: []anchorHistoryRunPlanWave{{Operations: []anchorHistoryRunPlanOperation{
			{ID: "crossed-a", AgentID: "writer-a", Kind: "advance", Target: targetA},
			{ID: "crossed-b", AgentID: "writer-b", Kind: "advance", Target: targetB},
		}}},
	}
	planPath := filepath.Join(receiptDirectory, "history-plan.json")
	controllerPath := filepath.Join(receiptDirectory, "controller-history.json")
	historyReceiptPath := filepath.Join(receiptDirectory, "history-receipt.json")
	if err := writeJSONExclusiveDurable(planPath, plan); err != nil {
		t.Fatal(err)
	}
	historyRunArguments := []string{
		"history-run", "--plan", planPath, "--out", controllerPath, "--timeout", "1s", "--source-revision", qualificationTestRevision,
		"--agent-ca", serverCAPath, "--agent-client-cert", clientCertificatePath, "--agent-client-key", clientKeyPath,
		"--cluster", "qualification-cluster", "--anchor-name", "orders", "--key-id", "primary", "--key-file", hmacKeyPath,
		"--ca", serverCAPath, "--client-cert", clientCertificatePath, "--client-key", clientKeyPath,
	}
	for index, node := range nodes {
		historyRunArguments = append(historyRunArguments, "--replica", members[index]+"="+node.url)
	}
	if err := runAnchorQualification(historyRunArguments, ioDiscard{}, ioDiscard{}); err != nil {
		t.Fatalf("run real multi-agent history qualification: %v", err)
	}
	var controller anchorHistoryControllerRecord
	if _, err := readQualificationReceipt(controllerPath, &controller); err != nil {
		t.Fatal(err)
	}
	if len(controller.Agents) != 2 || len(controller.Fragments) != 2 || len(controller.Operations) != 2 {
		t.Fatalf("incomplete multi-agent controller evidence: %+v", controller)
	}
	succeeded, failed := 0, 0
	for _, operation := range controller.Operations {
		if operation.Outcome == "succeeded" {
			succeeded++
		} else if operation.Outcome == "failed" {
			failed++
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("crossed writers outcomes succeeded=%d failed=%d", succeeded, failed)
	}
	if controller.Operations[0].Return < controller.Operations[1].Invoke || controller.Operations[1].Return < controller.Operations[0].Invoke {
		t.Fatalf("controller wave did not preserve overlapping call intervals: %+v", controller.Operations)
	}
	historySignArguments := []string{
		"history-sign", "--history", controllerPath, "--out", historyReceiptPath, "--signing-key", receiptKeyPath, "--timeout", "1s", "--source-revision", qualificationTestRevision,
		"--external-evidence-sha256", strings.Repeat("9", 64),
		"--cluster", "qualification-cluster", "--anchor-name", "orders", "--key-id", "primary", "--key-file", hmacKeyPath,
		"--ca", serverCAPath, "--client-cert", clientCertificatePath, "--client-key", clientKeyPath,
	}
	for index, node := range nodes {
		historySignArguments = append(historySignArguments, "--replica", members[index]+"="+node.url)
	}
	if err := runAnchorQualification(historySignArguments, ioDiscard{}, ioDiscard{}); err != nil {
		t.Fatalf("sign real-network history qualification: %v", err)
	}
	if err := runAnchorQualification([]string{
		"history-verify", "--public-key", receiptPublicPath, "--receipt", historyReceiptPath, "--history", controllerPath,
	}, ioDiscard{}, ioDiscard{}); err != nil {
		t.Fatalf("verify real-network history qualification: %v", err)
	}
	controllerRaw, err := os.ReadFile(controllerPath)
	if err != nil {
		t.Fatal(err)
	}
	tamperedControllerPath := filepath.Join(receiptDirectory, "controller-history-tampered.json")
	tamperedController := strings.Replace(string(controllerRaw), `"id": "crossed-a"`, `"id": "tampered-a"`, 1)
	if tamperedController == string(controllerRaw) {
		t.Fatal("controller history tamper setup did not change input")
	}
	if err := os.WriteFile(tamperedControllerPath, []byte(tamperedController), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runAnchorQualification([]string{
		"history-verify", "--public-key", receiptPublicPath, "--receipt", historyReceiptPath, "--history", tamperedControllerPath,
	}, ioDiscard{}, ioDiscard{}); err == nil {
		t.Fatal("history receipt accepted a changed controller log")
	}
	stop(nodes[0])
	nodes[0].cancel = nil
	stop(nodes[1])
	nodes[1].cancel = nil
	stop(nodes[2])
	nodes[2].cancel = nil
}

func newAnchorTestPKI(t *testing.T) anchorTestPKI {
	t.Helper()
	_, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Minute)
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "meldbase-test-ca"}, NotBefore: now, NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(serial int64, client bool) (tls.Certificate, []byte, []byte) {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "meldbase-test"}, NotBefore: now, NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
		if client {
			template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		} else {
			template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
		pair, err := tls.X509KeyPair(certificatePEM, keyPEM)
		if err != nil {
			t.Fatal(err)
		}
		return pair, certificatePEM, keyPEM
	}
	server, serverPEM, keyPEM := issue(2, false)
	client, clientPEM, clientKeyPEM := issue(3, true)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return anchorTestPKI{server: server, client: client, rootPool: pool, serverPEM: serverPEM, keyPEM: keyPEM, clientPEM: clientPEM, clientKeyPEM: clientKeyPEM, caPEM: caPEM}
}

func statusCode(response *http.Response) int {
	if response == nil {
		return 0
	}
	return response.StatusCode
}

type ioDiscard struct{}

func (ioDiscard) Write(value []byte) (int, error) { return len(value), nil }
