package anchorhttp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
	"github.com/crapthings/meldbase/internal/qualification"
)

var testSharedKey = []byte("0123456789abcdef0123456789abcdef")
var testRotatedKey = []byte("abcdef0123456789abcdef0123456789")

const testKeyID = "primary"
const testClusterID = "orders-production"

var testMemberCounter atomic.Uint64

const (
	faultNormal int32 = iota
	faultUnavailable
	faultDelay
	faultDropAdvance
	faultFailFirstAdvance
	faultSlow
	faultPersistThenDropAdvance
)

type faultHandler struct {
	handler                       http.Handler
	mode                          atomic.Int32
	advanceAttempts               atomic.Uint64
	persistedDroppedAdvance       chan<- struct{}
	releaseDroppedAdvanceResponse <-chan struct{}
}

func (handler *faultHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	switch handler.mode.Load() {
	case faultUnavailable:
		http.Error(response, "unavailable", http.StatusServiceUnavailable)
	case faultDelay:
		<-request.Context().Done()
	case faultDropAdvance:
		if request.Method == http.MethodPut {
			http.Error(response, "partitioned", http.StatusServiceUnavailable)
			return
		}
		handler.handler.ServeHTTP(response, request)
	case faultFailFirstAdvance:
		if request.Method == http.MethodPut && handler.advanceAttempts.Add(1) == 1 {
			http.Error(response, "transient", http.StatusServiceUnavailable)
			return
		}
		handler.handler.ServeHTTP(response, request)
	case faultSlow:
		timer := time.NewTimer(20 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-request.Context().Done():
			return
		case <-timer.C:
			handler.handler.ServeHTTP(response, request)
		}
	case faultPersistThenDropAdvance:
		if request.Method != http.MethodPut {
			handler.handler.ServeHTTP(response, request)
			return
		}
		// Complete the trusted storage operation, then close the connection
		// before publishing its response. The client must treat the outcome as
		// ambiguous even though the member may have durably accepted it.
		recorder := httptest.NewRecorder()
		handler.handler.ServeHTTP(recorder, request)
		// Tests that model an ambiguous durable outcome must first observe the
		// durable handler completing. Otherwise QuorumStore may correctly return
		// early and cancel a request which had not reached this node yet.
		if handler.persistedDroppedAdvance != nil {
			handler.persistedDroppedAdvance <- struct{}{}
		}
		if handler.releaseDroppedAdvanceResponse != nil {
			select {
			case <-handler.releaseDroppedAdvanceResponse:
			case <-request.Context().Done():
				return
			}
		}
		hijacker, ok := response.(http.Hijacker)
		if !ok {
			panic("test HTTP server does not support connection hijacking")
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			panic(err)
		}
		_ = connection.Close()
	default:
		handler.handler.ServeHTTP(response, request)
	}
}

type testNode struct {
	directory string
	memberID  string
	fault     *faultHandler
	server    *httptest.Server
}

func newTestNode(t *testing.T, clusterID, memberID string, members []string) testNode {
	t.Helper()
	directory := t.TempDir()
	handler, err := NewHandler(HandlerOptions{Directory: directory, ClusterID: clusterID, Members: members, MemberID: memberID, Keys: map[string][]byte{testKeyID: testSharedKey}})
	if err != nil {
		t.Fatal(err)
	}
	fault := &faultHandler{handler: handler}
	return testNode{directory: directory, memberID: memberID, fault: fault, server: httptest.NewServer(fault)}
}

func newTestNodes(t *testing.T, count int) []testNode {
	t.Helper()
	group := strconv.FormatUint(testMemberCounter.Add(1), 10)
	members := make([]string, count)
	for index := range members {
		members[index] = "group-" + group + "-member-" + strconv.Itoa(index+1)
	}
	nodes := make([]testNode, count)
	for index := range nodes {
		nodes[index] = newTestNode(t, testClusterID, members[index], members)
	}
	return nodes
}

func (node testNode) close() { node.server.Close() }

func testAnchor(sequence, generation uint64) meldbase.RollbackAnchor {
	return meldbase.RollbackAnchor{DatabaseID: [16]byte{1, 2, 3}, MinimumCommitSequence: sequence, MinimumGeneration: generation}
}

func TestSingleHTTPSStoreAuthenticatesAndAdvancesMonotonically(t *testing.T) {
	handler, err := NewHandler(HandlerOptions{Directory: t.TempDir(), ClusterID: testClusterID, Members: []string{"member-single"}, MemberID: "member-single", Keys: map[string][]byte{testKeyID: testSharedKey, "next": testRotatedKey}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	store, err := NewQuorumStore(QuorumOptions{ClusterID: testClusterID, Replicas: []Replica{{Endpoint: server.URL, MemberID: "member-single"}}, AnchorName: "orders", KeyID: testKeyID, SharedKey: testSharedKey, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if anchor, exists, err := store.Load(context.Background()); err != nil || exists || anchor != (meldbase.RollbackAnchor{}) {
		t.Fatalf("empty anchor=%+v exists=%t err=%v", anchor, exists, err)
	}
	first := testAnchor(1, 2)
	if err := store.Advance(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(context.Background(), first); err != nil {
		t.Fatalf("idempotent advance failed: %v", err)
	}
	if retained, exists, err := store.Load(context.Background()); err != nil || !exists || retained != first {
		t.Fatalf("retained=%+v exists=%t err=%v", retained, exists, err)
	}
	rotated, err := NewQuorumStore(QuorumOptions{ClusterID: testClusterID, Replicas: []Replica{{Endpoint: server.URL, MemberID: "member-single"}}, AnchorName: "orders", KeyID: "next", SharedKey: testRotatedKey, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if retained, exists, err := rotated.Load(context.Background()); err != nil || !exists || retained != first {
		t.Fatalf("rotated-key retained=%+v exists=%t err=%v", retained, exists, err)
	}
	if err := store.Advance(context.Background(), testAnchor(0, 1)); !errors.Is(err, ErrConflict) {
		t.Fatalf("regression error=%v", err)
	}
	wrongKey := append([]byte(nil), testSharedKey...)
	wrongKey[0] ^= 0xff
	unauthorized, err := NewQuorumStore(QuorumOptions{ClusterID: testClusterID, Replicas: []Replica{{Endpoint: server.URL, MemberID: "member-single"}}, AnchorName: "orders", KeyID: testKeyID, SharedKey: wrongKey, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if err := unauthorized.Advance(context.Background(), testAnchor(2, 3)); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("authentication error=%v", err)
	}
	if _, _, err := unauthorized.Load(context.Background()); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("load authentication error=%v", err)
	}
	if status := unauthorized.RollbackAnchorStatus(); status.Replicas != 1 || status.Quorum != 1 || status.Loads != 1 || status.Advances != 1 || status.AuthenticationFailures != 2 {
		t.Fatalf("unexpected authentication status: %+v", status)
	}
	unknownKeyID, err := NewQuorumStore(QuorumOptions{ClusterID: testClusterID, Replicas: []Replica{{Endpoint: server.URL, MemberID: "member-single"}}, AnchorName: "orders", KeyID: "unknown", SharedKey: testSharedKey, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := unknownKeyID.Load(context.Background()); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("unknown key ID error=%v", err)
	}
	if _, err := NewQuorumStore(QuorumOptions{ClusterID: testClusterID, Replicas: []Replica{{Endpoint: "http://127.0.0.1:1", MemberID: "member-single"}}, AnchorName: "orders", KeyID: testKeyID, SharedKey: testSharedKey}); !errors.Is(err, ErrInsecureTransport) {
		t.Fatalf("insecure endpoint error=%v", err)
	}
}

func TestHandlerRejectsStaleAndBodyTamperedAuthentication(t *testing.T) {
	handler, err := NewHandler(HandlerOptions{Directory: t.TempDir(), ClusterID: testClusterID, Members: []string{"member-auth"}, MemberID: "member-auth", Keys: map[string][]byte{testKeyID: testSharedKey}, MaxClockSkew: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	path := anchorPathPrefix + "orders"
	body, err := json.Marshal(encodeAnchor(testAnchor(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	staleTimestamp := strconv.FormatInt(time.Now().Add(-time.Minute).UnixMilli(), 10)
	stale := signedTestRequest(t, server.URL+path, path, staleTimestamp, body, body)
	assertHTTPStatus(t, server.Client(), stale, http.StatusUnauthorized)

	currentTimestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	tampered := append([]byte(nil), body...)
	tampered[len(tampered)-2] ^= 1
	request := signedTestRequest(t, server.URL+path, path, currentTimestamp, body, tampered)
	assertHTTPStatus(t, server.Client(), request, http.StatusUnauthorized)

	oversized := signedTestRequest(t, server.URL+path, path, currentTimestamp, body, body)
	oversized.Header.Set("Meldbase-Anchor-Member-ID", strings.Repeat("a", 129))
	assertHTTPStatus(t, server.Client(), oversized, http.StatusUnauthorized)
}

func TestConfigurationRejectsUnsafeMembershipAndPaths(t *testing.T) {
	if _, err := NewHandler(HandlerOptions{Directory: t.TempDir(), ClusterID: testClusterID, Members: []string{"member-config"}, MemberID: "member-config", Keys: map[string][]byte{testKeyID: []byte("short")}}); err == nil {
		t.Fatal("short server key accepted")
	}
	if _, err := NewHandler(HandlerOptions{Directory: filepath.Join(t.TempDir(), "missing"), ClusterID: testClusterID, Members: []string{"member-config"}, MemberID: "member-config", Keys: map[string][]byte{testKeyID: testSharedKey}}); err == nil {
		t.Fatal("missing server directory accepted")
	}
	for _, name := range []string{"", ".", "..", "../escape", "workspace/name", "name?query"} {
		if validAnchorName(name) {
			t.Fatalf("unsafe anchor name %q accepted", name)
		}
	}
	base := QuorumOptions{ClusterID: testClusterID, AnchorName: "orders", KeyID: testKeyID, SharedKey: testSharedKey, AllowInsecureHTTP: true}
	for _, replicas := range [][]Replica{
		{},
		{{Endpoint: "http://a", MemberID: "a"}, {Endpoint: "http://b", MemberID: "b"}},
		{{Endpoint: "http://a", MemberID: "a"}, {Endpoint: "http://a", MemberID: "b"}, {Endpoint: "http://c", MemberID: "c"}},
		{{Endpoint: "http://a", MemberID: "same"}, {Endpoint: "http://b", MemberID: "same"}, {Endpoint: "http://c", MemberID: "c"}},
		{{Endpoint: "http://user:pass@a", MemberID: "a"}},
		{{Endpoint: "http://a/path", MemberID: "a"}},
		{{Endpoint: "http://a?query=1", MemberID: "a"}},
	} {
		options := base
		options.Replicas = replicas
		if _, err := NewQuorumStore(options); err == nil {
			t.Fatalf("unsafe replicas accepted: %v", replicas)
		}
	}
}

func TestNodeDirectoryPermanentlyBindsStaticConfigurationAndMember(t *testing.T) {
	directory := t.TempDir()
	options := HandlerOptions{Directory: directory, ClusterID: testClusterID, Members: []string{"member-a"}, MemberID: "member-a", Keys: map[string][]byte{testKeyID: testSharedKey}}
	if _, err := NewHandler(options); err != nil {
		t.Fatal(err)
	}
	if _, err := NewHandler(options); err != nil {
		t.Fatalf("same binding did not reopen: %v", err)
	}
	changedCluster := options
	changedCluster.ClusterID = "different-cluster"
	if _, err := NewHandler(changedCluster); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("changed cluster binding error=%v", err)
	}
	changedMembers := options
	changedMembers.Members = []string{"member-a", "member-b", "member-c"}
	if _, err := NewHandler(changedMembers); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("changed membership binding error=%v", err)
	}
	changedMember := options
	changedMember.Members = []string{"member-b"}
	changedMember.MemberID = "member-b"
	if _, err := NewHandler(changedMember); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("changed member binding error=%v", err)
	}
	if err := os.WriteFile(filepath.Join(directory, nodeManifestName), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewHandler(options); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("corrupt manifest error=%v", err)
	}

	legacyDirectory := t.TempDir()
	if err := os.WriteFile(filepath.Join(legacyDirectory, "orders.anchor"), []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	legacy := options
	legacy.Directory = legacyDirectory
	if _, err := NewHandler(legacy); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("unbound legacy directory error=%v", err)
	}
}

func TestConfigurationDigestIsOrderIndependentAndMembershipSensitive(t *testing.T) {
	first, err := configurationDigest(testClusterID, []string{"member-c", "member-a", "member-b"}, "member-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := configurationDigest(testClusterID, []string{"member-a", "member-b", "member-c"}, "member-c")
	if err != nil || second != first || len(first) != sha256.Size*2 {
		t.Fatalf("configuration digests first=%q second=%q err=%v", first, second, err)
	}
	changed, err := configurationDigest(testClusterID, []string{"member-a", "member-b", "member-d"}, "")
	if err != nil || changed == first {
		t.Fatalf("membership change digest=%q err=%v", changed, err)
	}
	if _, err := configurationDigest(testClusterID, []string{"member-a", "member-a", "member-c"}, ""); err == nil {
		t.Fatal("duplicate members accepted")
	}
	if _, err := configurationDigest(testClusterID, []string{"member-a", "member-b", "member-c"}, "member-z"); err == nil {
		t.Fatal("server member outside membership accepted")
	}
}

func TestStaticMembershipBindingRejectsAliasesAndWrongCluster(t *testing.T) {
	members := []string{"member-a", "member-b", "member-c"}
	handler, err := NewHandler(HandlerOptions{Directory: t.TempDir(), ClusterID: testClusterID, Members: members, MemberID: "member-a", Keys: map[string][]byte{testKeyID: testSharedKey}})
	if err != nil {
		t.Fatal(err)
	}
	servers := []*httptest.Server{httptest.NewServer(handler), httptest.NewServer(handler), httptest.NewServer(handler)}
	for _, server := range servers {
		defer server.Close()
	}
	replicas := []Replica{
		{Endpoint: servers[0].URL, MemberID: "member-a"},
		{Endpoint: servers[1].URL, MemberID: "member-b"},
		{Endpoint: servers[2].URL, MemberID: "member-c"},
	}
	store, err := NewQuorumStore(QuorumOptions{ClusterID: testClusterID, Replicas: replicas, AnchorName: "orders", KeyID: testKeyID, SharedKey: testSharedKey, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(context.Background(), testAnchor(1, 2)); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("aliased member manufactured quorum: %v", err)
	}
	if status := store.RollbackAnchorStatus(); status.ConfigurationFailures == 0 {
		t.Fatalf("configuration failure was not observed: %+v", status)
	}

	nodes := newTestNodes(t, 3)
	for _, node := range nodes {
		defer node.close()
	}
	wrongClusterReplicas := make([]Replica, len(nodes))
	for index, node := range nodes {
		wrongClusterReplicas[index] = Replica{Endpoint: node.server.URL, MemberID: node.memberID}
	}
	wrongCluster, err := NewQuorumStore(QuorumOptions{ClusterID: "wrong-cluster", Replicas: wrongClusterReplicas, AnchorName: "orders", KeyID: testKeyID, SharedKey: testSharedKey, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := wrongCluster.Load(context.Background()); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("wrong cluster load error=%v", err)
	}
}

func TestReadQuorumMergeRequiresOneComparableHistory(t *testing.T) {
	older := testAnchor(1, 2)
	newer := testAnchor(2, 4)
	selected, exists, err := mergeLoadQuorum([]loadResult{{anchor: older, exists: true}, {}, {anchor: newer, exists: true}})
	if err != nil || !exists || selected != newer {
		t.Fatalf("selected=%+v exists=%t err=%v", selected, exists, err)
	}
	crossed := testAnchor(2, 3)
	older.MinimumGeneration = 4
	if _, _, err := mergeLoadQuorum([]loadResult{{anchor: older, exists: true}, {anchor: crossed, exists: true}}); !errors.Is(err, ErrConflict) {
		t.Fatalf("crossed history error=%v", err)
	}
	dominating := testAnchor(2, 5)
	selected, exists, err = mergeLoadQuorum([]loadResult{{anchor: older, exists: true}, {anchor: crossed, exists: true}, {anchor: dominating, exists: true}})
	if err != nil || !exists || selected != dominating {
		t.Fatalf("dominating history selected=%+v exists=%t err=%v", selected, exists, err)
	}
}

func TestFailedWriteForkFailsClosedAndLaterConverges(t *testing.T) {
	nodes := newTestNodes(t, 3)
	for _, node := range nodes {
		defer node.close()
	}
	store := newTestQuorum(t, nodes)
	initial := testAnchor(0, 1)
	if err := store.Advance(context.Background(), initial); err != nil {
		t.Fatal(err)
	}

	// A logical advance reaches only member 0 and therefore is not
	// acknowledged by the quorum store.
	failedLogical := testAnchor(1, 2)
	failedBody, err := json.Marshal(encodeAnchor(failedLogical))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.advanceOne(context.Background(), store.replicas[0], failedBody); err != nil {
		t.Fatalf("seed minority failed-write: %v", err)
	}
	nodes[1].fault.mode.Store(faultDropAdvance)
	nodes[2].fault.mode.Store(faultDropAdvance)
	if err := store.Advance(context.Background(), failedLogical); !errors.Is(err, ErrQuorum) {
		t.Fatalf("minority failed-write error=%v", err)
	}

	// Recover the last acknowledged database through members 1 and 2, then
	// advance maintenance generations despite member 0 rejecting that branch.
	nodes[1].fault.mode.Store(faultSlow)
	nodes[2].fault.mode.Store(faultSlow)
	for _, branch := range []meldbase.RollbackAnchor{testAnchor(0, 2), testAnchor(0, 3)} {
		if err := store.Advance(context.Background(), branch); err != nil {
			t.Fatalf("advance replacement branch %+v: %v", branch, err)
		}
	}
	if status := store.RollbackAnchorStatus(); status.Conflicts < 2 {
		t.Fatalf("rejecting minority was not observed before quorum success: %+v", status)
	}

	// With the clean branch member delayed, the first two replies are crossed.
	// Load must wait for the third reply and find the clean majority instead of
	// making result order an availability decision.
	nodes[0].fault.mode.Store(faultNormal)
	nodes[1].fault.mode.Store(faultNormal)
	nodes[2].fault.mode.Store(faultSlow)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	branch := testAnchor(0, 3)
	if retained, exists, err := store.Load(ctx); err != nil || !exists || retained != branch {
		t.Fatalf("clean quorum after crossed prefix retained=%+v exists=%t err=%v", retained, exists, err)
	}

	// If the second branch member is unavailable there is no safe quorum and
	// the two completed crossed replies fail closed.
	nodes[2].fault.mode.Store(faultUnavailable)
	if _, _, err := store.Load(context.Background()); !errors.Is(err, ErrConflict) {
		t.Fatalf("no-safe-quorum crossed load error=%v", err)
	}

	// A clean branch quorum remains readable. A later coordinate that advances
	// both dimensions repairs a quorum; Advance returns when that quorum is
	// durable and may cancel a trailing minority request.
	nodes[0].fault.mode.Store(faultUnavailable)
	nodes[2].fault.mode.Store(faultNormal)
	if retained, exists, err := store.Load(context.Background()); err != nil || !exists || retained != branch {
		t.Fatalf("replacement branch retained=%+v exists=%t err=%v", retained, exists, err)
	}
	nodes[0].fault.mode.Store(faultNormal)
	repair := testAnchor(1, 4)
	if err := store.Advance(context.Background(), repair); err != nil {
		t.Fatalf("converging advance: %v", err)
	}
	checks, err := store.CheckReplicas(context.Background())
	if err != nil || len(checks) != 3 {
		t.Fatalf("converged checks=%+v err=%v", checks, err)
	}
	repaired := 0
	for _, check := range checks {
		if check.State != ReplicaAvailable || !check.Exists || !anchorBeforeOrEqual(check.Anchor, repair) {
			t.Fatalf("member did not retain a compatible repair history: %+v", check)
		}
		if check.Anchor == repair {
			repaired++
		}
	}
	if repaired < 2 {
		t.Fatalf("repair did not reach a quorum: checks=%+v", checks)
	}
}

func TestRejectingMajorityCannotBeOverriddenByOneAcceptingMember(t *testing.T) {
	nodes := newTestNodes(t, 3)
	for _, node := range nodes {
		defer node.close()
	}
	store := newTestQuorum(t, nodes)
	initial := testAnchor(0, 1)
	if err := store.Advance(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	failedLogical := testAnchor(1, 2)
	body, err := json.Marshal(encodeAnchor(failedLogical))
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		if err := store.advanceOne(context.Background(), store.replicas[index], body); err != nil {
			t.Fatalf("seed rejecting member %d: %v", index, err)
		}
	}
	if err := store.Advance(context.Background(), testAnchor(0, 3)); !errors.Is(err, ErrConflict) {
		t.Fatalf("rejecting majority error=%v", err)
	}
	status := store.RollbackAnchorStatus()
	if status.Conflicts < 2 || status.QuorumFailures == 0 {
		t.Fatalf("rejecting majority metrics=%+v", status)
	}
}

func TestConcurrentWritersAreLinearizableAtStaticQuorum(t *testing.T) {
	nodes := newTestNodes(t, 3)
	for _, node := range nodes {
		defer node.close()
	}
	for iteration := 0; iteration < 50; iteration++ {
		crossedStore := newTestQuorumNamed(t, nodes, fmt.Sprintf("concurrent-crossed-%d", iteration))
		crossed := [2]meldbase.RollbackAnchor{testAnchor(1, 2), testAnchor(0, 3)}
		errorsByWriter, history, nextEvent := concurrentAdvances(crossedStore, crossed)
		winner := -1
		for writer, err := range errorsByWriter {
			if err == nil {
				if winner != -1 {
					t.Fatalf("iteration %d crossed writers both succeeded", iteration)
				}
				winner = writer
			} else if !errors.Is(err, ErrConflict) {
				t.Fatalf("iteration %d crossed writer %d error=%v", iteration, writer, err)
			}
		}
		if winner == -1 {
			t.Fatalf("iteration %d crossed writers elected no winner: %v", iteration, errorsByWriter)
		}
		retained, exists, err := crossedStore.Load(context.Background())
		if err != nil || !exists || retained != crossed[winner] {
			t.Fatalf("iteration %d crossed retained=%+v exists=%t winner=%+v err=%v", iteration, retained, exists, crossed[winner], err)
		}
		assertAnchorHistoryLinearizable(t, history, nextEvent, retained)

		comparableStore := newTestQuorumNamed(t, nodes, fmt.Sprintf("concurrent-comparable-%d", iteration))
		comparable := [2]meldbase.RollbackAnchor{testAnchor(1, 2), testAnchor(2, 3)}
		errorsByWriter, history, nextEvent = concurrentAdvances(comparableStore, comparable)
		if errorsByWriter[1] != nil || (errorsByWriter[0] != nil && !errors.Is(errorsByWriter[0], ErrConflict)) {
			t.Fatalf("iteration %d comparable writer errors=%v", iteration, errorsByWriter)
		}
		retained, exists, err = comparableStore.Load(context.Background())
		if err != nil || !exists || retained != comparable[1] {
			t.Fatalf("iteration %d comparable retained=%+v exists=%t err=%v", iteration, retained, exists, err)
		}
		assertAnchorHistoryLinearizable(t, history, nextEvent, retained)
	}
}

func concurrentAdvances(store *QuorumStore, anchors [2]meldbase.RollbackAnchor) ([2]error, []qualification.AnchorHistoryOperation, uint64) {
	start := make(chan struct{})
	execute := make(chan struct{})
	ready := make(chan struct{}, len(anchors))
	var events atomic.Uint64
	results := make(chan struct {
		writer    int
		err       error
		operation qualification.AnchorHistoryOperation
	}, 2)
	for writer := range anchors {
		go func(writer int) {
			<-start
			invoke := events.Add(1)
			ready <- struct{}{}
			<-execute
			err := store.Advance(context.Background(), anchors[writer])
			outcome := qualification.AnchorHistorySucceeded
			if err != nil {
				outcome = qualification.AnchorHistoryFailed
			}
			results <- struct {
				writer    int
				err       error
				operation qualification.AnchorHistoryOperation
			}{writer: writer, err: err, operation: qualification.AnchorHistoryOperation{
				ID: fmt.Sprintf("advance-%d", writer), Kind: qualification.AnchorHistoryAdvance, Outcome: outcome,
				Invoke: invoke, Return: events.Add(1), Value: qualificationAnchorValue(anchors[writer]),
			}}
		}(writer)
	}
	close(start)
	for range anchors {
		<-ready
	}
	close(execute)
	var errorsByWriter [2]error
	operations := make([]qualification.AnchorHistoryOperation, 0, len(anchors)+1)
	for range anchors {
		result := <-results
		errorsByWriter[result.writer] = result.err
		operations = append(operations, result.operation)
	}
	return errorsByWriter, operations, events.Load()
}

func assertAnchorHistoryLinearizable(t *testing.T, operations []qualification.AnchorHistoryOperation, nextEvent uint64, retained meldbase.RollbackAnchor) {
	t.Helper()
	operations = append(operations, qualification.AnchorHistoryOperation{
		ID: "final-load", Kind: qualification.AnchorHistoryLoad, Outcome: qualification.AnchorHistorySucceeded,
		Invoke: nextEvent + 1, Return: nextEvent + 2, Value: qualificationAnchorValue(retained),
	})
	check, err := qualification.CheckAnchorHistory(qualification.AnchorHistory{Operations: operations})
	if err != nil || !check.Linearizable {
		t.Fatalf("non-linearizable real HTTP history operations=%+v check=%+v err=%v", operations, check, err)
	}
}

func qualificationAnchorValue(anchor meldbase.RollbackAnchor) qualification.AnchorHistoryValue {
	return qualification.AnchorHistoryValue{
		Exists: true, DatabaseID: anchor.DatabaseID,
		CommitSequence: anchor.MinimumCommitSequence, Generation: anchor.MinimumGeneration,
	}
}

func TestQuorumAdvanceOutcomeIsAmbiguousWhenDurableResponsesAreLost(t *testing.T) {
	nodes := newTestNodes(t, 3)
	for _, node := range nodes {
		defer node.close()
	}
	store := newTestQuorumNamed(t, nodes, "ambiguous-response-loss")
	persisted := make(chan struct{}, 2)
	releaseResponses := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseResponses) }) }
	defer release()
	for index := range 2 {
		nodes[index].fault.persistedDroppedAdvance = persisted
		nodes[index].fault.releaseDroppedAdvanceResponse = releaseResponses
	}
	nodes[0].fault.mode.Store(faultPersistThenDropAdvance)
	nodes[1].fault.mode.Store(faultPersistThenDropAdvance)
	nodes[2].fault.mode.Store(faultUnavailable)
	target := testAnchor(1, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	advance := make(chan error, 1)
	go func() { advance <- store.Advance(ctx, target) }()
	for range 2 {
		select {
		case <-persisted:
		case <-ctx.Done():
			t.Fatalf("durable response-loss setup timed out: %v", ctx.Err())
		}
	}
	release()
	if err := <-advance; !errors.Is(err, ErrQuorum) {
		t.Fatalf("lost durable responses error=%v", err)
	}
	for index := range nodes {
		nodes[index].fault.mode.Store(faultNormal)
	}
	if retained, exists, err := store.Load(context.Background()); err != nil || !exists || retained != target {
		t.Fatalf("ambiguous durable outcome retained=%+v exists=%t err=%v", retained, exists, err)
	}
	checks, err := store.CheckReplicas(context.Background())
	if err != nil || len(checks) != 3 || !checks[0].Exists || checks[0].Anchor != target || !checks[1].Exists || checks[1].Anchor != target || checks[2].Exists {
		t.Fatalf("ambiguous durable member state=%+v err=%v", checks, err)
	}
}

func TestDatabaseRecoversCommitAfterQuorumResponsesAreLost(t *testing.T) {
	nodes := newTestNodes(t, 3)
	for _, node := range nodes {
		defer node.close()
	}
	store := newTestQuorumNamed(t, nodes, "ambiguous-database-commit")
	path := filepath.Join(t.TempDir(), "ambiguous-response-loss.meld")
	db, err := meldbase.OpenWithOptions(path, meldbase.OpenOptions{RollbackProtection: meldbase.RollbackProtection{
		AnchorStore: store, InitializeAnchor: true, OperationTimeout: time.Second,
	}})
	if err != nil {
		t.Fatal(err)
	}
	nodes[0].fault.mode.Store(faultPersistThenDropAdvance)
	nodes[1].fault.mode.Store(faultPersistThenDropAdvance)
	nodes[2].fault.mode.Store(faultUnavailable)
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.String("durable-but-unacknowledged")}); !errors.Is(err, meldbase.ErrDurability) {
		t.Fatalf("ambiguous database commit error=%v", err)
	}
	if !db.Stats().WritesDisabled || db.Stats().Storage.RollbackAnchorFailures == 0 {
		t.Fatalf("ambiguous database commit did not fail-stop: %+v", db.Stats())
	}
	if err := db.Close(); !errors.Is(err, meldbase.ErrDurability) {
		t.Fatalf("ambiguous database close error=%v", err)
	}
	for index := range nodes {
		nodes[index].fault.mode.Store(faultNormal)
	}

	reopened, err := meldbase.OpenWithOptions(path, meldbase.OpenOptions{RollbackProtection: meldbase.RollbackProtection{
		AnchorStore: store, OperationTimeout: time.Second,
	}})
	if err != nil {
		t.Fatal(err)
	}
	document, err := reopened.Collection("items").FindOne(context.Background(), meldbase.Filter{})
	value, valueOK := document["value"].StringValue()
	if err != nil || !valueOK || value != "durable-but-unacknowledged" {
		_ = reopened.Close()
		t.Fatalf("recovered ambiguous document=%v err=%v", document, err)
	}
	if _, err := reopened.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.String("converge")}); err != nil {
		_ = reopened.Close()
		t.Fatalf("post-recovery convergence commit: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	checks, err := store.CheckReplicas(context.Background())
	if err != nil || len(checks) != 3 {
		t.Fatalf("post-recovery checks=%+v err=%v", checks, err)
	}
	converged := 0
	for _, check := range checks {
		if check.State == ReplicaAvailable && check.Exists && check.Anchor.MinimumCommitSequence == 2 && check.Anchor.MinimumGeneration == 3 {
			converged++
			continue
		}
		// Advance returns after a durable majority and cancels outstanding
		// requests. A healthy member may therefore still expose the prior
		// anchor immediately after the commit; it must never expose a future or
		// different database anchor.
		if check.State != ReplicaAvailable || !check.Exists || check.Anchor.DatabaseID != reopened.DatabaseIdentity() ||
			check.Anchor.MinimumCommitSequence > 2 || check.Anchor.MinimumGeneration > 3 {
			t.Fatalf("post-recovery member has an invalid anchor: %+v", check)
		}
	}
	if converged < 2 {
		t.Fatalf("post-recovery quorum did not converge: checks=%+v", checks)
	}
}

func TestCheckReplicasWaitsForAndClassifiesEveryStaticMember(t *testing.T) {
	nodes := newTestNodes(t, 3)
	for _, node := range nodes {
		defer node.close()
	}
	store := newTestQuorum(t, nodes)
	if store.ConfigurationID() == "" {
		t.Fatal("static configuration digest is empty")
	}
	checks, err := store.CheckReplicas(context.Background())
	if err != nil || len(checks) != 3 {
		t.Fatalf("empty checks=%+v err=%v", checks, err)
	}
	for _, check := range checks {
		if check.State != ReplicaMissing || check.Exists || check.Anchor != (meldbase.RollbackAnchor{}) {
			t.Fatalf("unexpected empty member check: %+v", check)
		}
	}
	anchor := testAnchor(1, 2)
	if err := store.Advance(context.Background(), anchor); err != nil {
		t.Fatal(err)
	}
	nodes[1].fault.mode.Store(faultUnavailable)
	nodes[2].fault.mode.Store(faultDelay)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	checks, err = store.CheckReplicas(ctx)
	if err != nil || len(checks) != 3 || checks[0].State != ReplicaAvailable || !checks[0].Exists || checks[0].Anchor != anchor ||
		checks[1].State != ReplicaUnavailable || checks[2].State != ReplicaUnavailable {
		t.Fatalf("classified checks=%+v err=%v", checks, err)
	}
}

func TestQuorumToleratesStaleNodePartitionAndAllowsLaterRejoin(t *testing.T) {
	nodes := newTestNodes(t, 3)
	for _, node := range nodes {
		defer node.close()
	}
	store := newTestQuorum(t, nodes)
	first := testAnchor(1, 2)
	second := testAnchor(2, 3)
	third := testAnchor(3, 4)
	if err := store.Advance(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	nodes[2].fault.mode.Store(faultDropAdvance)
	nodes[1].fault.mode.Store(faultFailFirstAdvance)
	if err := store.Advance(context.Background(), second); err != nil {
		t.Fatalf("stale node and transient peer failure broke write quorum: %v", err)
	}
	if attempts := nodes[1].fault.advanceAttempts.Load(); attempts != 2 {
		t.Fatalf("transient peer attempts=%d, want 2", attempts)
	}
	if err := store.Advance(context.Background(), second); err != nil {
		t.Fatalf("duplicate quorum advance failed: %v", err)
	}
	if retained, exists, err := store.Load(context.Background()); err != nil || !exists || retained != second {
		t.Fatalf("stale-node load=%+v exists=%t err=%v", retained, exists, err)
	}
	nodes[2].fault.mode.Store(faultDelay)
	started := time.Now()
	if retained, exists, err := store.Load(context.Background()); err != nil || !exists || retained != second || time.Since(started) > time.Second {
		t.Fatalf("partition load=%+v exists=%t err=%v duration=%s", retained, exists, err, time.Since(started))
	}
	nodes[2].fault.mode.Store(faultNormal)
	nodes[0].fault.mode.Store(faultUnavailable)
	if err := store.Advance(context.Background(), third); err != nil {
		t.Fatal(err)
	}
	for index, node := range nodes[1:] {
		retained := loadNodeAnchor(t, node, "orders")
		if retained != third {
			t.Fatalf("rejoined quorum node %d retained=%+v", index+1, retained)
		}
	}
}

func TestQuorumFailsClosedOnNoMajorityConflictAndDeadline(t *testing.T) {
	nodes := newTestNodes(t, 3)
	for _, node := range nodes {
		defer node.close()
	}
	store := newTestQuorum(t, nodes)
	first := testAnchor(1, 2)
	if err := store.Advance(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	nodes[1].fault.mode.Store(faultUnavailable)
	nodes[2].fault.mode.Store(faultUnavailable)
	if err := store.Advance(context.Background(), testAnchor(2, 3)); !errors.Is(err, ErrQuorum) {
		t.Fatalf("minority write error=%v", err)
	}
	nodes[1].fault.mode.Store(faultDelay)
	nodes[2].fault.mode.Store(faultDelay)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, _, err := store.Load(ctx); !errors.Is(err, ErrQuorum) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline load error=%v", err)
	}

	nodes[1].fault.mode.Store(faultNormal)
	nodes[2].fault.mode.Store(faultUnavailable)
	conflict := first
	conflict.DatabaseID[0]++
	advanceNodeAnchor(t, nodes[1], "orders-conflict", conflict)
	advanceNodeAnchor(t, nodes[0], "orders-conflict", first)
	conflictStore, err := NewQuorumStore(QuorumOptions{
		ClusterID: testClusterID,
		Replicas:  []Replica{{Endpoint: nodes[0].server.URL, MemberID: nodes[0].memberID}, {Endpoint: nodes[1].server.URL, MemberID: nodes[1].memberID}, {Endpoint: nodes[2].server.URL, MemberID: nodes[2].memberID}}, AnchorName: "orders-conflict",
		KeyID: testKeyID, SharedKey: testSharedKey, AllowInsecureHTTP: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := conflictStore.Load(context.Background()); !errors.Is(err, ErrConflict) {
		t.Fatalf("identity conflict error=%v", err)
	}
	status := store.RollbackAnchorStatus()
	if status.Replicas != 3 || status.Quorum != 2 || status.Loads == 0 || status.Advances < 2 || status.EndpointFailures < 2 || status.QuorumFailures < 2 {
		t.Fatalf("unexpected failed-quorum status: %+v", status)
	}
	if status := conflictStore.RollbackAnchorStatus(); status.Conflicts != 1 {
		t.Fatalf("unexpected conflict status: %+v", status)
	}
}

func TestQuorumAnchorRejectsRolledBackDatabase(t *testing.T) {
	nodes := newTestNodes(t, 3)
	for _, node := range nodes {
		defer node.close()
	}
	store := newTestQuorum(t, nodes)
	path := filepath.Join(t.TempDir(), "quorum-protected.meld")
	db, err := meldbase.OpenWithOptions(path, meldbase.OpenOptions{RollbackProtection: meldbase.RollbackProtection{AnchorStore: store, InitializeAnchor: true, OperationTimeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	if status := db.Stats().Storage.RollbackAnchorStore; status.Replicas != 3 || status.Quorum != 2 {
		t.Fatalf("database did not expose quorum anchor status: %+v", status)
	}
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), meldbase.Document{"value": meldbase.String("one")}); err != nil {
		t.Fatal(err)
	}
	stale, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	nodes[2].fault.mode.Store(faultDropAdvance)
	if _, err := items.InsertOne(context.Background(), meldbase.Document{"value": meldbase.String("two")}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, stale, 0o600); err != nil {
		t.Fatal(err)
	}
	opened, err := meldbase.OpenWithOptions(path, meldbase.OpenOptions{RollbackProtection: meldbase.RollbackProtection{AnchorStore: store, OperationTimeout: time.Second}})
	if !errors.Is(err, meldbase.ErrRollbackDetected) || opened != nil {
		t.Fatalf("rolled-back database opened=%v err=%v", opened, err)
	}
}

func newTestQuorum(t *testing.T, nodes []testNode) *QuorumStore {
	return newTestQuorumNamed(t, nodes, "orders")
}

func newTestQuorumNamed(t *testing.T, nodes []testNode, anchorName string) *QuorumStore {
	t.Helper()
	replicas := make([]Replica, len(nodes))
	for index := range nodes {
		replicas[index] = Replica{Endpoint: nodes[index].server.URL, MemberID: nodes[index].memberID}
	}
	store, err := NewQuorumStore(QuorumOptions{ClusterID: testClusterID, Replicas: replicas, AnchorName: anchorName, KeyID: testKeyID, SharedKey: testSharedKey, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func loadNodeAnchor(t *testing.T, node testNode, name string) meldbase.RollbackAnchor {
	t.Helper()
	store, err := meldbase.NewFileRollbackAnchorStore(filepath.Join(node.directory, name+".anchor"))
	if err != nil {
		t.Fatal(err)
	}
	anchor, exists, err := store.Load(context.Background())
	if err != nil || !exists {
		t.Fatalf("node anchor exists=%t err=%v", exists, err)
	}
	return anchor
}

func advanceNodeAnchor(t *testing.T, node testNode, name string, anchor meldbase.RollbackAnchor) {
	t.Helper()
	store, err := meldbase.NewFileRollbackAnchorStore(filepath.Join(node.directory, name+".anchor"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(context.Background(), anchor); err != nil {
		t.Fatal(err)
	}
}

func signedTestRequest(t *testing.T, target, path, timestamp string, signedBody, sentBody []byte) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodPut, target, bytes.NewReader(sentBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Meldbase-Anchor-Timestamp", timestamp)
	configurationID, err := configurationDigest(testClusterID, []string{"member-auth"}, "")
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Meldbase-Anchor-Configuration-ID", configurationID)
	request.Header.Set("Meldbase-Anchor-Member-ID", "member-auth")
	request.Header.Set("Meldbase-Anchor-Key-ID", testKeyID)
	request.Header.Set("Meldbase-Anchor-Signature", hex.EncodeToString(requestSignature(testSharedKey, http.MethodPut, path, configurationID, "member-auth", testKeyID, timestamp, signedBody)))
	return request
}

func assertHTTPStatus(t *testing.T, client *http.Client, request *http.Request, status int) {
	t.Helper()
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != status {
		t.Fatalf("status=%d want=%d", response.StatusCode, status)
	}
}
