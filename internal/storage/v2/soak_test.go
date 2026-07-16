package v2

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestConfigurableLargeDatabaseSoak is an exploratory storage stress test.
// Release evidence is produced only by the VCS-stamped `meld storage-soak`
// command in internal/qualification; a Go test binary has no trustworthy VCS
// build identity and must never emit a qualification receipt.
func TestConfigurableLargeDatabaseSoak(t *testing.T) {
	documentCount := soakEnvInt(t, "MELDBASE_V2_SOAK_DOCUMENTS", 0)
	if documentCount == 0 {
		t.Skip("set MELDBASE_V2_SOAK_DOCUMENTS to run the large-database soak")
	}
	if documentCount < 100 {
		t.Fatal("MELDBASE_V2_SOAK_DOCUMENTS must be at least 100")
	}
	rounds := soakEnvInt(t, "MELDBASE_V2_SOAK_ROUNDS", 4)
	if rounds < 1 {
		t.Fatal("MELDBASE_V2_SOAK_ROUNDS must be positive")
	}

	path := filepath.Join(t.TempDir(), "large-soak.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, documentCount)
	transactionOrdinal := uint64(1)
	for start := 0; start < documentCount; start += 256 {
		end := min(start+256, documentCount)
		mutations := make([]DocumentMutation, 0, end-start)
		for index := start; index < end; index++ {
			keys[index] = soakKey(index, 0)
			mutations = append(mutations, DocumentMutation{
				Collection: "items", DocumentID: soakDocumentID(index), Operation: DocumentInsert,
				Document: []byte(keys[index]),
			})
		}
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
			TransactionID: soakTransactionID(transactionOrdinal), Mutations: mutations,
		}); err != nil {
			t.Fatalf("initial batch %d: %v", start/256, err)
		}
		transactionOrdinal++
	}
	entries := make([]IndexEntry, documentCount)
	for index := range documentCount {
		entries[index] = IndexEntry{Key: []byte(keys[index]), DocumentID: soakDocumentID(index)}
	}
	if _, err := file.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: soakTransactionID(transactionOrdinal), Collection: "items", Name: "by_key",
		FieldPath: "key", Unique: true, Entries: entries,
	}); err != nil {
		t.Fatal(err)
	}
	transactionOrdinal++

	pinned, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= rounds; round++ {
		stride := max(1, documentCount/1024)
		mutations := make([]DocumentMutation, 0, 1024)
		for index := round % stride; index < documentCount; index += stride {
			before := keys[index]
			after := soakKey(index, round)
			keys[index] = after
			mutations = append(mutations, DocumentMutation{
				Collection: "items", DocumentID: soakDocumentID(index), Operation: DocumentUpdate,
				Document: []byte(after), Indexes: []IndexMutation{{Name: "by_key", BeforeKey: []byte(before), AfterKey: []byte(after)}},
			})
			if len(mutations) == 256 {
				transactionOrdinal = applySoakMutations(t, file, transactionOrdinal, mutations)
				mutations = mutations[:0]
			}
		}
		if len(mutations) > 0 {
			transactionOrdinal = applySoakMutations(t, file, transactionOrdinal, mutations)
		}
		stats, err := file.ReclaimPages()
		if err != nil {
			t.Fatalf("round %d reclaim: %v", round, err)
		}
		if stats.PinnedSnapshots != 1 {
			t.Fatalf("round %d pinned snapshots=%d want=1", round, stats.PinnedSnapshots)
		}
	}
	old, exists, err := pinned.GetDocument("items", soakDocumentID(0))
	if err != nil || !exists || string(old) != soakKey(0, 0) {
		t.Fatalf("pinned snapshot document=%q exists=%t err=%v", old, exists, err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if stats, err := file.ReclaimPages(); err != nil || stats.PinnedSnapshots != 0 {
		t.Fatalf("final reclaim stats=%+v err=%v", stats, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if stats, err := reopened.Reachability(); err != nil || stats.PinnedSnapshots != 0 {
		t.Fatalf("reopen reachability stats=%+v err=%v", stats, err)
	}
	assertSoakContents(t, reopened, keys)
}

func applySoakMutations(t *testing.T, file *File, ordinal uint64, mutations []DocumentMutation) uint64 {
	t.Helper()
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: soakTransactionID(ordinal), Mutations: mutations,
	}); err != nil {
		t.Fatalf("churn transaction %d: %v", ordinal, err)
	}
	return ordinal + 1
}

func assertSoakContents(t *testing.T, file *File, keys []string) {
	t.Helper()
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	documents, err := snapshot.ScanCollection("items", nil, nil, 0)
	if err != nil || len(documents) != len(keys) {
		t.Fatalf("documents=%d want=%d err=%v", len(documents), len(keys), err)
	}
	entries, err := snapshot.ScanIndex("items", "by_key", nil, nil, 0)
	if err != nil || len(entries) != len(keys) {
		t.Fatalf("index entries=%d want=%d err=%v", len(entries), len(keys), err)
	}
	for _, entry := range entries {
		index := int(binary.BigEndian.Uint64(entry.DocumentID[8:]))
		if index < 0 || index >= len(keys) || string(entry.Key) != keys[index] {
			t.Fatalf("invalid index entry id=%x key=%q", entry.DocumentID, entry.Key)
		}
		document, exists, err := snapshot.GetDocument("items", entry.DocumentID)
		if err != nil || !exists || string(document) != keys[index] {
			t.Fatalf("document %d=%q exists=%t want=%q err=%v", index, document, exists, keys[index], err)
		}
	}
}

func soakEnvInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		t.Fatalf("%s must be a non-negative integer", name)
	}
	return parsed
}

func soakDocumentID(index int) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], uint64(index))
	id[0] = 1
	return id
}

func soakTransactionID(ordinal uint64) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], ordinal)
	id[0] = 2
	return id
}

func soakKey(index, revision int) string {
	return fmt.Sprintf("key-%012d-r%06d", index, revision)
}
