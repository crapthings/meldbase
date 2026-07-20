package storage

import (
	"bytes"
	"crypto/sha256"
	"path/filepath"
	"testing"
)

func TestSystemRecordsCompareAndSwapPersistOutsideUserCatalog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "system.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("rpc-idempotency-key")
	first := []byte("pending")
	result, err := file.ApplySystemRecordTransaction(SystemRecordTransaction{
		TransactionID: [16]byte{1}, Key: key, NewValue: first,
	})
	if err != nil || !result.Applied || result.Sequence != 1 {
		t.Fatalf("first CAS=%+v err=%v", result, err)
	}
	if got, exists, err := file.GetSystemRecord(key); err != nil || !exists || !bytes.Equal(got, first) {
		t.Fatalf("first record=%q/%t err=%v", got, exists, err)
	}

	wrong := sha256.Sum256([]byte("wrong"))
	result, err = file.ApplySystemRecordTransaction(SystemRecordTransaction{
		TransactionID: [16]byte{2}, Key: key, ExpectedExists: true, ExpectedHash: wrong, NewValue: []byte("must-not-publish"),
	})
	if err != nil || result.Applied || result.Sequence != 0 || !bytes.Equal(result.Current, first) || file.Meta().CommitSequence != 1 {
		t.Fatalf("mismatched CAS=%+v sequence=%d err=%v", result, file.Meta().CommitSequence, err)
	}

	firstHash := sha256.Sum256(first)
	second := bytes.Repeat([]byte{0x5a}, inlineSystemValueLimit+PageSize)
	result, err = file.ApplySystemRecordTransaction(SystemRecordTransaction{
		TransactionID: [16]byte{3}, Key: key, ExpectedExists: true, ExpectedHash: firstHash, NewValue: second,
	})
	if err != nil || !result.Applied || result.Sequence != 2 {
		t.Fatalf("large CAS=%+v err=%v", result, err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{4}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: [16]byte{1}, Operation: DocumentInsert, Document: []byte("document"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if got, exists, err := file.GetSystemRecord(key); err != nil || !exists || !bytes.Equal(got, second) {
		t.Fatalf("record after document commit=%d/%t err=%v", len(got), exists, err)
	}
	if stats, err := file.Reachability(); err != nil || stats.ReachablePages == 0 {
		t.Fatalf("reachability=%+v err=%v", stats, err)
	}
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	collections, err := snapshot.Collections()
	_ = snapshot.Close()
	if err != nil || len(collections) != 1 || collections[0].Name != "items" {
		t.Fatalf("public collections=%+v err=%v", collections, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, exists, err := reopened.GetSystemRecord(key); err != nil || !exists || !bytes.Equal(got, second) {
		t.Fatalf("reopened record=%d/%t err=%v", len(got), exists, err)
	}
	secondHash := sha256.Sum256(second)
	result, err = reopened.ApplySystemRecordTransaction(SystemRecordTransaction{
		TransactionID: [16]byte{5}, Key: key, ExpectedExists: true, ExpectedHash: secondHash, Delete: true,
	})
	if err != nil || !result.Applied || result.Sequence != 4 {
		t.Fatalf("delete CAS=%+v err=%v", result, err)
	}
	if got, exists, err := reopened.GetSystemRecord(key); err != nil || exists || got != nil {
		t.Fatalf("deleted record=%q/%t err=%v", got, exists, err)
	}
}

func TestDocumentAndSystemRecordPublishInOneAtomicGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "document-system.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	key, pending, terminal := []byte("rpc-key"), []byte("pending"), []byte("terminal")
	policyKey, generation := []byte("policy-generation"), []byte("generation-1")
	if result, err := file.ApplySystemRecordTransaction(SystemRecordTransaction{
		TransactionID: [16]byte{1}, Key: key, NewValue: pending,
	}); err != nil || !result.Applied {
		t.Fatalf("seed=%+v err=%v", result, err)
	}
	id := [16]byte{15: 1}
	result, err := file.ApplyDocumentSystemTransaction(DocumentSystemTransaction{
		DocumentTransaction: DocumentTransaction{TransactionID: [16]byte{2}, Mutations: []DocumentMutation{{
			Collection: "orders", DocumentID: id, Operation: DocumentInsert, Document: []byte("created"),
		}}},
		SystemRecords: []SystemRecordMutation{
			{Key: key, ExpectedExists: true, ExpectedHash: sha256.Sum256(pending), NewValue: terminal},
			{Key: policyKey, NewValue: generation, Unconditional: true},
		},
	})
	if err != nil || !result.Applied || result.Sequence != 2 {
		t.Fatalf("composite=%+v err=%v", result, err)
	}
	if got, exists, err := file.GetDocument("orders", id); err != nil || !exists || string(got) != "created" {
		t.Fatalf("document=%q/%t err=%v", got, exists, err)
	}
	if got, exists, err := file.GetSystemRecord(key); err != nil || !exists || !bytes.Equal(got, terminal) {
		t.Fatalf("terminal=%q/%t err=%v", got, exists, err)
	}
	if got, exists, err := file.GetSystemRecord(policyKey); err != nil || !exists || !bytes.Equal(got, generation) {
		t.Fatalf("policy generation=%q/%t err=%v", got, exists, err)
	}
	root, err := file.DatabaseRoot()
	if err != nil {
		t.Fatal(err)
	}
	batch, err := file.ReadCommit(root.CommitLogRoot, 2)
	if err != nil || len(batch.Changes) != 2 || batch.Changes[0].Operation != CommitCatalog || batch.Changes[1].Operation != CommitInsert {
		t.Fatalf("business commit=%+v err=%v", batch, err)
	}

	otherID := [16]byte{15: 2}
	mismatch, err := file.ApplyDocumentSystemTransaction(DocumentSystemTransaction{
		DocumentTransaction: DocumentTransaction{TransactionID: [16]byte{3}, Mutations: []DocumentMutation{{
			Collection: "orders", DocumentID: otherID, Operation: DocumentInsert, Document: []byte("must-not-exist"),
		}}},
		SystemRecords: []SystemRecordMutation{
			{Key: key, ExpectedExists: true, ExpectedHash: sha256.Sum256(terminal), NewValue: []byte("terminal-2")},
			{Key: policyKey, ExpectedExists: true, ExpectedHash: sha256.Sum256([]byte("wrong")), NewValue: []byte("generation-2")},
		},
	})
	if err != nil || mismatch.Applied || mismatch.Sequence != 0 || !bytes.Equal(mismatch.Current, generation) || file.Meta().CommitSequence != 2 {
		t.Fatalf("mismatch=%+v sequence=%d err=%v", mismatch, file.Meta().CommitSequence, err)
	}
	if got, exists, err := file.GetDocument("orders", otherID); err != nil || exists || got != nil {
		t.Fatalf("mismatched document=%q/%t err=%v", got, exists, err)
	}
	if got, exists, err := file.GetSystemRecord(key); err != nil || !exists || !bytes.Equal(got, terminal) {
		t.Fatalf("earlier staged system mutation leaked=%q/%t err=%v", got, exists, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, exists, err := reopened.GetDocument("orders", id); err != nil || !exists || string(got) != "created" {
		t.Fatalf("reopened document=%q/%t err=%v", got, exists, err)
	}
	if got, exists, err := reopened.GetSystemRecord(key); err != nil || !exists || !bytes.Equal(got, terminal) {
		t.Fatalf("reopened terminal=%q/%t err=%v", got, exists, err)
	}
}

func TestSystemRecordValidationRejectsAmbiguousMutations(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "invalid-system.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for _, transaction := range []SystemRecordTransaction{
		{},
		{TransactionID: [16]byte{1}, Key: []byte("key")},
		{TransactionID: [16]byte{1}, Key: []byte("key"), Delete: true, NewValue: []byte("value")},
		{TransactionID: [16]byte{1}, Key: bytes.Repeat([]byte("k"), maxSystemKeyBytes+1), NewValue: []byte("value")},
		{TransactionID: [16]byte{1}, Key: []byte("key"), NewValue: bytes.Repeat([]byte("v"), maxSystemValueBytes+1)},
	} {
		if _, err := file.ApplySystemRecordTransaction(transaction); err == nil {
			t.Fatalf("accepted invalid system transaction: %+v", transaction)
		}
	}
}
