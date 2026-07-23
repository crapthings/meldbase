package database

import "testing"

func TestResumeWindowExpiresOnlyPositionsOlderThanRetainedHistory(t *testing.T) {
	db := New()
	defer db.Close()
	db.mu.Lock()
	for token := uint64(1); token <= 1025; token++ {
		db.token = token
		db.recordCommittedBatch(ChangeBatch{Token: token, Changes: []Change{{Collection: "items", Operation: DeleteOperation}}})
	}
	db.mu.Unlock()
	if db.CanResumeFrom(0) {
		t.Fatal("expired positions were accepted")
	}
	if !db.CanResumeFrom(1) || !db.CanResumeFrom(2) || !db.CanResumeFrom(1025) {
		t.Fatal("retained positions were rejected")
	}
	if db.CanResumeFrom(1026) {
		t.Fatal("future position was accepted")
	}
}
