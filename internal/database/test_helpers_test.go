package database

import "testing"

// mustID creates a document identifier and fails the test if generation fails.
func mustID(t *testing.T) DocumentID {
	t.Helper()
	id, err := NewDocumentID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
