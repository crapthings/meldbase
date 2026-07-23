package database

import "testing"

func mustID(t *testing.T) DocumentID {
	t.Helper()
	id, err := NewDocumentID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
