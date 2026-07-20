package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// The public schema is an editor/tooling contract, while the Go parser remains
// the runtime authority. Keep their versioned URL in lockstep so an initialized
// manifest never advertises a schema the server would reject.
func TestCollectionAccessManifestSchemaMatchesRuntimeContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "public", "schemas", "collection-access-manifest-v1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		ID         string                                `json:"$id"`
		Properties map[string]map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.ID != CollectionAccessManifestSchemaURL {
		t.Fatalf("schema id=%q, want %q", schema.ID, CollectionAccessManifestSchemaURL)
	}
	var metadataURL string
	if err := json.Unmarshal(schema.Properties["$schema"]["const"], &metadataURL); err != nil {
		t.Fatalf("schema metadata URL: %v", err)
	}
	if metadataURL != CollectionAccessManifestSchemaURL {
		t.Fatalf("schema metadata URL=%q, want %q", metadataURL, CollectionAccessManifestSchemaURL)
	}
}
