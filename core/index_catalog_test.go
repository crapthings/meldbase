package meldbase

import (
	"context"
	"path/filepath"
	"testing"
)

func TestIndexCatalogIsCanonicalAndDoesNotShareDefinitions(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Collection("tasks").CreateIndex(context.Background(), "by_state_created", []IndexField{{Field: "state", Order: 1}, {Field: "createdAt", Order: -1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Collection("users").CreateIndex(context.Background(), "by_email", []IndexField{{Field: "email", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	catalog, err := db.IndexCatalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog) != 2 || catalog[0].Collection != "tasks" || catalog[0].Definition.Name != "by_state_created" || catalog[1].Collection != "users" || !catalog[1].Definition.Unique {
		t.Fatalf("catalog=%+v", catalog)
	}
	catalog[0].Definition.Fields[0].Field = "mutated"
	fresh, err := db.IndexCatalog(context.Background())
	if err != nil || fresh[0].Definition.Fields[0].Field != "state" {
		t.Fatalf("catalog copy=%+v err=%v", fresh, err)
	}
}

func TestIndexCatalogSurvivesDurableReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Collection("tasks").CreateIndex(context.Background(), "by_state", []IndexField{{Field: "state", Order: 1}}, IndexOptions{}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	catalog, err := reopened.IndexCatalog(context.Background())
	if err != nil || len(catalog) != 1 || catalog[0].Collection != "tasks" || catalog[0].Definition.Name != "by_state" {
		t.Fatalf("durable catalog=%+v err=%v", catalog, err)
	}
}
