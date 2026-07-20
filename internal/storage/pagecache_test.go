package storage

import "testing"

func TestPageCacheIsBoundedAndReportsHitsMisses(t *testing.T) {
	cache := newPageCache(16)
	for pageID := uint64(2); pageID < 102; pageID++ {
		raw, err := EncodePage(Page{Type: PagePrimaryLeaf, ID: pageID, Generation: 1, BornSequence: 1, Payload: make([]byte, 32)})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodePageView(raw, pageID)
		if err != nil {
			t.Fatal(err)
		}
		cache.put(pageID, raw, decoded)
	}
	if _, ok := cache.get(101); !ok {
		t.Fatal("newest page was not cached")
	}
	if _, ok := cache.get(2); ok {
		t.Fatal("oldest page was not evicted")
	}
	stats := cache.stats()
	if stats.CapacityPages != 16 || stats.ResidentPages != 16 || stats.Evictions != 84 || stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("cache stats = %+v", stats)
	}
}

func TestFilePageCacheWarmsPointReads(t *testing.T) {
	file, _, err := Open(t.TempDir() + "/cache.meld2")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("value")}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := file.GetDocument("items", id); err != nil {
		t.Fatal(err)
	}
	cold := file.PageCacheStats()
	if _, _, err := file.GetDocument("items", id); err != nil {
		t.Fatal(err)
	}
	warm := file.PageCacheStats()
	if cold.Misses == 0 || warm.Hits <= cold.Hits || warm.ResidentPages == 0 || warm.ResidentPages > warm.CapacityPages {
		t.Fatalf("cold=%+v warm=%+v", cold, warm)
	}
}
