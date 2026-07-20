package meldbase

import (
	"testing"
)

func TestOperationalStateDistinguishesWritableFailStopAndClosed(t *testing.T) {
	var nilDB *DB
	if state := nilDB.OperationalState(); state.Readable || state.Writable {
		t.Fatalf("nil state=%+v", state)
	}
	db := New()
	if state := db.OperationalState(); !state.Readable || !state.Writable {
		t.Fatalf("open state=%+v", state)
	}
	db.mu.Lock()
	db.fatalErr = ErrDurability
	db.mu.Unlock()
	if state := db.OperationalState(); !state.Readable || state.Writable {
		t.Fatalf("fail-stop state=%+v", state)
	}
	if allocations := testing.AllocsPerRun(1_000, func() { _ = db.OperationalState() }); allocations != 0 {
		t.Fatalf("operational state allocations=%v", allocations)
	}
	_ = db.Close()
	if state := db.OperationalState(); state.Readable || state.Writable {
		t.Fatalf("closed state=%+v", state)
	}
}

func BenchmarkOperationalState(b *testing.B) {
	db := New()
	defer db.Close()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		_ = db.OperationalState()
	}
}
