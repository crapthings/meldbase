package meldbase

// OperationalState is a minimal, allocation-free serving-state snapshot. A
// fail-stop durability error preserves reads from the last committed state but
// disables writes; a closed database is neither readable nor writable.
type OperationalState struct {
	Readable bool `json:"readable"`
	Writable bool `json:"writable"`
}

func (db *DB) OperationalState() OperationalState {
	if db == nil {
		return OperationalState{}
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return OperationalState{}
	}
	return OperationalState{Readable: true, Writable: db.fatalErr == nil}
}
