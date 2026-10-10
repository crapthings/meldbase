package database

// OperationalState records whether the database accepts reads and writes.
//
// A durability failure disables writes but preserves reads of committed data;
// closing the database disables both reads and writes.
type OperationalState struct {
	Readable bool `json:"readable"`
	Writable bool `json:"writable"`
}

// OperationalState reports whether the database currently accepts reads and writes.
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
