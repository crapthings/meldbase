package database

import "sync"

// ReplicationSourceLease reserves a replica consumer for one session in this process.
//
// It prevents sessions from racing the same checkpoint and does not replace
// external primary election or follower-promotion fencing.
type ReplicationSourceLease struct {
	db   *DB
	name string
	once sync.Once
}

// AcquireReplicationSourceLease reserves a consumer name until the lease is released.
//
// Callers must authenticate and derive the name outside untrusted replication
// frames; primary election remains an external responsibility.
func (db *DB) AcquireReplicationSourceLease(name string) (*ReplicationSourceLease, error) {
	if !validPublicDurableConsumerName(name) {
		return nil, ErrInvalidDocument
	}
	if db == nil {
		return nil, ErrClosed
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	if db.replicationSourceLeases == nil {
		db.replicationSourceLeases = make(map[string]struct{})
	}
	if _, exists := db.replicationSourceLeases[name]; exists {
		return nil, ErrReplicaSourceActive
	}
	db.replicationSourceLeases[name] = struct{}{}
	return &ReplicationSourceLease{db: db, name: name}, nil
}

// Release frees the reserved consumer name and is safe to call repeatedly.
//
// It leaves the durable checkpoint unchanged; only session acknowledgments
// advance that recovery position.
func (lease *ReplicationSourceLease) Release() {
	if lease == nil || lease.db == nil {
		return
	}
	lease.once.Do(func() {
		db := lease.db
		db.mu.Lock()
		delete(db.replicationSourceLeases, lease.name)
		db.mu.Unlock()
	})
}
