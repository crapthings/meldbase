package database

import "sync"

// ReplicationSourceLease gives one authenticated source-side replica identity
// exclusive process-local ownership of its durable consumer. A lease prevents
// duplicate concurrent transports from racing one checkpoint; it does not
// establish distributed primary authority or replace follower-promotion
// fencing.
type ReplicationSourceLease struct {
	db   *DB
	name string
	once sync.Once
}

// AcquireReplicationSourceLease reserves name until Release. Callers must
// authenticate and derive name outside untrusted replication frames. The lease
// is intentionally DB-local: a database file already has one writer process,
// while primary election remains an external control-plane responsibility.
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

// Release is idempotent. It does not alter the durable consumer checkpoint;
// normal source-session ACK handling remains the only way to advance that
// recovery position.
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
