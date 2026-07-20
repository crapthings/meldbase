package meldbase

import (
	"context"
	"fmt"
	"sync"
)

// ReplicationSourceSession is the primary-side state machine for one
// authenticated peer. It deliberately permits one unacknowledged batch at a
// time: this is both bounded flow control and the proof that a durable ACK can
// never skip an unseen source token.
type ReplicationSourceSession struct {
	mu           sync.Mutex
	subscription *DurableDatabaseChangeSubscription
	databaseID   [16]byte
	localMax     int
	peerMax      int
	hello        bool
	terminal     bool
	inFlight     uint64
}

// NewReplicationSourceSession binds an existing named durable database feed to
// one source identity. The caller owns peer authentication and must close the
// session when that authenticated connection ends.
func NewReplicationSourceSession(db *DB, subscription *DurableDatabaseChangeSubscription, limits ReplicationFrameLimits) (*ReplicationSourceSession, error) {
	if db == nil || subscription == nil {
		return nil, ErrClosed
	}
	localMax := normalizedReplicationFrameLimits(limits).MaxFrameBytes
	db.mu.RLock()
	closed, identity := db.closed, db.databaseID
	db.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	if identity == [16]byte{} {
		return nil, ErrCorrupt
	}
	if _, err := subscription.Checkpoint(); err != nil {
		return nil, err
	}
	return &ReplicationSourceSession{subscription: subscription, databaseID: identity, localMax: localMax}, nil
}

func (session *ReplicationSourceSession) Close() {
	if session != nil && session.subscription != nil {
		session.subscription.Close()
	}
}

// AcceptHello validates a receiver's exact durable position. A different
// identity or checkpoint is a safe resync response, never an implicit rewind
// or jump of the source durable consumer.
func (session *ReplicationSourceSession) AcceptHello(frame ReplicationFrame) (*ReplicationFrame, error) {
	if session == nil || session.subscription == nil {
		return nil, ErrClosed
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.terminal || session.hello || frame.Type != ReplicationHelloFrame || frame.MaxBytes < 64<<10 || frame.MaxBytes > DefaultReplicationMaxFrameBytes {
		return nil, fmt.Errorf("%w: invalid hello state", ErrReplicaProtocol)
	}
	if frame.DatabaseID != session.databaseID {
		session.terminal = true
		return &ReplicationFrame{Type: ReplicationResyncFrame, DatabaseID: session.databaseID, Reason: "identity_mismatch"}, nil
	}
	checkpoint, err := session.subscription.Checkpoint()
	if err != nil {
		return nil, err
	}
	if frame.AfterToken != checkpoint {
		session.terminal = true
		return &ReplicationFrame{Type: ReplicationResyncFrame, DatabaseID: session.databaseID, Reason: "snapshot_required"}, nil
	}
	session.hello, session.peerMax = true, min(session.localMax, frame.MaxBytes)
	return nil, nil
}

// NextFrame waits for the next ordered source batch. The caller must not send
// another frame until AcceptAck succeeds for this token. If the peer's declared
// frame cap cannot carry one atomic Commit Log batch, the session terminates
// with snapshot_required rather than splitting or partially sending a commit.
func (session *ReplicationSourceSession) NextFrame(ctx context.Context) (*ReplicationFrame, error) {
	if session == nil || session.subscription == nil {
		return nil, ErrClosed
	}
	session.mu.Lock()
	if !session.hello || session.terminal || session.inFlight != 0 {
		session.mu.Unlock()
		return nil, fmt.Errorf("%w: batch send state", ErrReplicaProtocol)
	}
	session.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err, ok := <-session.subscription.Errors:
		if !ok {
			return nil, ErrClosed
		}
		return nil, err
	case batch, ok := <-session.subscription.Batches:
		if !ok {
			return nil, ErrClosed
		}
		frame := &ReplicationFrame{Type: ReplicationBatchFrame, DatabaseID: session.databaseID, Batch: &batch}
		session.mu.Lock()
		defer session.mu.Unlock()
		if session.terminal || !session.hello || session.inFlight != 0 {
			return nil, fmt.Errorf("%w: batch send state", ErrReplicaProtocol)
		}
		if _, err := MarshalReplicationFrame(*frame, ReplicationFrameLimits{MaxFrameBytes: session.peerMax}); err != nil {
			session.terminal = true
			return &ReplicationFrame{Type: ReplicationResyncFrame, DatabaseID: session.databaseID, Reason: "snapshot_required"}, nil
		}
		session.inFlight = batch.Token
		return frame, nil
	}
}

// AcceptAck makes one remote acknowledgement durable on the source. The token
// must exactly match the one batch in flight; stale, future or duplicate ACKs
// cannot release retained history.
func (session *ReplicationSourceSession) AcceptAck(frame ReplicationFrame) error {
	if session == nil || session.subscription == nil {
		return ErrClosed
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.terminal || !session.hello || frame.Type != ReplicationAckFrame || frame.DatabaseID != session.databaseID || session.inFlight == 0 || frame.AckToken != session.inFlight {
		return fmt.Errorf("%w: invalid acknowledgement", ErrReplicaProtocol)
	}
	if err := session.subscription.Ack(frame.AckToken); err != nil {
		return err
	}
	session.inFlight = 0
	return nil
}

func (session *ReplicationSourceSession) Checkpoint() (uint64, error) {
	if session == nil || session.subscription == nil {
		return 0, ErrClosed
	}
	return session.subscription.Checkpoint()
}
