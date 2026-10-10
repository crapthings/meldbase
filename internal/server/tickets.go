package server

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"sync"
	"time"
)

// ticketRecord binds a one-use connection ticket to an actor and expiration time.
type ticketRecord struct {
	actor   Actor
	expires time.Time
}

// ticketStore keeps hashed connection tickets until they are consumed or cleaned up after expiration.
type ticketStore struct {
	mu      sync.Mutex
	ttl     time.Duration
	tickets map[[32]byte]ticketRecord
}

// newTicketStore creates an empty connection ticket store with the supplied lifetime.
func newTicketStore(ttl time.Duration) *ticketStore {
	return &ticketStore{ttl: ttl, tickets: make(map[[32]byte]ticketRecord)}
}

// issue generates an actor-bound connection ticket and removes expired tickets from the store.
func (s *ticketStore) issue(actor Actor) (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	ticket := base64.RawURLEncoding.EncodeToString(bytes)
	key := sha256.Sum256([]byte(ticket))
	now := time.Now()
	s.mu.Lock()
	for existing, record := range s.tickets {
		if !record.expires.After(now) {
			delete(s.tickets, existing)
		}
	}
	s.tickets[key] = ticketRecord{actor: actor, expires: now.Add(s.ttl)}
	s.mu.Unlock()
	return ticket, nil
}

// consume removes a ticket and returns its actor only if the ticket existed and has not expired.
func (s *ticketStore) consume(ticket string) (Actor, bool) {
	key := sha256.Sum256([]byte(ticket))
	s.mu.Lock()
	record, ok := s.tickets[key]
	delete(s.tickets, key)
	s.mu.Unlock()
	if !ok || !record.expires.After(time.Now()) {
		return Actor{}, false
	}
	return record.actor, true
}
