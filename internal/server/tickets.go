package server

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"sync"
	"time"
)

type ticketRecord struct {
	principal Principal
	expires   time.Time
}
type ticketStore struct {
	mu      sync.Mutex
	ttl     time.Duration
	tickets map[[32]byte]ticketRecord
}

func newTicketStore(ttl time.Duration) *ticketStore {
	return &ticketStore{ttl: ttl, tickets: make(map[[32]byte]ticketRecord)}
}

func (s *ticketStore) issue(principal Principal) (string, error) {
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
	s.tickets[key] = ticketRecord{principal: principal, expires: now.Add(s.ttl)}
	s.mu.Unlock()
	return ticket, nil
}

func (s *ticketStore) consume(ticket string) (Principal, bool) {
	key := sha256.Sum256([]byte(ticket))
	s.mu.Lock()
	record, ok := s.tickets[key]
	delete(s.tickets, key)
	s.mu.Unlock()
	if !ok || !record.expires.After(time.Now()) {
		return Principal{}, false
	}
	return record.principal, true
}
