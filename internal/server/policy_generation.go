package server

import (
	"context"
	"errors"

	"github.com/crapthings/meldbase"
	"github.com/crapthings/meldbase/internal/policyrecord"
	"github.com/crapthings/meldbase/internal/systemrecord"
)

type PolicyGenerationStore interface {
	LoadPolicyGeneration(context.Context, string) ([16]byte, bool, error)
}

type DurablePolicyGenerationStore struct {
	backend systemrecord.Backend
	db      *meldbase.DB
}

func NewDurablePolicyGenerationStore(db *meldbase.DB) (*DurablePolicyGenerationStore, error) {
	if db == nil {
		return nil, errors.New("meldbase server: policy generation database is required")
	}
	backend := db.MeldbaseSystemRecordBackend()
	if backend == nil {
		return nil, errors.New("meldbase server: durable policy generations require an open V2 database")
	}
	return &DurablePolicyGenerationStore{backend: backend, db: db}, nil
}

func (store *DurablePolicyGenerationStore) LoadPolicyGeneration(ctx context.Context, collection string) ([16]byte, bool, error) {
	var zero [16]byte
	if store == nil || store.backend == nil || ctx == nil {
		return zero, false, errors.New("invalid durable policy generation read")
	}
	key, err := policyrecord.Key(collection)
	if err != nil {
		return zero, false, err
	}
	end := append(append([]byte(nil), key...), 0)
	records, err := store.backend.Scan(ctx, key, end, 1)
	if err != nil {
		return zero, false, err
	}
	if len(records) == 0 {
		return zero, false, nil
	}
	if len(records) != 1 || string(records[0].Key) != string(key) {
		return zero, false, errors.New("invalid durable policy generation key")
	}
	generation, err := policyrecord.Decode(records[0].Value)
	if err != nil {
		return zero, false, err
	}
	return generation, true, nil
}
