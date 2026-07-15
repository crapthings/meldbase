package meldbase

import "errors"

var (
	ErrClosed            = errors.New("meldbase: database is closed")
	ErrInvalidDocument   = errors.New("meldbase: invalid document")
	ErrInvalidFilter     = errors.New("meldbase: invalid filter")
	ErrInvalidUpdate     = errors.New("meldbase: invalid update")
	ErrMutationLimit     = errors.New("meldbase: mutation affected-row limit exceeded")
	ErrNotFound          = errors.New("meldbase: document not found")
	ErrDuplicateID       = errors.New("meldbase: duplicate document id")
	ErrInvalidCollection = errors.New("meldbase: invalid collection")
	ErrImmutableID       = errors.New("meldbase: _id is immutable")
	ErrSlowConsumer      = errors.New("meldbase: change consumer is too slow")
	ErrCorrupt           = errors.New("meldbase: corrupt database")
	ErrDuplicateKey      = errors.New("meldbase: duplicate index key")
	ErrInvalidIndex      = errors.New("meldbase: invalid index")
	ErrDurability        = errors.New("meldbase: durability failure; writes are disabled")
)
