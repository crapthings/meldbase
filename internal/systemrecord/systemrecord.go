// Package systemrecord defines the private bridge between the root database and
// higher-level built-in services. It is internal so applications cannot depend
// on raw control-plane records as a public key/value API.
package systemrecord

import "context"

type Mutation struct {
	TransactionID  [16]byte
	Key            []byte
	ExpectedExists bool
	ExpectedHash   [32]byte
	NewValue       []byte
	Delete         bool
	Unconditional  bool
}

type Result struct {
	Applied bool
	Current []byte
}

type KeyValue struct {
	Key   []byte
	Value []byte
}

type Backend interface {
	CompareAndSwap(context.Context, Mutation) (Result, error)
	Scan(context.Context, []byte, []byte, int) ([]KeyValue, error)
}
