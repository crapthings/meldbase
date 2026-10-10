// Package systemrecord lets built-in services read and change private database records.
//
// Applications cannot use these records as a public key/value API.
package systemrecord

import "context"

// Mutation describes a conditional private record write or deletion.
type Mutation struct {
	TransactionID  [16]byte
	Key            []byte
	ExpectedExists bool
	ExpectedHash   [32]byte
	NewValue       []byte
	Delete         bool
	Unconditional  bool
}

// Result reports whether a mutation applied and returns the current bytes on a mismatch.
type Result struct {
	Applied bool
	Current []byte
}

// KeyValue pairs a private record key with its stored bytes.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// Backend provides conditional writes and bounded scans for built-in service records.
type Backend interface {
	CompareAndSwap(context.Context, Mutation) (Result, error)
	Scan(context.Context, []byte, []byte, int) ([]KeyValue, error)
}
