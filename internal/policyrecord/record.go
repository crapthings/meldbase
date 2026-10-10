// Package policyrecord encodes the stored versions of server query policies.
//
// It stores policy versions without deciding which queries are allowed.
package policyrecord

import (
	"encoding/binary"
	"errors"
	"regexp"

	"github.com/crapthings/meldbase/internal/systemrecord"
)

// recordVersion identifies the stored policy generation layout.
const recordVersion = 1

// collectionPattern defines accepted collection names for policy generation keys.
var collectionPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,127}$`)

// keyPrefix reserves the system-record namespace for collection policy generations.
var keyPrefix = []byte("query.policy.generation.v1\x00")

// recordMagic identifies encoded policy generation records.
var recordMagic = [8]byte{'M', 'E', 'L', 'D', 'P', 'O', 'L', '1'}

// Key validates a collection name and constructs its policy generation storage key.
func Key(collection string) ([]byte, error) {
	if !collectionPattern.MatchString(collection) {
		return nil, errors.New("invalid policy generation collection")
	}
	key := make([]byte, len(keyPrefix)+len(collection))
	copy(key, keyPrefix)
	copy(key[len(keyPrefix):], collection)
	return key, nil
}

// GenerationMutation creates an unconditional system-record write for a nonzero policy generation.
func GenerationMutation(collection string, generation [16]byte) (systemrecord.Mutation, error) {
	key, err := Key(collection)
	if err != nil || generation == [16]byte{} {
		return systemrecord.Mutation{}, errors.New("invalid policy generation mutation")
	}
	return systemrecord.Mutation{Key: key, NewValue: Encode(generation), Unconditional: true}, nil
}

// Encode writes a policy generation into the fixed-size binary record format.
func Encode(generation [16]byte) []byte {
	encoded := make([]byte, 32)
	copy(encoded[:8], recordMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], recordVersion)
	binary.LittleEndian.PutUint16(encoded[10:12], uint16(len(encoded)))
	copy(encoded[16:], generation[:])
	return encoded
}

// Decode validates a binary policy record and returns its nonzero generation.
func Decode(encoded []byte) ([16]byte, error) {
	var generation [16]byte
	if len(encoded) != 32 || string(encoded[:8]) != string(recordMagic[:]) ||
		binary.LittleEndian.Uint16(encoded[8:10]) != recordVersion ||
		binary.LittleEndian.Uint16(encoded[10:12]) != uint16(len(encoded)) {
		return generation, errors.New("invalid policy generation record")
	}
	for _, value := range encoded[12:16] {
		if value != 0 {
			return generation, errors.New("invalid policy generation reserved bytes")
		}
	}
	copy(generation[:], encoded[16:])
	if generation == [16]byte{} {
		return generation, errors.New("zero policy generation")
	}
	return generation, nil
}
