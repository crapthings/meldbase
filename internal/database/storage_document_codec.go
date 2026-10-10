package database

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
	// storedDocumentVersion identifies the stored document envelope revision.
	storedDocumentVersion = 1
	// storedDocumentHeader is the byte size of a stored document envelope header.
	storedDocumentHeader = 24
	// maxStoredDocumentBody bounds the byte size of a stored typed document body.
	maxStoredDocumentBody = 64 * 1024 * 1024
)

// storedDocumentMagic identifies the storage envelope of a typed document.
var storedDocumentMagic = [8]byte{'M', 'E', 'L', 'D', 'D', 'O', 'C', '2'}

// encodeStoredDocument wraps the canonical typed document body in a
// self-describing Storage  envelope.
//
// The distinct logical/stored lengths
// reserve a compatible path for optional compression without changing the
// typed body codec.
func encodeStoredDocument(document Document) ([]byte, error) {
	body, err := encodeDocumentBinary(document)
	if err != nil {
		return nil, err
	}
	if len(body) > maxStoredDocumentBody {
		return nil, ErrInvalidDocument
	}
	encoded := make([]byte, storedDocumentHeader+len(body))
	copy(encoded[:8], storedDocumentMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], storedDocumentVersion)
	binary.LittleEndian.PutUint16(encoded[10:12], storedDocumentHeader)
	// 12:16 is a zero flags field. No compression is currently defined.
	binary.LittleEndian.PutUint32(encoded[16:20], uint32(len(body)))
	binary.LittleEndian.PutUint32(encoded[20:24], uint32(len(body)))
	copy(encoded[storedDocumentHeader:], body)
	return encoded, nil
}

// decodeStoredDocument validates and decodes a stored typed document.
func decodeStoredDocument(encoded []byte) (Document, error) {
	body, err := storedDocumentBody(encoded)
	if err != nil {
		return nil, err
	}
	document, err := decodeDocumentBinary(body)
	if err != nil {
		return nil, errors.Join(ErrCorrupt, err)
	}
	if err := document.Validate(); err != nil {
		return nil, errors.Join(ErrCorrupt, err)
	}
	return document, nil
}

// storedDocumentBody validates the storage envelope and returns its shared document body.
func storedDocumentBody(encoded []byte) ([]byte, error) {
	if len(encoded) < storedDocumentHeader || !bytes.Equal(encoded[:8], storedDocumentMagic[:]) ||
		binary.LittleEndian.Uint16(encoded[8:10]) != storedDocumentVersion ||
		binary.LittleEndian.Uint16(encoded[10:12]) != storedDocumentHeader ||
		binary.LittleEndian.Uint32(encoded[12:16]) != 0 {
		return nil, ErrCorrupt
	}
	logical := uint64(binary.LittleEndian.Uint32(encoded[16:20]))
	stored := uint64(binary.LittleEndian.Uint32(encoded[20:24]))
	if logical != stored || stored > maxStoredDocumentBody || stored != uint64(len(encoded)-storedDocumentHeader) {
		return nil, ErrCorrupt
	}
	return encoded[storedDocumentHeader:], nil
}
