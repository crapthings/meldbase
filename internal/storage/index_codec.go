package storage

import (
	"encoding/binary"
	"math"
)

// encodeIndexMeta validates and encodes a stored index definition.
func encodeIndexMeta(meta IndexMeta) ([]byte, error) {
	fields, valid := normalizeIndexFields(meta.FieldPath, meta.Fields)
	if !valid || !validIndexName(meta.Name) || meta.Root < 2 || meta.CreatedSequence == 0 || meta.UpdatedSequence < meta.CreatedSequence {
		return nil, ErrCorrupt
	}
	codec := meta.KeyCodecVersion
	if codec == 0 {
		if compoundIndexFields(fields) {
			codec = indexKeyCodecV3
		} else {
			codec = indexKeyCodec
		}
	}
	if (codec == indexKeyCodec && compoundIndexFields(fields)) || (codec != indexKeyCodec && codec != indexKeyCodecV3) {
		return nil, ErrCorrupt
	}
	payload := []byte(nil)
	if codec == indexKeyCodec {
		payload = append(payload, fields[0].Path...)
	} else {
		for _, field := range fields {
			payload = append(payload, byte(field.Direction), 0, 0)
			binary.LittleEndian.PutUint16(payload[len(payload)-2:], uint16(len(field.Path)))
			payload = append(payload, field.Path...)
		}
	}
	if len(payload) == 0 || len(payload) > math.MaxUint16 {
		return nil, ErrCorrupt
	}
	encoded := make([]byte, indexMetaHeaderBytes+len(payload))
	copy(encoded[:8], indexMetaMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(encoded[10:12], indexMetaHeaderBytes)
	if meta.Unique {
		encoded[12] = 1
	}
	binary.LittleEndian.PutUint64(encoded[16:24], meta.Root)
	binary.LittleEndian.PutUint64(encoded[24:32], meta.EntryCount)
	binary.LittleEndian.PutUint64(encoded[32:40], meta.CreatedSequence)
	binary.LittleEndian.PutUint64(encoded[40:48], meta.UpdatedSequence)
	binary.LittleEndian.PutUint16(encoded[48:50], uint16(len(payload)))
	binary.LittleEndian.PutUint16(encoded[50:52], codec)
	if codec == indexKeyCodecV3 {
		binary.LittleEndian.PutUint16(encoded[52:54], uint16(len(fields)))
	}
	copy(encoded[indexMetaHeaderBytes:], payload)
	return encoded, nil
}

// decodeIndexMeta validates and reads a stored index definition.
func decodeIndexMeta(name string, encoded []byte) (IndexMeta, error) {
	if !validIndexName(name) || len(encoded) < indexMetaHeaderBytes || string(encoded[:8]) != string(indexMetaMagic[:]) ||
		binary.LittleEndian.Uint16(encoded[8:10]) != FormatVersion || binary.LittleEndian.Uint16(encoded[10:12]) != indexMetaHeaderBytes ||
		encoded[12] > 1 || !allZero(encoded[13:16]) {
		return IndexMeta{}, ErrCorrupt
	}
	payloadLength := int(binary.LittleEndian.Uint16(encoded[48:50]))
	codec := binary.LittleEndian.Uint16(encoded[50:52])
	if payloadLength == 0 || len(encoded) != indexMetaHeaderBytes+payloadLength {
		return IndexMeta{}, ErrCorrupt
	}
	meta := IndexMeta{
		Name: name, Unique: encoded[12] == 1,
		Root: binary.LittleEndian.Uint64(encoded[16:24]), EntryCount: binary.LittleEndian.Uint64(encoded[24:32]),
		CreatedSequence: binary.LittleEndian.Uint64(encoded[32:40]), UpdatedSequence: binary.LittleEndian.Uint64(encoded[40:48]),
		KeyCodecVersion: codec,
	}
	payload := encoded[indexMetaHeaderBytes:]
	switch codec {
	case indexKeyCodec:
		if !allZero(encoded[52:64]) || payloadLength > maxIndexFieldBytes {
			return IndexMeta{}, ErrCorrupt
		}
		meta.FieldPath = string(payload)
		meta.Fields = []IndexField{{Path: meta.FieldPath, Direction: 1}}
	case indexKeyCodecV3:
		fieldCount := int(binary.LittleEndian.Uint16(encoded[52:54]))
		if fieldCount == 0 || fieldCount > maxIndexFields || !allZero(encoded[54:64]) {
			return IndexMeta{}, ErrCorrupt
		}
		meta.Fields = make([]IndexField, 0, fieldCount)
		for offset := 0; offset < len(payload); {
			if len(payload)-offset < 3 {
				return IndexMeta{}, ErrCorrupt
			}
			direction := int8(payload[offset])
			length := int(binary.LittleEndian.Uint16(payload[offset+1 : offset+3]))
			offset += 3
			if length == 0 || length > maxIndexFieldBytes || offset+length > len(payload) {
				return IndexMeta{}, ErrCorrupt
			}
			meta.Fields = append(meta.Fields, IndexField{Path: string(payload[offset : offset+length]), Direction: direction})
			offset += length
		}
		if len(meta.Fields) != fieldCount {
			return IndexMeta{}, ErrCorrupt
		}
		meta.FieldPath = meta.Fields[0].Path
	default:
		return IndexMeta{}, ErrCorrupt
	}
	fields, valid := normalizeIndexFields(meta.FieldPath, meta.Fields)
	if !valid || (codec == indexKeyCodec && compoundIndexFields(fields)) || meta.Root < 2 || meta.CreatedSequence == 0 ||
		meta.UpdatedSequence < meta.CreatedSequence {
		return IndexMeta{}, ErrCorrupt
	}
	meta.Fields = fields
	return meta, nil
}

// DecodeIndexMeta reads an index definition from a commit log catalog event.
//
// This exposes the definition rather than a public contract for the physical catalog layout.
func DecodeIndexMeta(name string, encoded []byte) (IndexMeta, error) {
	return decodeIndexMeta(name, encoded)
}
