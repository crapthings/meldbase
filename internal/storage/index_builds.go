package storage

import (
	"encoding/binary"
	"time"
	"unicode/utf8"
)

const indexBuildMetaHeaderBytes = 160

var indexBuildMetaMagic = [8]byte{'M', 'E', 'L', 'D', 'I', 'B', 'L', 'D'}

type IndexBuildPhase uint8

const (
	IndexBuildScan IndexBuildPhase = 1 + iota
	IndexBuildCatchUp
	IndexBuildReady
	IndexBuildFailed
)

type IndexBuildFailure uint8

const (
	IndexBuildFailureNone IndexBuildFailure = iota
	IndexBuildFailureUniqueConflict
	IndexBuildFailureResourceLimit
	IndexBuildFailureHistoryLost
	IndexBuildFailureCanceled
	IndexBuildFailureInvalidIndex
)

type IndexBuildMeta struct {
	BuildID           [16]byte
	CollectionID      uint32
	Collection        string
	Name              string
	FieldPath         string
	Fields            []IndexField
	Unique            bool
	Phase             IndexBuildPhase
	Failure           IndexBuildFailure
	SourceSequence    uint64
	SourceCatalogRoot uint64
	// AppliedCatalogRoot is the immutable catalog snapshot represented by the
	// shadow after catch-up advances beyond SourceSequence. Zero is the legacy
	// encoding and is equivalent to SourceCatalogRoot only while both sequences
	// are equal.
	AppliedCatalogRoot uint64
	ShadowRoot         uint64
	ScanAfter          [16]byte
	AppliedSequence    uint64
	EntryCount         uint64
	CanonicalBytes     uint64
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func encodeIndexBuildMeta(meta IndexBuildMeta) ([]byte, error) {
	if !validIndexBuildMeta(meta) {
		return nil, ErrCorrupt
	}
	fields, _ := normalizeIndexFields(meta.FieldPath, meta.Fields)
	payload := []byte(nil)
	codec := uint16(0)
	if compoundIndexFields(fields) {
		codec = indexKeyCodecV3
		for _, field := range fields {
			payload = append(payload, byte(field.Direction), 0, 0)
			binary.LittleEndian.PutUint16(payload[len(payload)-2:], uint16(len(field.Path)))
			payload = append(payload, field.Path...)
		}
	} else {
		payload = append(payload, fields[0].Path...)
	}
	if len(meta.Collection) > 65535 || len(meta.Name) > 65535 || len(payload) > 65535 {
		return nil, ErrCorrupt
	}
	encoded := make([]byte, indexBuildMetaHeaderBytes+len(meta.Collection)+len(meta.Name)+len(payload))
	copy(encoded[:8], indexBuildMetaMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(encoded[10:12], indexBuildMetaHeaderBytes)
	encoded[12], encoded[13], encoded[14] = byte(meta.Phase), boolByte(meta.Unique), byte(meta.Failure)
	if meta.AppliedCatalogRoot != 0 {
		encoded[15] = 1
	}
	binary.LittleEndian.PutUint64(encoded[16:24], meta.SourceSequence)
	binary.LittleEndian.PutUint64(encoded[24:32], meta.SourceCatalogRoot)
	binary.LittleEndian.PutUint64(encoded[32:40], meta.ShadowRoot)
	binary.LittleEndian.PutUint64(encoded[40:48], meta.AppliedSequence)
	binary.LittleEndian.PutUint64(encoded[48:56], meta.EntryCount)
	binary.LittleEndian.PutUint64(encoded[56:64], meta.CanonicalBytes)
	binary.LittleEndian.PutUint64(encoded[64:72], uint64(meta.CreatedAt.UnixMilli()))
	binary.LittleEndian.PutUint64(encoded[72:80], uint64(meta.UpdatedAt.UnixMilli()))
	binary.LittleEndian.PutUint32(encoded[80:84], meta.CollectionID)
	binary.LittleEndian.PutUint16(encoded[84:86], uint16(len(meta.Collection)))
	binary.LittleEndian.PutUint16(encoded[86:88], uint16(len(meta.Name)))
	binary.LittleEndian.PutUint16(encoded[88:90], uint16(len(payload)))
	binary.LittleEndian.PutUint16(encoded[90:92], codec)
	if codec == indexKeyCodecV3 {
		binary.LittleEndian.PutUint16(encoded[92:94], uint16(len(fields)))
	}
	copy(encoded[96:112], meta.ScanAfter[:])
	binary.LittleEndian.PutUint64(encoded[112:120], meta.AppliedCatalogRoot)
	cursor := indexBuildMetaHeaderBytes
	copy(encoded[cursor:], meta.Collection)
	cursor += len(meta.Collection)
	copy(encoded[cursor:], meta.Name)
	cursor += len(meta.Name)
	copy(encoded[cursor:], payload)
	return encoded, nil
}

func decodeIndexBuildMeta(key, encoded []byte) (IndexBuildMeta, error) {
	var meta IndexBuildMeta
	if len(key) != len(meta.BuildID) || allZero(key) || len(encoded) < indexBuildMetaHeaderBytes ||
		string(encoded[:8]) != string(indexBuildMetaMagic[:]) ||
		binary.LittleEndian.Uint16(encoded[8:10]) != FormatVersion ||
		binary.LittleEndian.Uint16(encoded[10:12]) != indexBuildMetaHeaderBytes || encoded[13] > 1 || encoded[15] > 1 ||
		!allZero(encoded[94:96]) || !allZero(encoded[120:indexBuildMetaHeaderBytes]) {
		return meta, ErrCorrupt
	}
	collectionLength := int(binary.LittleEndian.Uint16(encoded[84:86]))
	nameLength := int(binary.LittleEndian.Uint16(encoded[86:88]))
	fieldLength := int(binary.LittleEndian.Uint16(encoded[88:90]))
	codec := binary.LittleEndian.Uint16(encoded[90:92])
	fieldCount := int(binary.LittleEndian.Uint16(encoded[92:94]))
	if collectionLength == 0 || nameLength == 0 || fieldLength == 0 ||
		len(encoded) != indexBuildMetaHeaderBytes+collectionLength+nameLength+fieldLength {
		return meta, ErrCorrupt
	}
	copy(meta.BuildID[:], key)
	meta.Phase, meta.Unique, meta.Failure = IndexBuildPhase(encoded[12]), encoded[13] == 1, IndexBuildFailure(encoded[14])
	meta.SourceSequence = binary.LittleEndian.Uint64(encoded[16:24])
	meta.SourceCatalogRoot = binary.LittleEndian.Uint64(encoded[24:32])
	meta.ShadowRoot = binary.LittleEndian.Uint64(encoded[32:40])
	meta.AppliedSequence = binary.LittleEndian.Uint64(encoded[40:48])
	meta.EntryCount = binary.LittleEndian.Uint64(encoded[48:56])
	meta.CanonicalBytes = binary.LittleEndian.Uint64(encoded[56:64])
	meta.CreatedAt = time.UnixMilli(int64(binary.LittleEndian.Uint64(encoded[64:72]))).UTC()
	meta.UpdatedAt = time.UnixMilli(int64(binary.LittleEndian.Uint64(encoded[72:80]))).UTC()
	meta.CollectionID = binary.LittleEndian.Uint32(encoded[80:84])
	copy(meta.ScanAfter[:], encoded[96:112])
	meta.AppliedCatalogRoot = binary.LittleEndian.Uint64(encoded[112:120])
	if (encoded[15] == 0) != (meta.AppliedCatalogRoot == 0) {
		return IndexBuildMeta{}, ErrCorrupt
	}
	cursor := indexBuildMetaHeaderBytes
	meta.Collection = string(encoded[cursor : cursor+collectionLength])
	cursor += collectionLength
	meta.Name = string(encoded[cursor : cursor+nameLength])
	cursor += nameLength
	payload := encoded[cursor:]
	switch codec {
	case 0:
		if fieldCount != 0 || fieldLength > maxIndexFieldBytes {
			return IndexBuildMeta{}, ErrCorrupt
		}
		meta.FieldPath = string(payload)
	case indexKeyCodecV3:
		if fieldCount == 0 || fieldCount > maxIndexFields {
			return IndexBuildMeta{}, ErrCorrupt
		}
		meta.Fields = make([]IndexField, 0, fieldCount)
		for offset := 0; offset < len(payload); {
			if len(payload)-offset < 3 {
				return IndexBuildMeta{}, ErrCorrupt
			}
			direction := int8(payload[offset])
			length := int(binary.LittleEndian.Uint16(payload[offset+1 : offset+3]))
			offset += 3
			if length == 0 || length > maxIndexFieldBytes || offset+length > len(payload) {
				return IndexBuildMeta{}, ErrCorrupt
			}
			meta.Fields = append(meta.Fields, IndexField{Path: string(payload[offset : offset+length]), Direction: direction})
			offset += length
		}
		if len(meta.Fields) != fieldCount {
			return IndexBuildMeta{}, ErrCorrupt
		}
		meta.FieldPath = meta.Fields[0].Path
	default:
		return IndexBuildMeta{}, ErrCorrupt
	}
	if !validIndexBuildMeta(meta) {
		return IndexBuildMeta{}, ErrCorrupt
	}
	return meta, nil
}

func validIndexBuildMeta(meta IndexBuildMeta) bool {
	fields, validFields := normalizeIndexFields(meta.FieldPath, meta.Fields)
	if allZero(meta.BuildID[:]) || meta.CollectionID == 0 || !validCollectionName(meta.Collection) ||
		!validIndexName(meta.Name) || !validFields ||
		meta.SourceSequence == 0 || meta.SourceCatalogRoot < 2 || (meta.AppliedCatalogRoot != 0 && meta.AppliedCatalogRoot < 2) || meta.ShadowRoot < 2 ||
		meta.AppliedSequence < meta.SourceSequence || meta.CreatedAt.IsZero() || meta.UpdatedAt.Before(meta.CreatedAt) ||
		!utf8.ValidString(meta.Collection) || !utf8.ValidString(meta.Name) || !utf8.ValidString(meta.FieldPath) {
		return false
	}
	_ = fields
	if meta.Phase < IndexBuildScan || meta.Phase > IndexBuildFailed || meta.Failure > IndexBuildFailureInvalidIndex {
		return false
	}
	if meta.Phase == IndexBuildScan && (meta.AppliedSequence != meta.SourceSequence || meta.AppliedCatalogRoot != 0) {
		return false
	}
	return (meta.Phase == IndexBuildFailed) == (meta.Failure != IndexBuildFailureNone)
}

func validateIndexBuildMetaFeatures(required uint64, meta IndexBuildMeta) error {
	if meta.AppliedCatalogRoot != 0 && required&RequiredFeatureIndexBuildAppliedRoot == 0 {
		return ErrCorrupt
	}
	return nil
}

func (meta IndexBuildMeta) effectiveAppliedCatalogRoot() (uint64, bool) {
	if meta.AppliedSequence == meta.SourceSequence {
		return meta.SourceCatalogRoot, true
	}
	return meta.AppliedCatalogRoot, meta.AppliedCatalogRoot >= 2
}

func boolByte(value bool) byte {
	if value {
		return 1
	}
	return 0
}
