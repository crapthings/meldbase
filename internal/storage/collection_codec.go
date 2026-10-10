package storage

import "encoding/binary"

// encodeCollectionMeta validates and encodes a collection catalog record.
func encodeCollectionMeta(meta CollectionMeta) ([]byte, error) {
	if !validCollectionMeta(meta) {
		return nil, ErrCorrupt
	}
	value := make([]byte, 72)
	copy(value[:8], catalogValueMagic[:])
	binary.LittleEndian.PutUint16(value[8:10], FormatVersion)
	binary.LittleEndian.PutUint32(value[12:16], meta.ID)
	binary.LittleEndian.PutUint64(value[16:24], meta.PrimaryRoot)
	binary.LittleEndian.PutUint64(value[24:32], meta.IndexCatalogRoot)
	binary.LittleEndian.PutUint64(value[32:40], meta.DocumentCount)
	binary.LittleEndian.PutUint64(value[40:48], meta.CreatedSequence)
	binary.LittleEndian.PutUint64(value[48:56], meta.UpdatedSequence)
	binary.LittleEndian.PutUint64(value[56:64], meta.NextDocumentPosition)
	binary.LittleEndian.PutUint64(value[64:72], meta.OrderRoot)
	return value, nil
}

// decodeCollectionMeta reads and validates a collection catalog record.
func decodeCollectionMeta(value []byte) (CollectionMeta, error) {
	if len(value) != 72 || string(value[:8]) != string(catalogValueMagic[:]) || binary.LittleEndian.Uint16(value[8:10]) != FormatVersion ||
		binary.LittleEndian.Uint16(value[10:12]) != 0 {
		return CollectionMeta{}, ErrCorrupt
	}
	meta := CollectionMeta{
		ID:                   binary.LittleEndian.Uint32(value[12:16]),
		PrimaryRoot:          binary.LittleEndian.Uint64(value[16:24]),
		IndexCatalogRoot:     binary.LittleEndian.Uint64(value[24:32]),
		DocumentCount:        binary.LittleEndian.Uint64(value[32:40]),
		CreatedSequence:      binary.LittleEndian.Uint64(value[40:48]),
		UpdatedSequence:      binary.LittleEndian.Uint64(value[48:56]),
		NextDocumentPosition: binary.LittleEndian.Uint64(value[56:64]),
		OrderRoot:            binary.LittleEndian.Uint64(value[64:72]),
	}
	if !validCollectionMeta(meta) {
		return CollectionMeta{}, ErrCorrupt
	}
	return meta, nil
}

// validCollectionMeta checks collection identity, tree pointers, and document counters.
func validCollectionMeta(meta CollectionMeta) bool {
	return !(meta.ID == 0 || meta.PrimaryRoot < 2 || meta.OrderRoot < 2 || meta.CreatedSequence == 0 || meta.UpdatedSequence < meta.CreatedSequence ||
		meta.NextDocumentPosition < meta.DocumentCount || (meta.IndexCatalogRoot != 0 && meta.IndexCatalogRoot < 2))
}
