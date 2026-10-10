package storage

import (
	"crypto/sha256"
	"encoding/binary"
)

// storeDocumentRecord encodes a document with its insertion position.
func (tx *WriteTxn) storeDocumentRecord(position uint64, value []byte) ([]byte, error) {
	if position == 0 {
		return nil, ErrCorrupt
	}
	descriptor, err := tx.storeDocumentValue(value)
	if err != nil {
		return nil, err
	}
	stored := make([]byte, documentRecordHeaderBytes+len(descriptor))
	copy(stored[:8], documentRecordMagic[:])
	binary.LittleEndian.PutUint16(stored[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(stored[10:12], documentRecordHeaderBytes)
	binary.LittleEndian.PutUint64(stored[16:24], position)
	copy(stored[documentRecordHeaderBytes:], descriptor)
	return stored, nil
}

// decodeDocumentRecordDescriptor validates a record header and reads its position and value descriptor.
func decodeDocumentRecordDescriptor(stored []byte) (uint64, []byte, error) {
	if len(stored) <= documentRecordHeaderBytes || string(stored[:8]) != string(documentRecordMagic[:]) ||
		binary.LittleEndian.Uint16(stored[8:10]) != FormatVersion ||
		binary.LittleEndian.Uint16(stored[10:12]) != documentRecordHeaderBytes || !allZero(stored[12:16]) {
		return 0, nil, ErrCorrupt
	}
	position := binary.LittleEndian.Uint64(stored[16:24])
	if position == 0 {
		return 0, nil, ErrCorrupt
	}
	return position, stored[documentRecordHeaderBytes:], nil
}

// loadDocumentRecord reads a stored document and its insertion position.
func (tx *WriteTxn) loadDocumentRecord(stored []byte) (uint64, []byte, error) {
	position, descriptor, err := decodeDocumentRecordDescriptor(stored)
	if err != nil {
		return 0, nil, err
	}
	value, err := tx.loadDocumentValue(descriptor)
	if err != nil {
		return 0, nil, err
	}
	return position, value, nil
}

// storeDocumentValue stores document bytes inline or across linked overflow pages.
func (tx *WriteTxn) storeDocumentValue(value []byte) ([]byte, error) {
	if len(value) == 0 || len(value) > maxDocumentBytes {
		return nil, ErrCorrupt
	}
	if len(value)+1 <= inlineDocumentLimit {
		return append([]byte{0}, value...), nil
	}
	const overflowHeader = 16
	chunkBytes := PageSize - PageHeaderSize - overflowHeader
	chunks := (len(value) + chunkBytes - 1) / chunkBytes
	pageIDs, err := tx.allocatePageIDs(chunks)
	if err != nil {
		return nil, err
	}
	firstPage := pageIDs[0]
	for index := 0; index < chunks; index++ {
		start := index * chunkBytes
		end := start + chunkBytes
		if end > len(value) {
			end = len(value)
		}
		payload := make([]byte, overflowHeader+end-start)
		binary.LittleEndian.PutUint64(payload[0:8], uint64(len(value)))
		binary.LittleEndian.PutUint32(payload[8:12], uint32(index))
		binary.LittleEndian.PutUint32(payload[12:16], uint32(chunks))
		copy(payload[16:], value[start:end])
		link := uint64(0)
		if index+1 < chunks {
			link = pageIDs[index+1]
		}
		if err := tx.appendPageAt(pageIDs[index], PageDocumentOverflow, 0, 1, link, payload); err != nil {
			return nil, err
		}
	}
	descriptor := make([]byte, 49)
	descriptor[0] = 1
	binary.LittleEndian.PutUint64(descriptor[1:9], uint64(len(value)))
	binary.LittleEndian.PutUint64(descriptor[9:17], firstPage)
	checksum := sha256.Sum256(value)
	copy(descriptor[17:], checksum[:])
	return descriptor, nil
}

// loadDocumentValue reads document bytes from an inline value or verified overflow pages.
func (tx *WriteTxn) loadDocumentValue(stored []byte) ([]byte, error) {
	if len(stored) == 0 {
		return nil, ErrCorrupt
	}
	if stored[0] == 0 {
		if len(stored) == 1 {
			return nil, ErrCorrupt
		}
		return append([]byte(nil), stored[1:]...), nil
	}
	if stored[0] != 1 || len(stored) != 49 {
		return nil, ErrCorrupt
	}
	total := binary.LittleEndian.Uint64(stored[1:9])
	pageID := binary.LittleEndian.Uint64(stored[9:17])
	if total < inlineDocumentLimit || total > maxDocumentBytes || pageID < 2 {
		return nil, ErrCorrupt
	}
	result := make([]byte, 0, int(total))
	seen := map[uint64]struct{}{}
	var expectedChunks uint32
	for index := uint32(0); uint64(len(result)) < total; index++ {
		if _, duplicate := seen[pageID]; duplicate {
			return nil, ErrCorrupt
		}
		seen[pageID] = struct{}{}
		raw, err := tx.readPage(pageID)
		if err != nil {
			return nil, err
		}
		page, err := DecodePage(raw, pageID)
		if err != nil || page.Type != PageDocumentOverflow || page.Flags != 0 || page.ItemCount != 1 || len(page.Payload) < 16 ||
			binary.LittleEndian.Uint64(page.Payload[0:8]) != total || binary.LittleEndian.Uint32(page.Payload[8:12]) != index {
			return nil, ErrCorrupt
		}
		chunks := binary.LittleEndian.Uint32(page.Payload[12:16])
		if chunks == 0 || (index > 0 && chunks != expectedChunks) || index >= chunks || uint64(len(result)+len(page.Payload)-16) > total {
			return nil, ErrCorrupt
		}
		expectedChunks = chunks
		result = append(result, page.Payload[16:]...)
		if index+1 == chunks {
			if page.Link != 0 || uint64(len(result)) != total {
				return nil, ErrCorrupt
			}
			break
		}
		if page.Link < 2 {
			return nil, ErrCorrupt
		}
		pageID = page.Link
	}
	checksum := sha256.Sum256(result)
	if !equalBytes(stored[17:], checksum[:]) {
		return nil, ErrCorrupt
	}
	return result, nil
}
