package storage

import (
	"bytes"
	"encoding/binary"
)

// freeSpaceExtentMagic identifies encoded reusable page ranges.
var freeSpaceExtentMagic = [8]byte{'M', 'E', 'L', 'D', 'F', 'R', '3', '1'}

// freeSpaceKey encodes a page range start in sortable byte order.
func freeSpaceKey(pageID uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, pageID)
	return key
}

// encodeFreeSpaceExtent encodes a reusable range end and its audited file bounds.
func encodeFreeSpaceExtent(end, safePhysicalPages, auditGeneration uint64) ([]byte, error) {
	if end <= 2 || safePhysicalPages < end || auditGeneration == 0 {
		return nil, ErrCorrupt
	}
	value := make([]byte, 32)
	copy(value[:8], freeSpaceExtentMagic[:])
	binary.LittleEndian.PutUint64(value[8:16], end)
	binary.LittleEndian.PutUint64(value[16:24], safePhysicalPages)
	binary.LittleEndian.PutUint64(value[24:32], auditGeneration)
	return value, nil
}

// decodeFreeSpaceExtent validates and reads a reusable page range and its audit generation.
func decodeFreeSpaceExtent(key, value []byte, physicalPages uint64) (uint64, uint64, uint64, error) {
	if len(key) != 8 || len(value) != 32 || !bytes.Equal(value[:8], freeSpaceExtentMagic[:]) {
		return 0, 0, 0, ErrCorrupt
	}
	start := binary.BigEndian.Uint64(key)
	end := binary.LittleEndian.Uint64(value[8:16])
	safePhysicalPages := binary.LittleEndian.Uint64(value[16:24])
	auditGeneration := binary.LittleEndian.Uint64(value[24:32])
	if start < 2 || end <= start || safePhysicalPages < end || safePhysicalPages > physicalPages || auditGeneration == 0 {
		return 0, 0, 0, ErrCorrupt
	}
	return start, end, auditGeneration, nil
}
