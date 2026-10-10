// Package storage implements the current Meldbase copy-on-write page format.
package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// PageSize is the byte size of every stored page.
	PageSize = 16 * 1024
	// PageHeaderSize is the byte size of a data page header.
	PageHeaderSize = 64
	// MetaHeaderSize is the byte size of the metadata header.
	MetaHeaderSize = 256
	// FormatVersion identifies the current on-disk format revision.
	FormatVersion = 3
)

var (
	// ErrCorrupt reports invalid stored data.
	ErrCorrupt = errors.New("meldbase storage: corrupt database")
	// ErrUnsupportedFormat reports a format revision this binary cannot read.
	ErrUnsupportedFormat = errors.New("meldbase storage: unsupported format version")
	// ErrUnsupportedFeature reports a required feature this binary does not understand.
	ErrUnsupportedFeature = errors.New("meldbase storage: unsupported required feature")
	// metaMagic identifies metadata pages.
	metaMagic = [8]byte{'M', 'E', 'L', 'D', 'M', 'T', '2', 0}
	// pageMagic identifies data pages.
	pageMagic = [8]byte{'M', 'E', 'L', 'D', 'P', 'G', '2', 0}
	// crcTable provides the CRC-32C table for data page checksums.
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

const (
	// RequiredFeatureShadowIndexBuilds marks stored background index build records.
	//
	// Required feature bits must be understood before any generation is opened.
	RequiredFeatureShadowIndexBuilds uint64 = 1 << 0
	// RequiredFeatureCompoundIndexes marks indexes with multiple ordered fields.
	RequiredFeatureCompoundIndexes uint64 = 1 << 1
	// RequiredFeatureIndexBuildAppliedRoot marks index builds that retain their last applied catalog root.
	RequiredFeatureIndexBuildAppliedRoot uint64 = 1 << 2
	// SupportedRequiredFeatures lists the required features this binary understands.
	SupportedRequiredFeatures uint64 = RequiredFeatureShadowIndexBuilds | RequiredFeatureCompoundIndexes | RequiredFeatureIndexBuildAppliedRoot
	// OptionalFeaturePersistentFreeSpace marks a stored index of reusable pages.
	//
	// Unknown optional bits are preserved and ignored. A writer may only alter
	// bits it owns; this permits older binaries to carry future acceleration
	// metadata through a normal commit without treating it as allocation truth.
	OptionalFeaturePersistentFreeSpace uint64 = 1 << 0
	// SupportedOptionalFeatures lists the optional features this binary understands.
	SupportedOptionalFeatures uint64 = OptionalFeaturePersistentFreeSpace
)

// PageType identifies the kind of content stored in a page.
type PageType uint8

const (
	// PageDatabaseRoot marks the page containing database root pointers.
	PageDatabaseRoot PageType = 1 + iota
	// PageCatalogBranch marks an internal collection catalog node.
	PageCatalogBranch
	// PageCatalogLeaf marks a collection catalog leaf.
	PageCatalogLeaf
	// PagePrimaryBranch marks an internal document index node.
	PagePrimaryBranch
	// PagePrimaryLeaf marks a document index leaf.
	PagePrimaryLeaf
	// PageSecondaryBranch marks an internal secondary index node.
	PageSecondaryBranch
	// PageSecondaryLeaf marks a secondary index leaf.
	PageSecondaryLeaf
	// PageDocumentOverflow stores document bytes that do not fit in a leaf.
	PageDocumentOverflow
	// PageCommitLogBranch marks an internal commit log node.
	PageCommitLogBranch
	// PageCommitLogLeaf marks a commit log leaf.
	PageCommitLogLeaf
	// PageCommitOverflow stores commit bytes that do not fit in a leaf.
	PageCommitOverflow
	// PageFreeSpaceBranch marks an internal free space index node.
	PageFreeSpaceBranch
	// PageFreeSpaceLeaf marks a free space index leaf.
	PageFreeSpaceLeaf
	// PageIndexCatalogBranch marks an internal index definition catalog node.
	PageIndexCatalogBranch
	// PageIndexCatalogLeaf marks an index definition catalog leaf.
	PageIndexCatalogLeaf
	// PageOrderBranch marks an internal insertion order index node.
	PageOrderBranch
	// PageOrderLeaf marks an insertion order index leaf.
	PageOrderLeaf
	// PageSystemBranch marks an internal system record index node.
	PageSystemBranch
	// PageSystemLeaf marks a system record index leaf.
	PageSystemLeaf
	// PageSystemOverflow stores system record bytes that do not fit in a leaf.
	PageSystemOverflow
	// PageIndexBuildCatalogBranch marks an internal index build catalog node.
	PageIndexBuildCatalogBranch
	// PageIndexBuildCatalogLeaf marks an index build catalog leaf.
	PageIndexBuildCatalogLeaf
)

// Meta describes a published database version and its storage bounds.
type Meta struct {
	DatabaseID             [16]byte
	Generation             uint64
	CommitSequence         uint64
	RootPage               uint64
	PhysicalPageCount      uint64
	OldestRetainedSequence uint64
	RequiredFeatures       uint64
	OptionalFeatures       uint64
}

// MetaEnvelope holds the header fields used to identify a database file format.
//
// These fields retain their byte positions across revisions and can be read
// from a checksum-valid future revision without interpreting its page graph.
type MetaEnvelope struct {
	Revision          uint16
	HeaderSize        uint16
	PageSize          uint32
	DatabaseID        [16]byte
	Generation        uint64
	CommitSequence    uint64
	RootPage          uint64
	PhysicalPageCount uint64
	RequiredFeatures  uint64
	OptionalFeatures  uint64
}

// Page holds a data page header and its payload.
type Page struct {
	Type         PageType
	Flags        uint8
	ID           uint64
	Generation   uint64
	BornSequence uint64
	ItemCount    uint32
	Link         uint64
	Payload      []byte
}

// DatabaseRoot locates the stored trees belonging to one committed database version.
type DatabaseRoot struct {
	CommitSequence         uint64
	CatalogRoot            uint64
	CommitLogRoot          uint64
	FreeSpaceRoot          uint64
	OldestRetainedSequence uint64
	CatalogGeneration      uint64
	DocumentCount          uint64
	CollectionCount        uint64
	IndexBuildCatalogRoot  uint64
}

// EncodeMeta validates and encodes database metadata with its checksum.
func EncodeMeta(meta Meta) ([]byte, error) {
	if unsupported := meta.RequiredFeatures &^ SupportedRequiredFeatures; unsupported != 0 {
		return nil, fmt.Errorf("%w: 0x%x", ErrUnsupportedFeature, unsupported)
	}
	if !validMeta(meta) {
		return nil, ErrCorrupt
	}
	page := make([]byte, PageSize)
	copy(page[:8], metaMagic[:])
	binary.LittleEndian.PutUint16(page[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(page[10:12], MetaHeaderSize)
	binary.LittleEndian.PutUint32(page[12:16], PageSize)
	copy(page[16:32], meta.DatabaseID[:])
	binary.LittleEndian.PutUint64(page[32:40], meta.Generation)
	binary.LittleEndian.PutUint64(page[40:48], meta.CommitSequence)
	binary.LittleEndian.PutUint64(page[48:56], meta.RootPage)
	binary.LittleEndian.PutUint64(page[56:64], meta.PhysicalPageCount)
	binary.LittleEndian.PutUint64(page[64:72], meta.OldestRetainedSequence)
	binary.LittleEndian.PutUint64(page[72:80], meta.RequiredFeatures)
	binary.LittleEndian.PutUint64(page[80:88], meta.OptionalFeatures)
	checksum := metaChecksum(page)
	copy(page[224:256], checksum[:])
	return page, nil
}

// DecodeMeta validates and reads metadata in the supported format.
func DecodeMeta(page []byte) (Meta, error) {
	// The magic, full-page SHA-256 envelope and checksum location are stable
	// across future  revisions. Validate that envelope before classifying a
	// version mismatch, so a torn version field remains ordinary corruption and
	// can safely fall back to the other Meta slot.
	envelope, err := InspectMetaEnvelope(page)
	if err != nil {
		return Meta{}, err
	}
	if envelope.Revision != FormatVersion {
		return Meta{}, fmt.Errorf("%w: found %d, supports %d", ErrUnsupportedFormat, envelope.Revision, FormatVersion)
	}
	if envelope.HeaderSize != MetaHeaderSize || envelope.PageSize != PageSize || !allZero(page[88:224]) || !allZero(page[256:]) {
		return Meta{}, ErrCorrupt
	}
	meta := Meta{
		Generation:             binary.LittleEndian.Uint64(page[32:40]),
		CommitSequence:         binary.LittleEndian.Uint64(page[40:48]),
		RootPage:               binary.LittleEndian.Uint64(page[48:56]),
		PhysicalPageCount:      binary.LittleEndian.Uint64(page[56:64]),
		OldestRetainedSequence: binary.LittleEndian.Uint64(page[64:72]),
		RequiredFeatures:       binary.LittleEndian.Uint64(page[72:80]),
		OptionalFeatures:       binary.LittleEndian.Uint64(page[80:88]),
	}
	copy(meta.DatabaseID[:], page[16:32])
	if unsupported := meta.RequiredFeatures &^ SupportedRequiredFeatures; unsupported != 0 {
		return Meta{}, fmt.Errorf("%w: 0x%x", ErrUnsupportedFeature, unsupported)
	}
	if !validMeta(meta) {
		return Meta{}, ErrCorrupt
	}
	return meta, nil
}

// InspectMetaEnvelope reads database header fields after checking the file signature and checksum.
//
// It can read these fields even when the revision is newer than this binary. Callers must not treat it as validation of the referenced page graph.
func InspectMetaEnvelope(page []byte) (MetaEnvelope, error) {
	if len(page) != PageSize || string(page[:8]) != string(metaMagic[:]) {
		return MetaEnvelope{}, ErrCorrupt
	}
	want := metaChecksum(page)
	if !equalBytes(page[224:256], want[:]) {
		return MetaEnvelope{}, ErrCorrupt
	}
	envelope := MetaEnvelope{
		Revision: binary.LittleEndian.Uint16(page[8:10]), HeaderSize: binary.LittleEndian.Uint16(page[10:12]),
		PageSize: binary.LittleEndian.Uint32(page[12:16]), Generation: binary.LittleEndian.Uint64(page[32:40]),
		CommitSequence: binary.LittleEndian.Uint64(page[40:48]), RootPage: binary.LittleEndian.Uint64(page[48:56]),
		PhysicalPageCount: binary.LittleEndian.Uint64(page[56:64]), RequiredFeatures: binary.LittleEndian.Uint64(page[72:80]),
		OptionalFeatures: binary.LittleEndian.Uint64(page[80:88]),
	}
	copy(envelope.DatabaseID[:], page[16:32])
	return envelope, nil
}

// EncodePage validates and encodes a data page with its checksum.
func EncodePage(value Page) ([]byte, error) {
	if value.Type < PageDatabaseRoot || value.Type > PageIndexBuildCatalogLeaf || value.ID < 2 ||
		value.Generation == 0 || len(value.Payload) > PageSize-PageHeaderSize {
		return nil, ErrCorrupt
	}
	page := make([]byte, PageSize)
	copy(page[:8], pageMagic[:])
	binary.LittleEndian.PutUint16(page[8:10], FormatVersion)
	page[10], page[11] = byte(value.Type), value.Flags
	binary.LittleEndian.PutUint16(page[12:14], PageHeaderSize)
	binary.LittleEndian.PutUint64(page[16:24], value.ID)
	binary.LittleEndian.PutUint64(page[24:32], value.Generation)
	binary.LittleEndian.PutUint64(page[32:40], value.BornSequence)
	binary.LittleEndian.PutUint32(page[40:44], uint32(len(value.Payload)))
	binary.LittleEndian.PutUint32(page[44:48], value.ItemCount)
	binary.LittleEndian.PutUint64(page[48:56], value.Link)
	copy(page[PageHeaderSize:], value.Payload)
	binary.LittleEndian.PutUint32(page[56:60], pageChecksum(page))
	return page, nil
}

// DecodePage validates a data page and copies its payload.
func DecodePage(page []byte, expectedID uint64) (Page, error) {
	return decodePage(page, expectedID, true)
}

// decodePageView validates a data page while sharing its input payload bytes.
func decodePageView(page []byte, expectedID uint64) (Page, error) {
	return decodePage(page, expectedID, false)
}

// decodePage validates a data page and optionally copies its payload.
func decodePage(page []byte, expectedID uint64, copyPayload bool) (Page, error) {
	if len(page) != PageSize || string(page[:8]) != string(pageMagic[:]) ||
		binary.LittleEndian.Uint16(page[8:10]) != FormatVersion ||
		binary.LittleEndian.Uint16(page[12:14]) != PageHeaderSize ||
		binary.LittleEndian.Uint16(page[14:16]) != 0 || binary.LittleEndian.Uint32(page[60:64]) != 0 ||
		binary.LittleEndian.Uint64(page[16:24]) != expectedID || expectedID < 2 ||
		binary.LittleEndian.Uint32(page[56:60]) != pageChecksum(page) {
		return Page{}, ErrCorrupt
	}
	length := uint64(binary.LittleEndian.Uint32(page[40:44]))
	if length > PageSize-PageHeaderSize || !allZero(page[PageHeaderSize+length:]) {
		return Page{}, ErrCorrupt
	}
	payload := page[PageHeaderSize : PageHeaderSize+length]
	if copyPayload {
		payload = append([]byte(nil), payload...)
	}
	result := Page{
		Type:         PageType(page[10]),
		Flags:        page[11],
		ID:           expectedID,
		Generation:   binary.LittleEndian.Uint64(page[24:32]),
		BornSequence: binary.LittleEndian.Uint64(page[32:40]),
		ItemCount:    binary.LittleEndian.Uint32(page[44:48]),
		Link:         binary.LittleEndian.Uint64(page[48:56]),
		Payload:      payload,
	}
	if result.Type < PageDatabaseRoot || result.Type > PageIndexBuildCatalogLeaf || result.Generation == 0 {
		return Page{}, ErrCorrupt
	}
	return result, nil
}

// EncodeDatabaseRoot encodes database root pointers as a data page.
func EncodeDatabaseRoot(pageID, generation uint64, root DatabaseRoot) ([]byte, error) {
	if root.OldestRetainedSequence > root.CommitSequence ||
		(root.CommitSequence == 0 && (root.CatalogRoot != 0 || root.CommitLogRoot != 0 || root.FreeSpaceRoot != 0 || root.IndexBuildCatalogRoot != 0)) {
		return nil, ErrCorrupt
	}
	payload := make([]byte, 128)
	binary.LittleEndian.PutUint64(payload[0:8], root.CommitSequence)
	binary.LittleEndian.PutUint64(payload[8:16], root.CatalogRoot)
	binary.LittleEndian.PutUint64(payload[16:24], root.CommitLogRoot)
	binary.LittleEndian.PutUint64(payload[24:32], root.FreeSpaceRoot)
	binary.LittleEndian.PutUint64(payload[32:40], root.OldestRetainedSequence)
	binary.LittleEndian.PutUint64(payload[40:48], root.CatalogGeneration)
	binary.LittleEndian.PutUint64(payload[48:56], root.DocumentCount)
	binary.LittleEndian.PutUint64(payload[56:64], root.CollectionCount)
	binary.LittleEndian.PutUint64(payload[64:72], root.IndexBuildCatalogRoot)
	return EncodePage(Page{Type: PageDatabaseRoot, ID: pageID, Generation: generation, BornSequence: root.CommitSequence, ItemCount: 1, Payload: payload})
}

// DecodeDatabaseRoot validates and reads a database root page.
func DecodeDatabaseRoot(page []byte, expectedID uint64) (DatabaseRoot, Page, error) {
	decoded, err := DecodePage(page, expectedID)
	if err != nil || decoded.Type != PageDatabaseRoot || decoded.Flags != 0 || decoded.ItemCount != 1 || decoded.Link != 0 || len(decoded.Payload) != 128 || !allZero(decoded.Payload[72:]) {
		return DatabaseRoot{}, Page{}, ErrCorrupt
	}
	root := DatabaseRoot{
		CommitSequence:         binary.LittleEndian.Uint64(decoded.Payload[0:8]),
		CatalogRoot:            binary.LittleEndian.Uint64(decoded.Payload[8:16]),
		CommitLogRoot:          binary.LittleEndian.Uint64(decoded.Payload[16:24]),
		FreeSpaceRoot:          binary.LittleEndian.Uint64(decoded.Payload[24:32]),
		OldestRetainedSequence: binary.LittleEndian.Uint64(decoded.Payload[32:40]),
		CatalogGeneration:      binary.LittleEndian.Uint64(decoded.Payload[40:48]),
		DocumentCount:          binary.LittleEndian.Uint64(decoded.Payload[48:56]),
		CollectionCount:        binary.LittleEndian.Uint64(decoded.Payload[56:64]),
		IndexBuildCatalogRoot:  binary.LittleEndian.Uint64(decoded.Payload[64:72]),
	}
	if root.CommitSequence != decoded.BornSequence || root.OldestRetainedSequence > root.CommitSequence {
		return DatabaseRoot{}, Page{}, ErrCorrupt
	}
	return root, decoded, nil
}

// validMeta checks database identity, version bounds, and the root page location.
func validMeta(meta Meta) bool {
	if allZero(meta.DatabaseID[:]) || meta.Generation == 0 || meta.PhysicalPageCount < 2 ||
		meta.OldestRetainedSequence > meta.CommitSequence {
		return false
	}
	if meta.RootPage == 0 {
		return meta.CommitSequence == 0
	}
	return meta.RootPage >= 2 && meta.RootPage < meta.PhysicalPageCount && meta.CommitSequence > 0
}

// metaChecksum hashes a metadata page with its checksum field treated as zero.
func metaChecksum(page []byte) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write(page[:224])
	var zero [32]byte
	_, _ = hash.Write(zero[:])
	_, _ = hash.Write(page[256:])
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

// pageChecksum computes CRC-32C with the checksum field treated as zero.
func pageChecksum(page []byte) uint32 {
	checksum := crc32.Update(0, crcTable, page[:56])
	var zero [4]byte
	checksum = crc32.Update(checksum, crcTable, zero[:])
	return crc32.Update(checksum, crcTable, page[60:])
}

// allZero reports whether every byte is zero.
func allZero(value []byte) bool {
	var found byte
	for _, item := range value {
		found |= item
	}
	return found == 0
}

// equalBytes reports whether two byte slices have identical contents.
func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var different byte
	for index := range left {
		different |= left[index] ^ right[index]
	}
	return different == 0
}
