package database

import (
	"encoding/hex"
	"errors"
	"io/fs"
	"os"

	storage "github.com/crapthings/meldbase/internal/storage"
)

// StorageFormatInfo is a read-only negotiation view, not a full graph audit.
// ReaderCompatible says this binary understands the reported revision and all
// required feature bits; callers must still Open the database before use.
type StorageFormatInfo struct {
	Format            StorageFormat `json:"format"`
	Revision          uint16        `json:"revision"`
	Generation        uint64        `json:"generation"`
	CommitSequence    uint64        `json:"commitSequence"`
	PhysicalPageCount uint64        `json:"physicalPageCount,omitempty"`
	RequiredFeatures  uint64        `json:"requiredFeatures"`
	OptionalFeatures  uint64        `json:"optionalFeatures"`
	DatabaseIDHex     string        `json:"databaseIdHex,omitempty"`
	ValidMetaSlots    int           `json:"validMetaSlots"`
	ReaderCompatible  bool          `json:"readerCompatible"`
}

// StorageFormat identifies the sole supported on-disk engine. Unknown denotes
// a missing or zero-length path, not an unrecognized non-empty file.
type StorageFormat string

const (
	StorageFormatUnknown StorageFormat = ""
	StorageFormatCurrent StorageFormat = "current"
)

// DetectStorageFormat performs only enough inspection to distinguish a new
// path from the current database format. Old database files deliberately fail
// closed: this build contains no legacy reader or automatic migration path.
func DetectStorageFormat(path string) (StorageFormat, error) {
	if path == "" {
		return StorageFormatUnknown, errors.New("meldbase: empty database path")
	}
	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return StorageFormatUnknown, nil
	}
	if err != nil {
		return StorageFormatUnknown, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return StorageFormatUnknown, err
	}
	if info.Size() == 0 {
		return StorageFormatUnknown, nil
	}
	if info.Size() < 2*int64(storage.PageSize) {
		return StorageFormatUnknown, ErrCorrupt
	}
	var pages [2][]byte
	for slot := range pages {
		pages[slot] = make([]byte, storage.PageSize)
		if _, err := file.ReadAt(pages[slot], int64(slot)*int64(storage.PageSize)); err != nil {
			return StorageFormatUnknown, err
		}
	}
	if _, err := inspectCurrentFormat(pages); err != nil {
		return StorageFormatUnknown, err
	}
	return StorageFormatCurrent, nil
}

// InspectStorageFormat validates current Meta checksums and reports its newest
// readable envelope without opening the database for mutation.
func InspectStorageFormat(path string) (StorageFormatInfo, error) {
	format, err := DetectStorageFormat(path)
	if err != nil || format == StorageFormatUnknown {
		return StorageFormatInfo{Format: format}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return StorageFormatInfo{}, err
	}
	defer file.Close()
	var pages [2][]byte
	for slot := range pages {
		pages[slot] = make([]byte, storage.PageSize)
		if _, err := file.ReadAt(pages[slot], int64(slot)*int64(storage.PageSize)); err != nil {
			return StorageFormatInfo{}, err
		}
	}
	return inspectCurrentFormat(pages)
}

func inspectCurrentFormat(pages [2][]byte) (StorageFormatInfo, error) {
	var envelopes [2]storage.MetaEnvelope
	valid := [2]bool{}
	for slot := range pages {
		envelope, err := storage.InspectMetaEnvelope(pages[slot])
		if err == nil {
			envelopes[slot], valid[slot] = envelope, true
		}
	}
	if !valid[0] && !valid[1] {
		return StorageFormatInfo{}, ErrCorrupt
	}
	if valid[0] && valid[1] && envelopes[0].DatabaseID != envelopes[1].DatabaseID {
		return StorageFormatInfo{}, ErrCorrupt
	}
	selected := 0
	if !valid[0] || valid[1] && envelopes[1].Generation > envelopes[0].Generation {
		selected = 1
	}
	if valid[0] && valid[1] && envelopes[0].Generation == envelopes[1].Generation && envelopes[0] != envelopes[1] {
		return StorageFormatInfo{}, ErrCorrupt
	}
	envelope := envelopes[selected]
	validSlots := 1
	if valid[0] && valid[1] {
		validSlots = 2
	}
	compatible := envelope.Revision == storage.FormatVersion && envelope.HeaderSize == storage.MetaHeaderSize &&
		envelope.PageSize == storage.PageSize && envelope.RequiredFeatures&^storage.SupportedRequiredFeatures == 0
	return StorageFormatInfo{
		Format: StorageFormatCurrent, Revision: envelope.Revision, Generation: envelope.Generation,
		CommitSequence: envelope.CommitSequence, PhysicalPageCount: envelope.PhysicalPageCount,
		RequiredFeatures: envelope.RequiredFeatures, OptionalFeatures: envelope.OptionalFeatures,
		DatabaseIDHex: hex.EncodeToString(envelope.DatabaseID[:]), ValidMetaSlots: validSlots, ReaderCompatible: compatible,
	}, nil
}
