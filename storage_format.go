package meldbase

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
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

const storageFormatPageSize = 16 * 1024

// StorageFormat identifies the on-disk engine family without opening or
// mutating the database. Unknown denotes a missing or zero-length path, not an
// unrecognized non-empty file.
type StorageFormat string

const (
	StorageFormatUnknown StorageFormat = ""
	StorageFormatV1      StorageFormat = "v1"
	StorageFormatV2      StorageFormat = "v2"
)

var (
	storageV1MetaMagic = [8]byte{'M', 'E', 'L', 'D', 'P', 'A', 'G', 'E'}
	storageV2MetaMagic = [8]byte{'M', 'E', 'L', 'D', 'M', 'T', '2', 0}
)

// DetectStorageFormat reads only the two fixed meta-page magic fields. The
// selected engine remains responsible for checksums and complete validation.
// A non-empty unknown or mixed-family file fails closed as ErrCorrupt.
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
	if info.Size() < 2*storageFormatPageSize {
		return StorageFormatUnknown, ErrCorrupt
	}
	foundV1, foundV2 := false, false
	for slot := 0; slot < 2; slot++ {
		var magic [8]byte
		if _, err := file.ReadAt(magic[:], int64(slot*storageFormatPageSize)); err != nil && !errors.Is(err, io.EOF) {
			return StorageFormatUnknown, err
		}
		switch magic {
		case storageV1MetaMagic:
			foundV1 = true
		case storageV2MetaMagic:
			foundV2 = true
		}
	}
	if foundV1 == foundV2 {
		return StorageFormatUnknown, ErrCorrupt
	}
	if foundV2 {
		return StorageFormatV2, nil
	}
	return StorageFormatV1, nil
}

// InspectStorageFormat validates stable Meta checksums and reports the newest
// negotiation envelope without locking, opening, migrating, or mutating the
// database. For checksum-valid future V2 revisions it still reports revision
// and feature bits while ReaderCompatible is false.
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
	pages := [2][]byte{make([]byte, storageFormatPageSize), make([]byte, storageFormatPageSize)}
	for slot := range pages {
		if _, err := file.ReadAt(pages[slot], int64(slot*storageFormatPageSize)); err != nil {
			return StorageFormatInfo{}, err
		}
	}
	if format == StorageFormatV1 {
		return inspectV1Format(pages)
	}
	return inspectV2Format(pages)
}

func inspectV2Format(pages [2][]byte) (StorageFormatInfo, error) {
	var envelopes [2]storagev2.MetaEnvelope
	valid := [2]bool{}
	for slot := range pages {
		envelope, err := storagev2.InspectMetaEnvelope(pages[slot])
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
	compatible := envelope.Revision == storagev2.FormatVersion && envelope.HeaderSize == storagev2.MetaHeaderSize &&
		envelope.PageSize == storagev2.PageSize && envelope.RequiredFeatures&^storagev2.SupportedRequiredFeatures == 0
	return StorageFormatInfo{
		Format: StorageFormatV2, Revision: envelope.Revision, Generation: envelope.Generation,
		CommitSequence: envelope.CommitSequence, PhysicalPageCount: envelope.PhysicalPageCount,
		RequiredFeatures: envelope.RequiredFeatures, OptionalFeatures: envelope.OptionalFeatures,
		DatabaseIDHex: hex.EncodeToString(envelope.DatabaseID[:]), ValidMetaSlots: validSlots, ReaderCompatible: compatible,
	}, nil
}

func inspectV1Format(pages [2][]byte) (StorageFormatInfo, error) {
	type v1Envelope struct {
		revision, payloadLength uint16
		generation, sequence    uint64
		databaseID              [16]byte
		current                 bool
	}
	var envelopes [2]v1Envelope
	valid := [2]bool{}
	for slot, page := range pages {
		if len(page) != storageFormatPageSize || !bytes.Equal(page[:8], storageV1MetaMagic[:]) || page[10] != 1 ||
			binary.LittleEndian.Uint64(page[12:20]) != uint64(slot) {
			continue
		}
		length := binary.LittleEndian.Uint32(page[44:48])
		if length > storageFormatPageSize-80 {
			continue
		}
		digest := sha256.New()
		_, _ = digest.Write(page[:48])
		_, _ = digest.Write(page[80 : 80+length])
		if !bytes.Equal(digest.Sum(nil), page[48:80]) {
			continue
		}
		envelope := v1Envelope{
			revision: binary.LittleEndian.Uint16(page[8:10]), payloadLength: uint16(length),
			generation: binary.LittleEndian.Uint64(page[20:28]), sequence: binary.LittleEndian.Uint64(page[28:36]),
		}
		if envelope.revision == 1 && length == 52 {
			copy(envelope.databaseID[:], page[80:96])
			envelope.current = binary.LittleEndian.Uint64(page[96:104]) == envelope.generation &&
				binary.LittleEndian.Uint64(page[116:124]) == envelope.sequence &&
				binary.LittleEndian.Uint64(page[124:132]) == storageFormatPageSize
		}
		envelopes[slot], valid[slot] = envelope, true
	}
	if !valid[0] && !valid[1] {
		return StorageFormatInfo{}, ErrCorrupt
	}
	if valid[0] && valid[1] && envelopes[0].current && envelopes[1].current && envelopes[0].databaseID != envelopes[1].databaseID {
		return StorageFormatInfo{}, ErrCorrupt
	}
	if valid[0] && valid[1] && envelopes[0].generation == envelopes[1].generation && envelopes[0] != envelopes[1] {
		return StorageFormatInfo{}, ErrCorrupt
	}
	selected := 0
	if !valid[0] || valid[1] && envelopes[1].generation > envelopes[0].generation {
		selected = 1
	}
	validSlots := 1
	if valid[0] && valid[1] {
		validSlots = 2
	}
	envelope := envelopes[selected]
	return StorageFormatInfo{
		Format: StorageFormatV1, Revision: envelope.revision, Generation: envelope.generation,
		CommitSequence: envelope.sequence, DatabaseIDHex: hex.EncodeToString(envelope.databaseID[:]),
		ValidMetaSlots: validSlots, ReaderCompatible: envelope.revision == 1 && envelope.current,
	}, nil
}
