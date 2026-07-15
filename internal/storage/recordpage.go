package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	recordPageHeaderSize = 80
	recordSlotSize       = 12
	recordPageType       = 3
	indexPageType        = 5
	recordPageVersion    = 1
)

var (
	ErrNoSpace        = errors.New("meldbase storage: record page has no space")
	ErrStaleRecordID  = errors.New("meldbase storage: stale record id")
	ErrRecordTooLarge = errors.New("meldbase storage: record exceeds page capacity")
	recordPageMagic   = [8]byte{'M', 'E', 'L', 'D', 'R', 'E', 'C', 'D'}
)

// RecordID never aliases a reused slot: deleting a record advances the slot's
// generation before that slot can be allocated again.
type RecordID struct {
	Page       uint64
	Slot       uint16
	Generation uint32
}

type recordSlot struct {
	generation uint32
	used       bool
	data       []byte
}

// RecordPage is a slotted 16 KiB document page. Its binary form packs record
// bodies from the header upward and the fixed-size slot directory downward.
type RecordPage struct {
	pageID     uint64
	generation uint64
	lsn        uint64
	pageType   uint8
	slots      []recordSlot
}

func NewRecordPage(pageID uint64) *RecordPage {
	return NewRecordPageAt(pageID, 1, 0)
}

func NewRecordPageAt(pageID, generation, lsn uint64) *RecordPage {
	return newSlottedPageAt(pageID, generation, lsn, recordPageType)
}

func NewIndexPageAt(pageID, generation, lsn uint64) *RecordPage {
	return newSlottedPageAt(pageID, generation, lsn, indexPageType)
}

func newSlottedPageAt(pageID, generation, lsn uint64, pageType uint8) *RecordPage {
	if generation == 0 {
		generation = 1
	}
	return &RecordPage{pageID: pageID, generation: generation, lsn: lsn, pageType: pageType}
}

func (p *RecordPage) PageID() uint64     { return p.pageID }
func (p *RecordPage) Generation() uint64 { return p.generation }
func (p *RecordPage) LSN() uint64        { return p.lsn }
func (p *RecordPage) FreeBytes() int {
	return PageSize - recordPageHeaderSize - len(p.slots)*recordSlotSize - p.usedBytes()
}

func (p *RecordPage) Insert(record []byte) (RecordID, error) {
	if len(record) > PageSize-recordPageHeaderSize-recordSlotSize {
		return RecordID{}, ErrRecordTooLarge
	}
	for index := range p.slots {
		if p.slots[index].used {
			continue
		}
		if p.FreeBytes() < len(record) {
			return RecordID{}, ErrNoSpace
		}
		slot := &p.slots[index]
		if slot.generation == 0 {
			slot.generation = 1
		}
		slot.used = true
		slot.data = append([]byte(nil), record...)
		return RecordID{Page: p.pageID, Slot: uint16(index), Generation: slot.generation}, nil
	}
	if len(p.slots) >= math.MaxUint16 || p.FreeBytes() < len(record)+recordSlotSize {
		return RecordID{}, ErrNoSpace
	}
	slotGeneration := uint32(p.generation)
	if slotGeneration == 0 {
		slotGeneration = 1
	}
	p.slots = append(p.slots, recordSlot{generation: slotGeneration, used: true, data: append([]byte(nil), record...)})
	return RecordID{Page: p.pageID, Slot: uint16(len(p.slots) - 1), Generation: slotGeneration}, nil
}

func (p *RecordPage) Get(id RecordID) ([]byte, error) {
	slot, err := p.resolve(id)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), slot.data...), nil
}

func (p *RecordPage) Update(id RecordID, record []byte) error {
	slot, err := p.resolve(id)
	if err != nil {
		return err
	}
	if len(record) > PageSize-recordPageHeaderSize-recordSlotSize {
		return ErrRecordTooLarge
	}
	available := p.FreeBytes() + len(slot.data)
	if len(record) > available {
		return ErrNoSpace
	}
	slot.data = append(slot.data[:0], record...)
	return nil
}

func (p *RecordPage) Delete(id RecordID) error {
	slot, err := p.resolve(id)
	if err != nil {
		return err
	}
	if slot.generation == math.MaxUint32 {
		return fmt.Errorf("%w: slot generation exhausted", ErrStaleRecordID)
	}
	slot.used = false
	slot.data = nil
	slot.generation++
	return nil
}

func (p *RecordPage) MarshalBinary() ([]byte, error) {
	if p == nil || len(p.slots) > math.MaxUint16 || p.FreeBytes() < 0 {
		return nil, ErrCorrupt
	}
	page := make([]byte, PageSize)
	copy(page[:8], recordPageMagic[:])
	binary.LittleEndian.PutUint16(page[8:10], recordPageVersion)
	if p.pageType != recordPageType && p.pageType != indexPageType {
		return nil, ErrCorrupt
	}
	page[10] = p.pageType
	binary.LittleEndian.PutUint64(page[12:20], p.pageID)
	binary.LittleEndian.PutUint64(page[20:28], p.generation)
	binary.LittleEndian.PutUint64(page[28:36], p.lsn)
	binary.LittleEndian.PutUint16(page[36:38], uint16(len(p.slots)))
	offset := recordPageHeaderSize
	directoryStart := PageSize - len(p.slots)*recordSlotSize
	for index := range p.slots {
		slot := &p.slots[index]
		entry := page[PageSize-(index+1)*recordSlotSize : PageSize-index*recordSlotSize]
		binary.LittleEndian.PutUint32(entry[4:8], slot.generation)
		if !slot.used {
			continue
		}
		if len(slot.data) > math.MaxUint16 || offset+len(slot.data) > directoryStart {
			return nil, ErrCorrupt
		}
		binary.LittleEndian.PutUint16(entry[0:2], uint16(offset))
		binary.LittleEndian.PutUint16(entry[2:4], uint16(len(slot.data)))
		binary.LittleEndian.PutUint16(entry[8:10], 1)
		copy(page[offset:], slot.data)
		offset += len(slot.data)
	}
	binary.LittleEndian.PutUint16(page[38:40], uint16(offset))
	binary.LittleEndian.PutUint16(page[40:42], uint16(directoryStart))
	checksum := recordPageChecksum(page)
	copy(page[48:80], checksum[:])
	return page, nil
}

func DecodeRecordPage(page []byte, expectedPageID uint64) (*RecordPage, error) {
	return decodeSlottedPage(page, expectedPageID, recordPageType)
}

func DecodeIndexPage(page []byte, expectedPageID uint64) (*RecordPage, error) {
	return decodeSlottedPage(page, expectedPageID, indexPageType)
}

func decodeSlottedPage(page []byte, expectedPageID uint64, expectedType uint8) (*RecordPage, error) {
	if len(page) != PageSize || string(page[:8]) != string(recordPageMagic[:]) ||
		binary.LittleEndian.Uint16(page[8:10]) != recordPageVersion || page[10] != expectedType ||
		binary.LittleEndian.Uint64(page[12:20]) != expectedPageID {
		return nil, ErrCorrupt
	}
	want := append([]byte(nil), page[48:80]...)
	checksum := recordPageChecksum(page)
	if !equal8(want, checksum[:]) {
		return nil, ErrCorrupt
	}
	count := int(binary.LittleEndian.Uint16(page[36:38]))
	freeStart := int(binary.LittleEndian.Uint16(page[38:40]))
	freeEnd := int(binary.LittleEndian.Uint16(page[40:42]))
	directoryStart := PageSize - count*recordSlotSize
	if freeStart < recordPageHeaderSize || freeEnd != directoryStart || freeStart > freeEnd {
		return nil, ErrCorrupt
	}
	result := &RecordPage{
		pageID: expectedPageID, generation: binary.LittleEndian.Uint64(page[20:28]),
		lsn: binary.LittleEndian.Uint64(page[28:36]), pageType: expectedType, slots: make([]recordSlot, count),
	}
	if result.generation == 0 {
		return nil, ErrCorrupt
	}
	cursor := recordPageHeaderSize
	for index := range count {
		entry := page[PageSize-(index+1)*recordSlotSize : PageSize-index*recordSlotSize]
		offset := int(binary.LittleEndian.Uint16(entry[0:2]))
		length := int(binary.LittleEndian.Uint16(entry[2:4]))
		generation := binary.LittleEndian.Uint32(entry[4:8])
		flags := binary.LittleEndian.Uint16(entry[8:10])
		if generation == 0 || flags > 1 {
			return nil, ErrCorrupt
		}
		result.slots[index].generation = generation
		if flags == 0 {
			if offset != 0 || length != 0 {
				return nil, ErrCorrupt
			}
			continue
		}
		if offset != cursor || offset+length > freeStart {
			return nil, ErrCorrupt
		}
		result.slots[index].used = true
		result.slots[index].data = append([]byte(nil), page[offset:offset+length]...)
		cursor += length
	}
	if cursor != freeStart || result.FreeBytes() != freeEnd-freeStart {
		return nil, ErrCorrupt
	}
	return result, nil
}

func (p *RecordPage) resolve(id RecordID) (*recordSlot, error) {
	if id.Page != p.pageID || int(id.Slot) >= len(p.slots) {
		return nil, ErrStaleRecordID
	}
	slot := &p.slots[id.Slot]
	if !slot.used || slot.generation != id.Generation {
		return nil, ErrStaleRecordID
	}
	return slot, nil
}

func (p *RecordPage) usedBytes() int {
	total := 0
	for index := range p.slots {
		if p.slots[index].used {
			total += len(p.slots[index].data)
		}
	}
	return total
}

func recordPageChecksum(page []byte) [32]byte {
	copy := append([]byte(nil), page...)
	clear(copy[48:80])
	return sha256.Sum256(copy)
}
