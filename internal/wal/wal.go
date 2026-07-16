package wal

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const headerSize = 64

var magic = [8]byte{'M', 'E', 'L', 'D', 'W', 'A', 'L', '1'}
var (
	ErrCorrupt          = errors.New("meldbase wal: corrupt log")
	ErrRecoveryRequired = errors.New("meldbase wal: recovery required")
)

type OpenOptions struct{ RequireClean bool }

type Record struct {
	Token   uint64
	Payload []byte
}
type Log struct {
	file      *os.File
	lastToken uint64
	offset    int64
}

type OpenReport struct {
	RecordsReplayed uint64
	BytesDiscarded  uint64
}

// Size reports the current durable log length. Callers serialize Append and
// Reset; Meldbase uses it only while holding the database lock.
func (l *Log) Size() int64 {
	if l == nil {
		return 0
	}
	return l.offset
}

func Open(path string, checkpointToken uint64) (*Log, []Record, error) {
	log, records, _, err := OpenWithReport(path, checkpointToken)
	return log, records, err
}

func OpenWithReport(path string, checkpointToken uint64) (*Log, []Record, OpenReport, error) {
	return OpenWithOptions(path, checkpointToken, OpenOptions{})
}

func OpenWithOptions(path string, checkpointToken uint64, options OpenOptions) (*Log, []Record, OpenReport, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, OpenReport{}, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, OpenReport{}, err
	}
	records, offset, last, partial, err := scan(file, checkpointToken)
	if err != nil {
		file.Close()
		return nil, nil, OpenReport{}, err
	}
	if options.RequireClean && (partial || len(records) != 0) {
		file.Close()
		return nil, nil, OpenReport{}, ErrRecoveryRequired
	}
	if partial {
		if err := file.Truncate(offset); err != nil {
			file.Close()
			return nil, nil, OpenReport{}, err
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, nil, OpenReport{}, err
		}
	}
	return &Log{file: file, lastToken: last, offset: offset}, records, OpenReport{
		RecordsReplayed: uint64(len(records)), BytesDiscarded: uint64(info.Size() - offset),
	}, nil
}

func (l *Log) Append(token uint64, payload []byte) error {
	if l == nil || l.file == nil {
		return errors.New("WAL is closed")
	}
	if token <= l.lastToken {
		return errors.New("WAL token did not increase")
	}
	if len(payload) > 64<<20 {
		return errors.New("WAL record too large")
	}
	header := make([]byte, headerSize)
	copy(header[:8], magic[:])
	binary.LittleEndian.PutUint16(header[8:10], 1)
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(payload)))
	binary.LittleEndian.PutUint64(header[16:24], token)
	checksum := sha256.Sum256(append(append([]byte(nil), header[:24]...), payload...))
	copy(header[24:56], checksum[:])
	if _, err := l.file.WriteAt(header, l.offset); err != nil {
		return err
	}
	if _, err := l.file.WriteAt(payload, l.offset+headerSize); err != nil {
		return err
	}
	if err := l.file.Sync(); err != nil {
		return err
	}
	l.offset += int64(headerSize + len(payload))
	l.lastToken = token
	return nil
}

func (l *Log) Reset(checkpointToken uint64) error {
	if l == nil || l.file == nil {
		return errors.New("WAL is closed")
	}
	if checkpointToken < l.lastToken {
		return errors.New("checkpoint token precedes WAL")
	}
	if err := l.file.Truncate(0); err != nil {
		return err
	}
	if err := l.file.Sync(); err != nil {
		return err
	}
	l.offset, l.lastToken = 0, checkpointToken
	return nil
}
func (l *Log) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func scan(file *os.File, base uint64) ([]Record, int64, uint64, bool, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, 0, base, false, err
	}
	size := info.Size()
	offset := int64(0)
	last, lastSeen := base, uint64(0)
	records := []Record{}
	for offset < size {
		if size-offset < headerSize {
			return records, offset, last, true, nil
		}
		header := make([]byte, headerSize)
		if _, err := file.ReadAt(header, offset); err != nil {
			return nil, offset, last, false, err
		}
		if string(header[:8]) != string(magic[:]) || binary.LittleEndian.Uint16(header[8:10]) != 1 {
			return nil, offset, last, false, fmt.Errorf("%w: invalid header at %d", ErrCorrupt, offset)
		}
		length := binary.LittleEndian.Uint32(header[12:16])
		if length > 64<<20 {
			return nil, offset, last, false, ErrCorrupt
		}
		end := offset + headerSize + int64(length)
		if end > size {
			return records, offset, last, true, nil
		}
		payload := make([]byte, length)
		if _, err := file.ReadAt(payload, offset+headerSize); err != nil && !errors.Is(err, io.EOF) {
			return nil, offset, last, false, err
		}
		checksum := sha256.Sum256(append(append([]byte(nil), header[:24]...), payload...))
		if !equal(checksum[:], header[24:56]) {
			return nil, offset, last, false, fmt.Errorf("%w: checksum at %d", ErrCorrupt, offset)
		}
		token := binary.LittleEndian.Uint64(header[16:24])
		if token <= lastSeen {
			return nil, offset, last, false, fmt.Errorf("%w: non-increasing token", ErrCorrupt)
		}
		lastSeen = token
		if token > base {
			records = append(records, Record{Token: token, Payload: payload})
		}
		if token > last {
			last = token
		}
		offset = end
	}
	return records, offset, last, false, nil
}

func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var difference byte
	for i := range a {
		difference |= a[i] ^ b[i]
	}
	return difference == 0
}
