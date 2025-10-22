package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

// store is a low-level append-only log file wrapper that provides thread-safe
// write operations with buffered I/O. Each record written to the store is
// prefixed with an 8-byte length header (uint64) followed by the raw data bytes.
//
// The store maintains the current file size to track append positions and uses
// a mutex to ensure concurrent operations are serialized.
type store struct {
	*os.File
	mu   sync.Mutex    // protects concurrent access to the store
	buf  *bufio.Writer // buffers writes for better I/O performance
	size uint64        // current file size in bytes (end position)
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append writes a record to the store using a length-prefixed format.
// The record format is: [8-byte length header][raw data bytes].
//
// The length header is encoded as an uint64 in BigEndian byte order, indicating
// the number of bytes in the data payload that follows.
//
// Parameters:
//   - p: the raw data bytes to append to the store
//
// Returns:
//   - n: total bytes written (including 8-byte length prefix)
//   - pos: the file position where this record was written (use this to read it back)
//   - err: any error encountered during the write operation
//
// Append is thread-safe and uses a mutex to serialize concurrent writes.
// Data is written to a buffered writer for performance; call Close or Read
// to ensure data is flushed to disk.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	// Serialize access to ensure only one goroutine can append at a time
	s.mu.Lock()
	defer s.mu.Unlock()

	// Capture the current file position before writing; captures pos WHERE we're about to write to
	// Caller can use this position to read the record back later with Read(pos)
	pos = s.size

	// Write the length prefix: convert len(p) to 8 bytes using BigEndian encoding
	// This allows readers to know exactly how many bytes to read for the data payload
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// Write the raw data bytes to the buffered writer
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// Calculate total bytes written and update the store's size tracker
	w += lenWidth // Add the 8-byte length prefix to the count
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// Read retrieves a record from the store at the specified position.
// It uses the length-prefixed format written by Append to determine how many
// bytes to read: [8-byte length header][raw data bytes].
//
// The read process:
//  1. Reads the 8-byte length prefix at the given position
//  2. Decodes it to determine the data payload size
//  3. Reads exactly that many bytes of data
//
// Parameters:
//   - pos: the file position where the record's length prefix starts
//
// Returns:
//   - []byte: the raw data bytes of the record
//   - error: any error encountered during the read operation
//
// Read is thread-safe and uses a mutex to serialize concurrent access.
// It flushes the write buffer before reading to ensure all recent writes
// are visible.
func (s *store) Read(pos uint64) ([]byte, error) {
	// Serialize access to ensure only one goroutine can read at a time
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush buffered writes to disk so we can read the most recent data
	// Without this, we might try to read data that's still in the write buffer
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Read the 8-byte length prefix to determine how much data to read
	// This buffer stays on the stack (doesn't escape) for performance
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Decode the length prefix from 8 bytes to an uint64 number
	// Then allocate a buffer of exactly that size for the data payload
	// This buffer escapes to heap (returned to caller) but is optimally sized
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()

}
