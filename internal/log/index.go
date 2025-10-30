package log

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

// Index entry format constants
// Each index entry maps a record offset (sequence number) to its position in the store file
const (
	offWidth uint64 = 4                   // Number of bytes for offset field (stored as uint32)
	posWidth uint64 = 8                   // Number of bytes for position field (stored as uint64)
	entWidth        = offWidth + posWidth // Total bytes per entry (12 bytes)
)

// index provides fast lookups from record offset to store position using memory-mapped I/O.
//
// Structure on disk:
//
//	Entry 0: [4-byte offset][8-byte position]
//	Entry 1: [4-byte offset][8-byte position]
//	Entry 2: [4-byte offset][8-byte position]
//	...
//
// Each entry is entWidth (12) bytes, so entry N is at file position N * 12.
// The offset field stores the record sequence number (0, 1, 2, ...) as a uint32.
// The position field stores where that record starts in the store file as a uint64.
type index struct {
	file *os.File  // Underlying file
	mmap mmap.MMap // Memory-mapped view of the file for fast access
	size uint64    // Current size of index in bytes (also the write position)
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = mmap.Map(idx.file, mmap.RDWR, 0); err != nil {
		return nil, err
	}

	return idx, nil
}
