package log

import (
	"io"
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

// Close flushes any pending writes to disk, truncates the index file to its
// actual size, and releases all resources. Must be called to prevent resource
// leaks and ensure data durability.
func (i *index) Close() error {
	// Unmap flushes data and releases memory mapping
	if err := i.mmap.Unmap(); err != nil {
		return err
	}

	// Sync file metadata to disk
	if err := i.file.Sync(); err != nil {
		return err
	}

	// Truncate to actual size (not pre-allocated size)
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	// Close file descriptor
	return i.file.Close()
}

// Read returns the record offset and store position for the given index entry.
// Pass in = -1 to read the last entry. Returns io.EOF if index is empty or entry not found.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends an index entry mapping the record offset to its store position.
// Returns io.EOF if the index has reached its maximum capacity.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()

}
