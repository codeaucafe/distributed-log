package log

import (
	"fmt"
	"os"
	"path"

	"google.golang.org/protobuf/proto"

	api "github.com/codeaucafe/distributed-log/api/v1"
)

type segment struct {
	store      *store
	index      *index
	baseOffset uint64
	nextOffset uint64
	config     Config
}

// newSegment creates a new segment with the given base offset in the specified directory. It initializes
// the store and index files according to the provided configuration. If the index already contains entries,
// it sets the next offset accordingly; otherwise, it starts from the base offset. If there is an error during
// initialization, it returns the error.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// Append adds a new record to the segment and returns the record's absolute offset.
//
// It assigns the current next offset to the record, serializes it using Protocol Buffers,
// and appends it to the store. It then writes an index entry mapping the relative offset
// (nextOffset - baseOffset) to the record's position in the store. The relative offset
// is used to keep index entries small since each segment tracks positions independently.
// Finally, it increments the next offset for subsequent appends.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	curr := s.nextOffset
	record.Offset = curr

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	if err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos); err != nil {
		return 0, err
	}

	s.nextOffset++

	return curr, nil
}

// Read retrieves a record from the segment at the given absolute offset.
//
// It first looks up the record's position in the store by reading the index
// using the relative offset (offset - baseOffset), then reads the raw bytes
// from the store and unmarshals them into a Record. Returns nil and an error
// if the offset is not found in the index or if reading/unmarshaling fails.
func (s *segment) Read(offset uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(offset - s.baseOffset))
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	if err := proto.Unmarshal(p, record); err != nil {
		return nil, err
	}
	return record, nil
}

// IsMaxed reports whether the segment has reached its capacity.
//
// It checks both the store and index sizes against their configured limits
// because either can fill up first depending on the workload: writing a small
// number of large records will hit the store limit first, while writing many
// small records will hit the index limit first (since index entries are fixed-size).
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove closes the segment and permanently deletes its data from disk.
//
// It first closes the segment to release resources, then removes the index
// file followed by the store file. The index is removed first to ensure crash
// safety: if a failure occurs mid-removal, we're left with orphan store data
// (harmless) rather than an index pointing to a deleted store (dangerous).
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close persists any buffered data and releases resources held by the segment.
//
// It closes the index first (truncating the memory-mapped file to its actual
// size and syncing to disk), then closes the store (flushing buffered writes).
// The index is closed first to ensure it's safely persisted while the store
// is still accessible, maintaining consistency for crash recovery.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
