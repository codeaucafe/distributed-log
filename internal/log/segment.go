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
