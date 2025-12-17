package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/codeaucafe/distributed-log/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

// NewLog creates or restores a log in the specified directory.
//
// It applies default configuration values for any unset limits, then calls
// setup to restore existing segments from disk or create the initial segment.
// The directory must exist; NewLog does not create it.
func NewLog(dir string, c Config) (*Log, error) {
	// Apply defaults for unset config values
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

// Append writes a record to the log and returns its assigned offset.
//
// The record is written to the active segment. If the active segment reaches
// its capacity after the write, a new segment is created for subsequent writes.
// This method is safe for concurrent use.
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	// Roll to a new segment if the current one is full
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// Read retrieves the record at the given offset.
//
// It locates the segment containing the offset, then delegates to that
// segment's Read method. Returns an error if the offset doesn't exist.
// This method is safe for concurrent use.
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	for _, segment := range l.segments {
		// offset falls within this segment's range
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	return s.Read(off)

}

// setup restores the log from existing segment files on disk, or creates the
// first segment if the directory is empty.
//
// It reads all files in the log directory, extracts base offsets from filenames
// (e.g., "16.store" -> 16), and reopens each segment in ascending order. The
// last restored segment becomes the active segment for new writes.
func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	// Extract base offsets from filenames (e.g., "16.store" -> 16)
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })

	// Restore segments in order; skip duplicates since each segment has two files (.store, .index)
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		i++
	}

	// Create initial segment if no existing segments were found
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// newSegment creates a segment with the given base offset and makes it the
// active segment for writes.
//
// The new segment is appended to the segments slice, ensuring segments remain
// ordered by base offset. This is called both during setup (to restore existing
// segments) and during normal operation (when the active segment fills up).
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
