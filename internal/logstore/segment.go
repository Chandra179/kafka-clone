package logstore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	SegmentMaxBytes = 128 * 1024 * 1024 // 128 MB
	IndexInterval   = 4096              // Index every 4KB
)

type LogEntry struct {
	Offset  int64
	Payload []byte
}

type IndexEntry struct {
	RelativeOffset int32
	Position       int32
}

type Segment struct {
	baseOffset   int64
	logFile      *os.File
	indexFile    *os.File
	size         int64
	nextOffset   int64
	indexEntries []IndexEntry
	mutex        sync.RWMutex
}

func NewSegment(dir string, baseOffset int64) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		logFile.Close()
		return nil, err
	}

	stat, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, err
	}

	segment := &Segment{
		baseOffset: baseOffset,
		logFile:    logFile,
		indexFile:  indexFile,
		size:       stat.Size(),
		nextOffset: baseOffset,
	}

	if err := segment.recover(); err != nil {
		segment.Close()
		return nil, err
	}

	return segment, nil
}

func (s *Segment) recover() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Load index entries
	s.indexFile.Seek(0, io.SeekStart)
	for {
		var entry IndexEntry
		if err := binary.Read(s.indexFile, binary.BigEndian, &entry.RelativeOffset); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := binary.Read(s.indexFile, binary.BigEndian, &entry.Position); err != nil {
			return err
		}
		s.indexEntries = append(s.indexEntries, entry)
	}

	// Scan log file to find next offset
	s.logFile.Seek(0, io.SeekStart)
	var position int64

	for {
		var offset int64
		var length int32
		var crc uint32

		if err := binary.Read(s.logFile, binary.BigEndian, &offset); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if err := binary.Read(s.logFile, binary.BigEndian, &length); err != nil {
			return err
		}

		if err := binary.Read(s.logFile, binary.BigEndian, &crc); err != nil {
			return err
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(s.logFile, payload); err != nil {
			return err
		}

		// Verify CRC
		expectedCRC := crc32.ChecksumIEEE(payload)
		if crc != expectedCRC {
			// Truncate at this position to remove corrupted entry
			s.logFile.Truncate(position)
			break
		}

		s.nextOffset = offset + 1
		position, _ = s.logFile.Seek(0, io.SeekCurrent)
	}

	return nil
}

func (s *Segment) Append(entry LogEntry) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	entry.Offset = s.nextOffset

	// Calculate CRC
	crc := crc32.ChecksumIEEE(entry.Payload)

	// Write: [offset][length][crc][payload]
	position, _ := s.logFile.Seek(0, io.SeekEnd)

	if err := binary.Write(s.logFile, binary.BigEndian, entry.Offset); err != nil {
		return err
	}

	if err := binary.Write(s.logFile, binary.BigEndian, int32(len(entry.Payload))); err != nil {
		return err
	}

	if err := binary.Write(s.logFile, binary.BigEndian, crc); err != nil {
		return err
	}

	if _, err := s.logFile.Write(entry.Payload); err != nil {
		return err
	}

	s.size = position + 8 + 4 + 4 + int64(len(entry.Payload))

	// Update index if needed
	if len(s.indexEntries) == 0 || position-int64(s.indexEntries[len(s.indexEntries)-1].Position) >= IndexInterval {
		indexEntry := IndexEntry{
			RelativeOffset: int32(entry.Offset - s.baseOffset),
			Position:       int32(position),
		}

		if err := binary.Write(s.indexFile, binary.BigEndian, indexEntry.RelativeOffset); err != nil {
			return err
		}

		if err := binary.Write(s.indexFile, binary.BigEndian, indexEntry.Position); err != nil {
			return err
		}

		s.indexEntries = append(s.indexEntries, indexEntry)
	}

	s.nextOffset++
	return nil
}

func (s *Segment) Read(offset int64) (*LogEntry, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, fmt.Errorf("offset %d out of range [%d, %d)", offset, s.baseOffset, s.nextOffset)
	}

	position := s.findPosition(offset)
	s.logFile.Seek(position, io.SeekStart)

	// Scan from position to find exact offset
	for {
		var entryOffset int64
		var length int32
		var crc uint32

		if err := binary.Read(s.logFile, binary.BigEndian, &entryOffset); err != nil {
			return nil, err
		}

		if err := binary.Read(s.logFile, binary.BigEndian, &length); err != nil {
			return nil, err
		}

		if err := binary.Read(s.logFile, binary.BigEndian, &crc); err != nil {
			return nil, err
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(s.logFile, payload); err != nil {
			return nil, err
		}

		if entryOffset == offset {
			return &LogEntry{Offset: entryOffset, Payload: payload}, nil
		}

		if entryOffset > offset {
			return nil, fmt.Errorf("offset %d not found", offset)
		}
	}
}

func (s *Segment) findPosition(offset int64) int64 {
	relativeOffset := int32(offset - s.baseOffset)

	// Binary search in index
	idx := sort.Search(len(s.indexEntries), func(i int) bool {
		return s.indexEntries[i].RelativeOffset > relativeOffset
	})

	if idx == 0 {
		return 0
	}

	return int64(s.indexEntries[idx-1].Position)
}

func (s *Segment) Size() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.size
}

func (s *Segment) BaseOffset() int64 {
	return s.baseOffset
}

func (s *Segment) NextOffset() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.nextOffset
}

func (s *Segment) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.logFile != nil {
		s.logFile.Sync()
		s.logFile.Close()
	}

	if s.indexFile != nil {
		s.indexFile.Sync()
		s.indexFile.Close()
	}

	return nil
}

// LoadSegments loads all segments from a partition directory
func LoadSegments(dir string) ([]*Segment, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var baseOffsets []int64
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".log") {
			name := strings.TrimSuffix(file.Name(), ".log")
			offset, err := strconv.ParseInt(name, 10, 64)
			if err == nil {
				baseOffsets = append(baseOffsets, offset)
			}
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	var segments []*Segment
	for _, baseOffset := range baseOffsets {
		segment, err := NewSegment(dir, baseOffset)
		if err != nil {
			// Close previously opened segments
			for _, s := range segments {
				s.Close()
			}
			return nil, err
		}
		segments = append(segments, segment)
	}

	return segments, nil
}
