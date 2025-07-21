package logstore

import (
	"fmt"
	"path/filepath"
	"sync"
)

type Partition struct {
	id       int32
	dir      string
	segments []*Segment
	mutex    sync.RWMutex
}

func NewPartition(dataDir string, topic string, id int32) (*Partition, error) {
	dir := filepath.Join(dataDir, topic, fmt.Sprintf("partition%d", id))

	p := &Partition{
		id:  id,
		dir: dir,
	}

	segments, err := LoadSegments(dir)
	if err != nil {
		return nil, err
	}

	p.segments = segments

	// Create first segment if none exist
	if len(p.segments) == 0 {
		segment, err := NewSegment(dir, 0)
		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, segment)
	}

	return p, nil
}

func (p *Partition) Append(payload []byte) (int64, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	activeSegment := p.segments[len(p.segments)-1]

	// Roll segment if it's too large
	if activeSegment.Size() >= SegmentMaxBytes {
		newBaseOffset := activeSegment.NextOffset()
		newSegment, err := NewSegment(p.dir, newBaseOffset)
		if err != nil {
			return 0, err
		}
		p.segments = append(p.segments, newSegment)
		activeSegment = newSegment
	}

	entry := LogEntry{Payload: payload}
	if err := activeSegment.Append(entry); err != nil {
		return 0, err
	}

	return entry.Offset, nil
}

func (p *Partition) Read(offset int64) (*LogEntry, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Find segment containing the offset
	for _, segment := range p.segments {
		if offset >= segment.BaseOffset() && offset < segment.NextOffset() {
			return segment.Read(offset)
		}
	}

	return nil, fmt.Errorf("offset %d not found in partition %d", offset, p.id)
}

func (p *Partition) NextOffset() int64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if len(p.segments) == 0 {
		return 0
	}

	return p.segments[len(p.segments)-1].NextOffset()
}

func (p *Partition) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, segment := range p.segments {
		segment.Close()
	}

	return nil
}
