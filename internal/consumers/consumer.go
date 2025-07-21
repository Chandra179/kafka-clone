package consumers

import (
	"kafka-clone/internal/logstore"
	"kafka-clone/internal/topics"
	"sync"
)

type ConsumerOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}

type Consumer struct {
	registry *topics.Registry
	offsets  map[string]int64 // key: "topic:partition"
	mutex    sync.RWMutex
}

func NewConsumer(registry *topics.Registry) *Consumer {
	return &Consumer{
		registry: registry,
		offsets:  make(map[string]int64),
	}
}

func (c *Consumer) Consume(topic string, partition int32, offset int64) (*logstore.LogEntry, error) {
	part, err := c.registry.GetPartition(topic, partition)
	if err != nil {
		return nil, err
	}

	entry, err := part.Read(offset)
	if err != nil {
		return nil, err
	}

	// Update consumer offset
	key := c.offsetKey(topic, partition)
	c.mutex.Lock()
	c.offsets[key] = offset + 1
	c.mutex.Unlock()

	return entry, nil
}

func (c *Consumer) GetOffset(topic string, partition int32) int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	key := c.offsetKey(topic, partition)
	return c.offsets[key]
}

func (c *Consumer) offsetKey(topic string, partition int32) string {
	return topic + ":" + string(rune(partition))
}
