package topics

import (
	"fmt"
	"kafka-clone/internal/logstore"
	"sync"
)

type Topic struct {
	Name       string
	Partitions []*logstore.Partition
}

type Registry struct {
	dataDir string
	topics  map[string]*Topic
	mutex   sync.RWMutex
}

func NewRegistry(dataDir string) *Registry {
	return &Registry{
		dataDir: dataDir,
		topics:  make(map[string]*Topic),
	}
}

func (r *Registry) CreateTopic(name string, numPartitions int32) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	topic := &Topic{
		Name:       name,
		Partitions: make([]*logstore.Partition, numPartitions),
	}

	for i := int32(0); i < numPartitions; i++ {
		partition, err := logstore.NewPartition(r.dataDir, name, i)
		if err != nil {
			// Clean up created partitions on error
			for j := int32(0); j < i; j++ {
				topic.Partitions[j].Close()
			}
			return err
		}
		topic.Partitions[i] = partition
	}

	r.topics[name] = topic
	return nil
}

func (r *Registry) GetTopic(name string) (*Topic, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	topic, exists := r.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", name)
	}

	return topic, nil
}

func (r *Registry) GetPartition(topic string, partitionID int32) (*logstore.Partition, error) {
	t, err := r.GetTopic(topic)
	if err != nil {
		return nil, err
	}

	if partitionID < 0 || partitionID >= int32(len(t.Partitions)) {
		return nil, fmt.Errorf("partition %d not found in topic %s", partitionID, topic)
	}

	return t.Partitions[partitionID], nil
}

func (r *Registry) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, topic := range r.topics {
		for _, partition := range topic.Partitions {
			partition.Close()
		}
	}

	return nil
}
