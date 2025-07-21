package producers

import (
	"kafka-clone/internal/topics"
)

type Producer struct {
	registry *topics.Registry
}

func NewProducer(registry *topics.Registry) *Producer {
	return &Producer{
		registry: registry,
	}
}

func (p *Producer) Produce(topic string, partition int32, payload []byte) (int32, int64, error) {
	part, err := p.registry.GetPartition(topic, partition)
	if err != nil {
		return 0, 0, err
	}

	offset, err := part.Append(payload)
	if err != nil {
		return 0, 0, err
	}

	return partition, offset, nil
}
