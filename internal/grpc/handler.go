package grpc

import (
	"context"
	"io"
	"kafka-clone/internal/consumers"
	"kafka-clone/internal/producers"
	"kafka-clone/internal/topics"
	pb "kafka-clone/proto"
)

type Handler struct {
	pb.UnimplementedBrokerServer
	registry *topics.Registry
	producer *producers.Producer
	consumer *consumers.Consumer
}

func NewHandler(registry *topics.Registry) *Handler {
	return &Handler{
		registry: registry,
		producer: producers.NewProducer(registry),
		consumer: consumers.NewConsumer(registry),
	}
}

func (h *Handler) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	err := h.registry.CreateTopic(req.Topic, req.Partitions)
	if err != nil {
		return &pb.CreateTopicResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.CreateTopicResponse{
		Success: true,
	}, nil
}

func (h *Handler) Produce(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	partition, offset, err := h.producer.Produce(req.Topic, req.Partition, req.Payload)
	if err != nil {
		return &pb.ProduceResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ProduceResponse{
		Partition: partition,
		Offset:    offset,
	}, nil
}

func (h *Handler) Consume(req *pb.ConsumeRequest, stream pb.Broker_ConsumeServer) error {
	currentOffset := req.Offset

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
			entry, err := h.consumer.Consume(req.Topic, req.Partition, currentOffset)
			if err != nil {
				// If no more messages, wait and retry
				if err.Error() == io.EOF.Error() {
					continue
				}

				return stream.Send(&pb.ConsumeResponse{
					Error: err.Error(),
				})
			}

			err = stream.Send(&pb.ConsumeResponse{
				Offset:  entry.Offset,
				Payload: entry.Payload,
			})
			if err != nil {
				return err
			}

			currentOffset = entry.Offset + 1
		}
	}
}
