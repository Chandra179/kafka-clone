# Kafka Clone

A simplified Kafka-like message broker implemented in Go with gRPC API.

## Features

- **Topic and Partition Management**: Create topics with multiple partitions
- **Persistent Storage**: Segment-based log storage with automatic rolling
- **Producer API**: Append messages to partitions with offset tracking
- **Consumer API**: Read messages from any offset with streaming support
- **Crash Recovery**: Automatic recovery of segment metadata and offsets on startup
- **Index Files**: Sparse indexing for efficient offset lookups

## Architecture

### Storage Layer
- **Segments**: Each partition consists of multiple segment files
- **Log Files**: Store actual message data with format `[offset][length][crc][payload]`
- **Index Files**: Sparse index mapping relative offsets to byte positions
- **Automatic Rolling**: Create new segments when size exceeds 128MB

### API Layer
- **gRPC Service**: High-performance binary protocol
- **Streaming**: Consumer API supports streaming reads
- **Error Handling**: Comprehensive error responses

## Quick Start

1. **Generate Protocol Buffers**:
```bash
cd proto
protoc --go_out=. --go-grpc_out=. broker.proto
```

2. **Build and Run**:
```bash
go mod tidy
go run cmd/broker/main.go -port 9092 -data-dir ./data
```

3. **Example Usage** (using grpcurl):

Create a topic:
```bash
grpcurl -plaintext -d '{"topic":"test-topic","partitions":3}' localhost:9092 broker.Broker/CreateTopic
```

Produce a message:
```bash
grpcurl -plaintext -d '{"topic":"test-topic","partition":0,"payload":"SGVsbG8gV29ybGQ="}' localhost:9092 broker.Broker/Produce
```

Consume messages:
```bash
grpcurl -plaintext -d '{"topic":"test-topic","partition":0,"offset":0}' localhost:9092 broker.Broker/Consume
```

## Configuration

- **Port**: Default 9092, configurable via `-port` flag
- **Data Directory**: Default `./data`, configurable via `-data-dir` flag
- **Segment Size**: 128MB maximum per segment (configurable in code)
- **Index Interval**: Index entry every 4KB (configurable in code)

## File Structure

```
data/
└── {topic}/
    └── partition{ID}/
        ├── 00000000000000000000.log    # Log segment
        ├── 00000000000000000000.index  # Sparse index
        ├── 00000000000000001000.log    # Next segment
        └── 00000000000000001000.index
```

## Implementation Details

### Segment Rolling
- New segments created when active segment exceeds `SegmentMaxBytes`
- Base offset determines segment filename (20-digit zero-padded)
- Atomic operations ensure consistency during rolling

### Recovery Process
1. Scan partition directories for segment files
2. Load index entries from `.index` files
3. Validate log entries using CRC checksums
4. Truncate corrupted entries from the end
5. Set next append offset based on highest valid entry

### Consumer Offset Tracking
- In-memory offset tracking per consumer
- Automatic advancement after successful reads
- Support for seeking to arbitrary offsets

## Limitations

This is a simplified implementation with the following limitations:
- Single-node only (no replication)
- In-memory consumer offset storage (not persistent)
- No retention policies
- No compression
- Basic error handling

## Future Enhancements

- Multi-broker clustering
- Persistent consumer offset storage
- Message retention policies
- Compression support
- Authentication and authorization
- Metrics and monitoring
```

This implementation provides:

1. **Complete gRPC service** with CreateTopic, Produce, and Consume methods
2. **Segment-based storage** with automatic rolling at 128MB
3. **Sparse indexing** for efficient offset lookups
4. **CRC validation** for data integrity
5. **Crash recovery** that validates and repairs segments on startup
6. **In-memory structures** for topics, partitions, and consumer offsets
7. **Concurrent safety** with proper mutex usage
8. **Binary log format** with offset, length, CRC, and payload

The broker supports all Phase 1 requirements:
- ✅ Topic/partition creation
- ✅ Persistent writes to segment files
- ✅ Segment rolling on size threshold
- ✅ Consumer reads from any offset across segments
- ✅ In-memory consumer offset tracking
- ✅ Recovery of segment metadata and next offset at startup

To use this implementation:

1. Generate the protobuf files: `protoc --go_out=. --go-grpc_out=. proto/broker.proto`
2. Run `go mod tidy` to install dependencies
3. Start the broker: `go run cmd/broker/main.go`
4. Use grpcurl or write client code to interact with the broker

The implementation includes proper error handling, concurrent access protection, and follows Go best practices for a production-ready message broker foundation.