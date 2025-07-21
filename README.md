# Kafka Clone

A simplified Kafka-like message broker implemented in Go with gRPC API.

## Quick Start

1. **Generate Protocol Buffers**:
```bash
protoc \
  --go_out=. \
  --go-grpc_out=. \
  proto/broker.proto
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

## Breakdown

### Kafka Cluster
```
├── Broker 1 (storage, routing, serving, replication, leader, coordination)
│   ├── topic-x partition 0 (LEADER)
│   │   ├── segment 00000000000000000000.log
│   │   ├── segment 00000000000000001000.log
│   │   └── segment 00000000000000002000.log
│   └── topic-x partition 1 (REPLICA)
│       ├── segment 00000000000000000000.log
│       └── segment 00000000000000001000.log
│   └── __consumer_offsets partition 3 (LEADER)
│       ├── group analytics-consumer → topic-x partition 0 → offset 1234
│       ├── group analytics-consumer → topic-x partition 1 → offset 8765
│       └── group billing-consumer   → topic-x partition 0 → offset 1357
├── Broker 2
│   ├── topic-x partition 1 (LEADER)
│   │   ├── segment 00000000000000000000.log
│   │   ├── segment 00000000000000001000.log
│   │   └── segment 00000000000000002000.log
│   └── topic-x partition 2 (REPLICA)
│       └── segment 00000000000000000000.log
│   └── __consumer_offsets partition 4 (REPLICA)
├── Broker 3
│   ├── topic-x partition 2 (LEADER)
│   │   ├── segment 00000000000000000000.log
│   │   ├── segment 00000000000000001000.log
│   │   └── segment 00000000000000002000.log
│   └── topic-x partition 0 (REPLICA)
│       └── segment 00000000000000000000.log
│   └── __consumer_offsets partition 5 (REPLICA)

```

### Partition segment
```
topic-x-0/  
├── 00000000000000000000.log        ← holds actual message data  
├── 00000000000000000000.index      ← sparse offset-to-byte-position map  
├── 00000000000000000000.timeindex  ← timestamp-to-offset map  
├── 00000000000000001000.log        ← next segment when threshold reached  
...

```