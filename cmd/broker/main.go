package main

import (
	"flag"
	"fmt"
	"kafka-clone/internal/grpc"
	"kafka-clone/internal/topics"
	pb "kafka-clone/proto"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	grpcServer "google.golang.org/grpc"
)

func main() {
	var (
		port    = flag.Int("port", 9092, "gRPC server port")
		dataDir = flag.String("data-dir", "data", "Data directory for logs")
	)
	flag.Parse()

	// Create data directory
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Initialize components
	registry := topics.NewRegistry(*dataDir)
	handler := grpc.NewHandler(registry)

	// Setup gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpcServer.NewServer()
	pb.RegisterBrokerServer(s, handler)

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		log.Println("Shutting down...")
		s.GracefulStop()
		registry.Close()
	}()

	log.Printf("Kafka clone broker starting on port %d", *port)
	log.Printf("Data directory: %s", *dataDir)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
