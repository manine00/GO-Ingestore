package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go-ingestor/internal/ingest" 
)

func main() {
	var wg sync.WaitGroup
	var dlqWg sync.WaitGroup

	// Initialize our domain server
	srv := ingest.NewServer(&wg, &dlqWg, 1000)

	// Start the background worker pool
	srv.StartWorkers(3)
	srv.StartJanitor()

	// Set up the explicit HTTP Router
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", srv.HandleIngest)
	mux.HandleFunc("/metrics", srv.HandleMetrics)

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Run the web server asynchronously - might put in the server module and only call it
	go func() {
		fmt.Println("Ingestor server starting on :8080...")
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Shutdown Orchestration
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)

	<-stopSignal
	fmt.Println("\n Shutdown signal received! Starting graceful shutdown...")

	// Stop accepting new HTTP requests
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		fmt.Printf("HTTP shutdown error: %v\n", err)
	}
	fmt.Println("HTTP server stopped.")

	// Close queue to drain workers
	close(srv.Queue)
	fmt.Println("Draining remaining events in queue. Waiting for workers...")
	wg.Wait()

	close(srv.DLQ)
	fmt.Println("Waiting for Janitor to save final failed events...")
	dlqWg.Wait()

	fmt.Println("Graceful shutdown complete. Zero data loss.")
}