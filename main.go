package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ClickEvent struct {
	EventID   string            `json:"event_id"`
	UserID    string            `json:"user_id"`
	EventType string            `json:"event_type"`
	Timestamp int64             `json:"timestamp"`
	Metadata  map[string]string `json:"metadata"`
}


type Server struct {
	Queue chan ClickEvent
	Wg    sync.WaitGroup
}


func (s *Server) worker(id int) {
	
	defer s.Wg.Done()
	fmt.Printf("Worker %d started\n", id)

	for event := range s.Queue {
		fmt.Printf("👷 Worker %d processing event: %s\n", id, event.EventID)
		// Simulate a slight delay for writing to disk/processing
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Printf("Worker %d cleaned up and stopped\n", id)
}


func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event ClickEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	// Safety check: Don't allow writing to a closed channel
	select {
	case s.Queue <- event:
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"status": "accepted"}`))
	default:
		// If the channel is blocked or closed during shutdown
		http.Error(w, "Server shutting down", http.StatusServiceUnavailable)
	}
}

func main() {
	
	srv := &Server{
		Queue: make(chan ClickEvent, 1000),
	}

	numWorkers := 3
	for i := 1; i <= numWorkers; i++ {
		srv.Wg.Add(1)
		go srv.worker(i)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", srv.handleIngest)

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux, 
	}

	//Run the web server in its own Goroutine so it doesn't block the main function
	go func() {
		fmt.Println("Ingestor server starting on :8080...")
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server ListenAndServe error: %v", err)
		}
	}()

	// SHUTDOWN ORCHESTRATION: Listen for system kill signals (like Ctrl+C)
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)

	<-stopSignal // Execution stops here until you press Ctrl+C or kill the process
	fmt.Println("\n Shutdown signal received! Starting graceful shutdown...")

	// Stop the web server from taking any new HTTP traffic
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		fmt.Printf("HTTP shutdown error: %v\n", err)
	}
	fmt.Println("HTTP server stopped. No more incoming requests allowed.")

	// Close the channel queue and tells the workers no more data is coming.
	close(srv.Queue)

	// Wait right here until all workers finish processing whatever data was left in the channel
	fmt.Println("Draining remaining events in queue. Waiting for workers...")
	srv.Wg.Wait()

	fmt.Println("Graceful shutdown complete. Zero data loss.")
}