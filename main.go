package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
}


func (s *Server) worker() {
	for event := range s.Queue {
		fmt.Printf("👷 Worker pulled event %s from the queue\n", event.EventID)
	}
}


func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event ClickEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Bad request: Invalid JSON", http.StatusBadRequest)
		return
	}
	//handing the event to the channel
	s.Queue <- event

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "accepted"}`))
}

func main() {
	
	srv := &Server{
		Queue: make(chan ClickEvent, 1000),
	}

	go srv.worker()

	http.HandleFunc("/ingest", srv.handleIngest)

	fmt.Println("Ingestor server starting on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}