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

func handleIngest(w http.ResponseWriter, r *http.Request) {
	// Security check: Only allow POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event ClickEvent

	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		http.Error(w, "Bad request: Invalid JSON", http.StatusBadRequest)
		return
	}

	fmt.Printf("Received Event! ID: %s | User: %s | Type: %s\n", 
		event.EventID, event.UserID, event.EventType)

	w.WriteHeader(http.StatusAccepted) // HTTP 202
	w.Write([]byte(`{"status": "accepted"}`))
}

func main() {
	http.HandleFunc("/ingest", handleIngest)

	fmt.Println("Ingestor server starting on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}