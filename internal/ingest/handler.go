package ingest

import (
	"encoding/json"
	"net/http"
)

func (s *Server) HandleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event ClickEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	select {
	case s.Queue <- event:
		s.Metrics.Ingested.Add(1)
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"status": "accepted"}`))
	default:
		http.Error(w, "Server shutting down", http.StatusServiceUnavailable)
	}
}