package ingest

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

type Metrics struct {
	Ingested  atomic.Uint64 `json:"ingested"`
	Processed atomic.Uint64 `json:"processed"`
	Failed    atomic.Uint64 `json:"failed"`
}

func (s *Server) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	
	stats := map[string]interface{}{
		"ingested":        s.Metrics.Ingested.Load(),
		"processed":       s.Metrics.Processed.Load(),
		"failed":          s.Metrics.Failed.Load(),
		"current_queue":   len(s.Queue),
		"current_dlq_len": len(s.DLQ),
	}

	json.NewEncoder(w).Encode(stats)
}