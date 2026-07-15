package ingest

import (
	"fmt"
	"log"
	"os"
	"time"
)

func (s *Server) StartJanitor() {
	s.DlqWg.Add(1)
	go func() {
		defer s.DlqWg.Done()
		fmt.Println("Janitor started listening on the DLQ")

		file, err := os.OpenFile("failed_events.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Janitor failed to open log file: %v", err)
		}
		defer file.Close()

		for event := range s.DLQ {
			// Janitor logs the structurally invalid event
			errorMsg := fmt.Sprintf("FAILED EVENT - Missing Data: %+v\n", event)
			file.WriteString(errorMsg)
			fmt.Printf("Janitor routed invalid event to log file\n")
		}
		fmt.Println("Janitor cleaned up and stopped")
	}()
}

func (s *Server) StartWorkers(numWorkers int) {
	for i := 1; i <= numWorkers; i++ {
		s.Wg.Add(1)
		go s.worker(i)
	}
}

func (s *Server) worker(id int) {
	defer s.Wg.Done()
	fmt.Printf("Worker %d started\n", id)

	for event := range s.Queue {
		time.Sleep(100 * time.Millisecond) // Simulate processing time

		// If the JSON parsed, but the sender forgot the ID fields, reject it.
		if event.EventID == "" || event.UserID == "" {
			fmt.Printf("Worker %d REJECTED event (Missing ID fields)\n", id)
			s.DLQ <- event
			continue
		}

		fmt.Printf("Worker %d successfully processed event: %s\n", id, event.EventID)
	}
	fmt.Printf("Worker %d stopped\n", id)
}