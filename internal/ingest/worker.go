package ingest

import (
	"fmt"
	"time"
)

func (s *Server) StartWorkers(numWorkers int) {
	for i := 1; i <= numWorkers; i++ {
		s.Wg.Add(1)
		go s.worker(i)
	}
}

func (s *Server) worker(id int) {
	defer s.Wg.Done()

	fmt.Printf("👷 Worker %d started\n", id)

	for event := range s.Queue {
		fmt.Printf("👷 Worker %d processing event: %s\n", id, event.EventID)
		time.Sleep(100 * time.Millisecond) // Simulate disk I/O or DB write
	}

	fmt.Printf("Worker %d cleaned up and stopped\n", id)
}