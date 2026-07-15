package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

func main() {
	totalRequests := 5000
	concurrentWorkers := 50

	fmt.Printf("🔫 Starting Simulator: %d total requests across %d concurrent workers...\n", totalRequests, concurrentWorkers)

	var wg sync.WaitGroup
	jobs := make(chan int, totalRequests)

	
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
	}
	client := &http.Client{
		Timeout:   2 * time.Second,
		Transport: transport,
	}

	for w := 1; w <= concurrentWorkers; w++ {
		wg.Add(1)
		
		go func(workerID int) {
			defer wg.Done()
			
			for i := range jobs {
				var payload []byte

				// 20% chance of a bad payload
				if rand.Intn(100) < 20 {
					payload = []byte(`{"event_type": "click", "timestamp": 1718023455}`) 
				} else {
					payload = []byte(fmt.Sprintf(`{"event_id": "EVT-%d", "user_id": "USR-99", "event_type": "click", "timestamp": 1718023455}`, i))
				}

				req, _ := http.NewRequest("POST", "http://localhost:8080/ingest", bytes.NewBuffer(payload))
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if err == nil {
					resp.Body.Close()
				}
			}
		}(w)
	}

	for i := 1; i <= totalRequests; i++ {
		jobs <- i
	}
	close(jobs)

	wg.Wait()
	fmt.Println("🏁 Simulation complete. All payloads fired.")
}