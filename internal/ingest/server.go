package ingest

import "sync"

type Server struct {
	Queue chan ClickEvent
	DLQ   chan ClickEvent
	Wg    *sync.WaitGroup
	DlqWg *sync.WaitGroup 
}

func NewServer(wg *sync.WaitGroup, dlqWg *sync.WaitGroup, queueSize int) *Server {
	return &Server{
		Queue: make(chan ClickEvent, queueSize),
		DLQ:   make(chan ClickEvent, queueSize),
		Wg:    wg,
		DlqWg: dlqWg,
	}
}