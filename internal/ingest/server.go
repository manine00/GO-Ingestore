package ingest

import "sync"

type Server struct {
	Queue chan ClickEvent
	Wg    *sync.WaitGroup 
}

func NewServer(wg *sync.WaitGroup, queueSize int) *Server {
	return &Server{
		Queue: make(chan ClickEvent, queueSize),
		Wg:    wg,
	}
}