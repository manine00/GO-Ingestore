package ingest

type ClickEvent struct {
	EventID   string `json:"event_id"`
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	Timestamp int64  `json:"timestamp"`
}