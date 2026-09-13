package orchestrator

import (
	"sync"
	"time"
)

// EventType represents the category of execution lifecycle events.
type EventType string

const (
	EventRunStarted          EventType = "RUN_STARTED"
	EventTaskStarted         EventType = "TASK_STARTED"
	EventTaskWaitingApproval EventType = "TASK_WAITING_APPROVAL"
	EventTaskCompleted       EventType = "TASK_COMPLETED"
	EventTaskFailed          EventType = "TASK_FAILED"
	EventRunFinished         EventType = "RUN_FINISHED"
)

// Event captures real-time telemetry emitted during workflow execution.
type Event struct {
	RunID     string    `json:"run_id"`
	TaskID    string    `json:"task_id,omitempty"`
	Type      EventType `json:"type"`
	Payload   any       `json:"payload,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// EventEmitter provides a thread-safe pub/sub event broadcaster.
type EventEmitter struct {
	mu     sync.RWMutex
	subs   map[uint64]chan Event
	nextID uint64
}

// NewEventEmitter initializes an EventEmitter.
func NewEventEmitter() *EventEmitter {
	return &EventEmitter{
		subs: make(map[uint64]chan Event),
	}
}

// Subscribe registers a new subscriber channel with the given buffer size.
// Returns a read-only channel and an unsubscribe cleanup function.
func (e *EventEmitter) Subscribe(bufferSize int) (<-chan Event, func()) {
	if bufferSize <= 0 {
		bufferSize = 64
	}
	ch := make(chan Event, bufferSize)

	e.mu.Lock()
	e.nextID++
	id := e.nextID
	e.subs[id] = ch
	e.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			e.mu.Lock()
			delete(e.subs, id)
			close(ch)
			e.mu.Unlock()
		})
	}

	return ch, unsubscribe
}

// Publish broadcasts an event to all active subscribers in a non-blocking manner.
func (e *EventEmitter) Publish(event Event) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, ch := range e.subs {
		select {
		case ch <- event:
		default:
			// Buffer full: drop event to avoid deadlocking worker goroutines
		}
	}
}
