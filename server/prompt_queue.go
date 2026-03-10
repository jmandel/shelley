package server

import (
	"context"
	"sync"
	"time"
)

type QueuedPrompt struct {
	Text     string
	SenderID string
	QueuedAt time.Time
}

type PromptQueue struct {
	mu     sync.Mutex
	queue  []QueuedPrompt
	notify chan struct{}
}

func NewPromptQueue() *PromptQueue {
	return &PromptQueue{
		queue:  make([]QueuedPrompt, 0),
		notify: make(chan struct{}, 1),
	}
}

func (pq *PromptQueue) Enqueue(prompt QueuedPrompt) {
	pq.mu.Lock()
	pq.queue = append(pq.queue, prompt)
	pq.mu.Unlock()

	select {
	case pq.notify <- struct{}{}:
	default:
	}
}

func (pq *PromptQueue) WaitForNext(ctx context.Context) (QueuedPrompt, bool) {
	for {
		pq.mu.Lock()
		if len(pq.queue) > 0 {
			next := pq.queue[0]
			pq.queue = pq.queue[1:]
			pq.mu.Unlock()
			return next, true
		}
		pq.mu.Unlock()

		select {
		case <-ctx.Done():
			return QueuedPrompt{}, false
		case <-pq.notify:
		}
	}
}

func (pq *PromptQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.queue)
}
