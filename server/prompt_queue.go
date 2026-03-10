package server

import (
	"context"
	"errors"
	"sync"
	"time"
)

type PromptStatus string

const (
	PromptStatusAccepted  PromptStatus = "accepted"
	PromptStatusQueued    PromptStatus = "queued"
	PromptStatusStarted   PromptStatus = "started"
	PromptStatusCompleted PromptStatus = "completed"
	PromptStatusCancelled PromptStatus = "cancelled"
	PromptStatusFailed    PromptStatus = "failed"
)

var (
	ErrQueuedPromptNotFound       = errors.New("queued prompt not found")
	ErrQueuedPromptNotCancellable = errors.New("queued prompt is not cancellable")
	ErrQueuedPromptNotOwned       = errors.New("queued prompt is not owned by requester")
	ErrQueuedPromptInvalidMove    = errors.New("queued prompt move direction must be up, down, top, or bottom")
)

type QueuedPrompt struct {
	PromptID string
	Text     string
	SenderID string
	QueuedAt time.Time
	Status   PromptStatus
}

type PromptQueueSnapshot struct {
	Active  *QueuedPrompt
	Entries []QueuedPrompt
}

type PromptQueue struct {
	mu     sync.Mutex
	active *QueuedPrompt
	queue  []QueuedPrompt
	notify chan struct{}
}

func NewPromptQueue() *PromptQueue {
	return &PromptQueue{
		queue:  make([]QueuedPrompt, 0),
		notify: make(chan struct{}, 1),
	}
}

func (pq *PromptQueue) Enqueue(prompt QueuedPrompt) int {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	prompt.Status = PromptStatusQueued
	pq.queue = append(pq.queue, prompt)
	position := len(pq.queue)

	select {
	case pq.notify <- struct{}{}:
	default:
	}

	return position
}

func (pq *PromptQueue) WaitForNext(ctx context.Context) (QueuedPrompt, bool) {
	for {
		pq.mu.Lock()
		if pq.active == nil && len(pq.queue) > 0 {
			next := pq.queue[0]
			pq.queue = pq.queue[1:]
			next.Status = PromptStatusStarted
			pq.active = &next
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

func (pq *PromptQueue) CompleteActive(status PromptStatus) (QueuedPrompt, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.active == nil {
		return QueuedPrompt{}, false
	}
	completed := *pq.active
	completed.Status = status
	pq.active = nil

	select {
	case pq.notify <- struct{}{}:
	default:
	}

	return completed, true
}

func (pq *PromptQueue) Cancel(promptID, senderID string) (QueuedPrompt, int, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.active != nil && pq.active.PromptID == promptID {
		return QueuedPrompt{}, 0, ErrQueuedPromptNotCancellable
	}
	for i, prompt := range pq.queue {
		if prompt.PromptID != promptID {
			continue
		}
		_ = senderID
		removed := prompt
		removed.Status = PromptStatusCancelled
		pq.queue = append(pq.queue[:i], pq.queue[i+1:]...)
		return removed, i + 1, nil
	}
	return QueuedPrompt{}, 0, ErrQueuedPromptNotFound
}

func (pq *PromptQueue) CancelMine(senderID string) []QueuedPrompt {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if senderID == "" || len(pq.queue) == 0 {
		return nil
	}

	removed := make([]QueuedPrompt, 0)
	filtered := pq.queue[:0]
	for _, prompt := range pq.queue {
		if prompt.SenderID == senderID {
			prompt.Status = PromptStatusCancelled
			removed = append(removed, prompt)
			continue
		}
		filtered = append(filtered, prompt)
	}
	pq.queue = filtered
	return removed
}

func (pq *PromptQueue) Update(promptID, senderID, text string) (QueuedPrompt, int, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.active != nil && pq.active.PromptID == promptID {
		return QueuedPrompt{}, 0, ErrQueuedPromptNotCancellable
	}
	for i, prompt := range pq.queue {
		if prompt.PromptID != promptID {
			continue
		}
		_ = senderID
		prompt.Text = text
		pq.queue[i] = prompt
		return prompt, i + 1, nil
	}
	return QueuedPrompt{}, 0, ErrQueuedPromptNotFound
}

func (pq *PromptQueue) Move(promptID, senderID, direction string) (QueuedPrompt, int, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.active != nil && pq.active.PromptID == promptID {
		return QueuedPrompt{}, 0, ErrQueuedPromptNotCancellable
	}
	for i, prompt := range pq.queue {
		if prompt.PromptID != promptID {
			continue
		}
		_ = senderID

		target := i
		switch direction {
		case "up":
			if i > 0 {
				target = i - 1
			}
		case "down":
			if i < len(pq.queue)-1 {
				target = i + 1
			}
		case "top":
			target = 0
		case "bottom":
			target = len(pq.queue) - 1
		default:
			return QueuedPrompt{}, 0, ErrQueuedPromptInvalidMove
		}

		if target != i {
			moved := pq.queue[i]
			if target < i {
				copy(pq.queue[target+1:i+1], pq.queue[target:i])
			} else {
				copy(pq.queue[i:target], pq.queue[i+1:target+1])
			}
			pq.queue[target] = moved
		}
		return pq.queue[target], target + 1, nil
	}
	return QueuedPrompt{}, 0, ErrQueuedPromptNotFound
}

func (pq *PromptQueue) Snapshot() PromptQueueSnapshot {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	var active *QueuedPrompt
	if pq.active != nil {
		current := *pq.active
		active = &current
	}

	entries := make([]QueuedPrompt, len(pq.queue))
	copy(entries, pq.queue)
	return PromptQueueSnapshot{
		Active:  active,
		Entries: entries,
	}
}

func (pq *PromptQueue) ActivePromptID() string {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.active == nil {
		return ""
	}
	return pq.active.PromptID
}

func (pq *PromptQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.queue)
}
