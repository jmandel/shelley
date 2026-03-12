package server

import (
	"context"
	"sync"

	"github.com/coder/websocket"
)

type wsHubClient struct {
	outCh     chan<- workspaceWSMessage
	cancel    context.CancelFunc
	closeConn func(websocket.StatusCode, string)
}

type WSHub struct {
	mu      sync.Mutex
	clients map[string]*wsHubClient
}

func NewWSHub() *WSHub {
	return &WSHub{
		clients: make(map[string]*wsHubClient),
	}
}

func (h *WSHub) Add(id string, outCh chan<- workspaceWSMessage, cancel context.CancelFunc, closeConn func(websocket.StatusCode, string)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[id] = &wsHubClient{
		outCh:     outCh,
		cancel:    cancel,
		closeConn: closeConn,
	}
}

func (h *WSHub) Remove(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, id)
}

func (h *WSHub) Size() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

func (h *WSHub) Broadcast(msg workspaceWSMessage) {
	h.mu.Lock()
	stale := make([]*wsHubClient, 0)
	for id, client := range h.clients {
		select {
		case client.outCh <- msg:
		default:
			delete(h.clients, id)
			stale = append(stale, client)
		}
	}
	h.mu.Unlock()

	for _, client := range stale {
		if client.closeConn != nil {
			client.closeConn(websocket.StatusTryAgainLater, "outbound queue full")
		}
		client.cancel()
	}
}

func (h *WSHub) Close() {
	h.mu.Lock()
	clients := make([]*wsHubClient, 0, len(h.clients))
	for id, client := range h.clients {
		delete(h.clients, id)
		clients = append(clients, client)
	}
	h.mu.Unlock()

	for _, client := range clients {
		client.cancel()
	}
}
