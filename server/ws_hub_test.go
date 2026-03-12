package server

import (
	"testing"

	"github.com/coder/websocket"
)

func TestWSHubBroadcastClosesSlowSubscriber(t *testing.T) {
	hub := NewWSHub()

	outCh := make(chan workspaceWSMessage, 1)
	outCh <- workspaceWSMessage{Type: "occupied"}

	closed := false
	var closeCode websocket.StatusCode
	var closeReason string
	cancelled := false

	hub.Add("slow", outCh, func() {
		cancelled = true
	}, func(code websocket.StatusCode, reason string) {
		closed = true
		closeCode = code
		closeReason = reason
	})

	hub.Broadcast(workspaceWSMessage{Type: "next"})

	if !closed {
		t.Fatal("expected slow subscriber to be closed explicitly")
	}
	if closeCode != websocket.StatusTryAgainLater {
		t.Fatalf("expected close code %d, got %d", websocket.StatusTryAgainLater, closeCode)
	}
	if closeReason != "outbound queue full" {
		t.Fatalf("unexpected close reason %q", closeReason)
	}
	if !cancelled {
		t.Fatal("expected slow subscriber context to be cancelled")
	}
	if hub.Size() != 0 {
		t.Fatalf("expected slow subscriber to be removed, hub size=%d", hub.Size())
	}
}
