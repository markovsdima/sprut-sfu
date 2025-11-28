package room

import (
	"fmt"
	"testing"
)

func TestRoomCreation(t *testing.T) {
	room := NewRoom("test-room")
	if room.GetName() != "test-room" {
		t.Errorf("Expected room name 'test-room', got '%s'", room.GetName())
	}
}

func TestRoomAddPeer(t *testing.T) {
	room := NewRoom("test-room")

	// Создаем mock peer (пока без реального WebRTC)
	peer := &Peer{
		ID:   "test-peer",
		Room: room,
	}

	room.AddPeer(peer)

	peers := room.GetPeers()
	if len(peers) != 1 {
		t.Errorf("Expected 1 peer, got %d", len(peers))
	}

	if peers["test-peer"] != peer {
		t.Error("Peer not found in room")
	}
}

func TestRoomRemovePeer(t *testing.T) {
	room := NewRoom("test-room")
	peer := &Peer{ID: "test-peer", Room: room}

	room.AddPeer(peer)
	room.RemovePeer(peer)

	peers := room.GetPeers()
	if len(peers) != 0 {
		t.Errorf("Expected 0 peers after removal, got %d", len(peers))
	}
}

func TestRoomRaceCondition(t *testing.T) {
	room := NewRoom("test")

	// 1000 горутин одновременно читают и пишут
	for i := 0; i < 1000; i++ {
		go func(id int) {
			room.AddPeer(&Peer{ID: fmt.Sprintf("peer-%d", id)})
			_ = room.GetPeers() // Чтение
		}(i)
	}

	// go test -race ./internal/room/
}
