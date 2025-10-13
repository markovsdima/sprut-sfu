package room

import (
	"log"
	"sync"
)

// Manager handles all active rooms
type Manager struct {
	rooms map[string]*Room
	mu    sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		rooms: make(map[string]*Room),
	}
}

// GetOrCreate returns existing room or creates a new one
func (m *Manager) GetOrCreate(roomID string) *Room {
	m.mu.Lock()
	defer m.mu.Unlock()

	if room, exists := m.rooms[roomID]; exists {
		return room
	}

	room := NewRoom(roomID)
	m.rooms[roomID] = room

	log.Printf("🏠 Room %s created", roomID)

	return room
}

func (m *Manager) Get(roomID string) *Room {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rooms[roomID]
}

// Remove deletes a room when all participants leave
func (m *Manager) Remove(roomID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.rooms, roomID)
	log.Printf("🗑️  Room %s removed", roomID)
}

func (m *Manager) Shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, room := range m.rooms {
		room.Close()
	}

	m.rooms = make(map[string]*Room)
	log.Println("All rooms closed")
}

// GetStats returns peer count per room for monitoring
func (m *Manager) GetStats() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]int)
	for roomID, room := range m.rooms {
		stats[roomID] = len(room.GetPeerIDs())
	}
	return stats
}
