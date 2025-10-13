// internal/room/room.go
package room

import (
	"log"
	"sync"
)

type Room struct {
	name    string
	peers   map[string]*Peer
	peersMu sync.RWMutex
}

func NewRoom(name string) *Room {
	return &Room{
		name:  name,
		peers: map[string]*Peer{},
	}
}

// GetName возвращает имя комнаты
func (r *Room) GetName() string {
	return r.name
}

func (r *Room) AddPeer(p *Peer) {
	r.peersMu.Lock()
	defer r.peersMu.Unlock()
	r.peers[p.ID] = p
	p.setRoom(r)
	log.Printf("👤 Peer %s joined room %s", p.ID, r.name)
	log.Printf("📊 Room %s now has %d peers", r.name, len(r.peers))
}

func (r *Room) RemovePeer(p *Peer) {
	r.peersMu.Lock()
	defer r.peersMu.Unlock()
	delete(r.peers, p.ID)
	p.Close()
	log.Printf("👋 Peer %s left room %s", p.ID, r.name)
	log.Printf("📊 Room %s now has %d peers", r.name, len(r.peers))
}

// ForwardTrack перенаправляет трек от одного peer всем остальным
// (Эта функция сейчас не используется, т.к. логика в Peer.addTrackToOtherPeers)
func (r *Room) ForwardTrack(fromPeerID string, forward func(target *Peer) error) {
	r.peersMu.RLock()
	defer r.peersMu.RUnlock()

	for id, p := range r.peers {
		if id == fromPeerID {
			continue
		}
		if err := forward(p); err != nil {
			log.Printf("❌ forward to %s error: %v", id, err)
		}
	}
}

func (r *Room) GetPeers() map[string]*Peer {
	r.peersMu.RLock()
	defer r.peersMu.RUnlock()

	peers := make(map[string]*Peer)
	for k, v := range r.peers {
		peers[k] = v
	}
	return peers
}

func (r *Room) GetPeerIDs() []string {
	r.peersMu.RLock()
	defer r.peersMu.RUnlock()

	ids := make([]string, 0, len(r.peers))
	for id := range r.peers {
		ids = append(ids, id)
	}
	return ids
}

// Close закрывает комнату и всех её пиров
func (r *Room) Close() {
	r.peersMu.Lock()
	defer r.peersMu.Unlock()

	// Закрываем всех пиров в комнате
	for _, peer := range r.peers {
		peer.Close()
	}

	// Очищаем мапу пиров
	r.peers = make(map[string]*Peer)
	log.Printf("🗑️  Room %s closed", r.name)
}
