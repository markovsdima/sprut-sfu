// internal/room/room.go
package room

import (
	"log"
	"sync"

	"github.com/pion/webrtc/v4"
)

type Room struct {
	name           string
	peers          map[string]*Peer
	peersMu        sync.RWMutex
	trackReceivers map[*webrtc.TrackLocalStaticRTP]*webrtc.RTPReceiver // Maps original receiver per local track
	trackSSRC      map[*webrtc.TrackLocalStaticRTP]uint32              // SSRC per local track
}

func NewRoom(name string) *Room {
	return &Room{
		name:           name,
		peers:          map[string]*Peer{},
		trackReceivers: make(map[*webrtc.TrackLocalStaticRTP]*webrtc.RTPReceiver),
		trackSSRC:      make(map[*webrtc.TrackLocalStaticRTP]uint32),
	}
}

func (r *Room) GetName() string {
	return r.name
}

func (r *Room) AddPeer(p *Peer) {
	r.peersMu.Lock()
	r.peers[p.ID] = p
	p.setRoom(r)
	r.peersMu.Unlock()

	log.Printf("👤 Peer %s joined room %s", p.ID, r.name)
	log.Printf("📊 Room %s now has %d peers", r.name, len(r.peers))
}

func (r *Room) RemovePeer(p *Peer) {
	r.peersMu.Lock()
	tracksToRemove := p.GetPublishedTracks()

	for _, track := range tracksToRemove {
		delete(r.trackReceivers, track)
		delete(r.trackSSRC, track)
	}

	delete(r.peers, p.ID)
	r.peersMu.Unlock()

	if len(tracksToRemove) > 0 {
		r.removeTracksFromOtherPeers(p.ID, tracksToRemove)
	}

	p.closeConnections()
}

// removeTracksFromOtherPeers removes leaving peer's tracks from all other subscribers
func (r *Room) removeTracksFromOtherPeers(leavingPeerID string, tracks []*webrtc.TrackLocalStaticRTP) {
	r.peersMu.RLock()
	otherPeers := make([]*Peer, 0, len(r.peers))
	for id, peer := range r.peers {
		if id != leavingPeerID {
			otherPeers = append(otherPeers, peer)
		}
	}
	r.peersMu.RUnlock()

	for _, otherPeer := range otherPeers {
		otherPeer.removeTracksFromSubscriber(tracks)
	}
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

func (r *Room) getPeerByID(peerID string) *Peer {
	r.peersMu.RLock()
	defer r.peersMu.RUnlock()
	return r.peers[peerID]
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
		peer.closeConnections()
	}

	// Clean peers map
	r.peers = make(map[string]*Peer)

	// Clean metadata maps
	r.trackReceivers = make(map[*webrtc.TrackLocalStaticRTP]*webrtc.RTPReceiver)
	r.trackSSRC = make(map[*webrtc.TrackLocalStaticRTP]uint32)

	log.Printf("🗑️  Room %s closed and all track metadata cleaned", r.name)
}
