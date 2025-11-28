package room

import (
	"fmt"
	"log"
	"simple-sfu/internal/config"
	"simple-sfu/pkg/protocol"
	"sync"
	"time"

	"github.com/pion/rtcp" // For PLI/FIR packets
	"github.com/pion/webrtc/v4"
)

// TODO: почему без этой задержки iOS WebRTC не всегда добавляет видео-трек собеседника
const renegotiationDebounce = 550 * time.Millisecond

// SignalingClient interface avoids circular dependency with signaling package
type SignalingClient interface {
	SendNotification(method string, params interface{})
}

// Peer represents a call participant
type Peer struct {
	ID         string
	Room       *Room
	client     SignalingClient
	Publisher  *webrtc.PeerConnection // receives media from client
	Subscriber *webrtc.PeerConnection // sends all media from others to client (single, multiplexed)

	publishedTracks []*webrtc.TrackLocalStaticRTP
	config          *config.Config

	subMu       sync.Mutex
	publisherMu sync.RWMutex

	renegotiationMu      sync.Mutex
	pendingRenegotiation bool
	isNegotiating        bool
	subscriberICE        []webrtc.ICECandidateInit
	renegotiationTimer   *time.Timer

	cameraEnabled bool
	cameraMu      sync.RWMutex

	done     chan struct{}
	doneOnce sync.Once
}

func NewPeer(id string, client SignalingClient, cfg *config.Config) *Peer {
	return &Peer{
		ID:              id,
		client:          client,
		publishedTracks: make([]*webrtc.TrackLocalStaticRTP, 0),
		config:          cfg,
		subscriberICE:   make([]webrtc.ICECandidateInit, 0),
		cameraEnabled:   true,
		done:            make(chan struct{}),
	}
}

func (p *Peer) GetClient() SignalingClient {
	return p.client
}

func (p *Peer) GetPublishedTracks() []*webrtc.TrackLocalStaticRTP {
	p.publisherMu.RLock()
	defer p.publisherMu.RUnlock()

	tracks := make([]*webrtc.TrackLocalStaticRTP, len(p.publishedTracks))
	copy(tracks, p.publishedTracks)
	return tracks
}

func (p *Peer) HandlePublisherOffer(sdp string) (string, error) {
	config := p.config.GetWebRTCConfig()
	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return "", fmt.Errorf("create peer connection: %w", err)
	}

	p.publisherMu.Lock()
	p.Publisher = pc
	p.publisherMu.Unlock()

	// OnTrack fires when client starts sending media
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("📻 New track from peer %s: kind=%s, id=%s", p.ID, track.Kind(), track.ID())

		codec := track.Codec()
		log.Printf("🎵 TRACK INFO: %s | Codec: %s", track.Kind(), codec.MimeType)

		// Create local track for relaying to other participants
		localTrack, err := webrtc.NewTrackLocalStaticRTP(
			track.Codec().RTPCodecCapability,
			track.ID(),
			p.ID,
		)
		if err != nil {
			log.Printf("❌ Error creating local track: %v", err)
			return
		}

		p.publisherMu.Lock()
		p.publishedTracks = append(p.publishedTracks, localTrack)
		p.publisherMu.Unlock()

		if p.Room != nil {
			p.Room.peersMu.Lock()
			p.Room.trackReceivers[localTrack] = receiver
			p.Room.trackSSRC[localTrack] = uint32(track.SSRC())
			p.Room.peersMu.Unlock()
		}

		go p.relayTrack(track, localTrack)

		log.Printf("📡 DISTRIBUTING: Adding %s track from peer %s to other peers",
			track.Kind(), p.ID)
		p.addTrackToOtherPeers(localTrack, track)
	})

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		p.client.SendNotification(protocol.NotifyIceCandidate, map[string]interface{}{
			"target":        "publisher",
			"candidate":     candidate.ToJSON().Candidate,
			"sdpMid":        *candidate.ToJSON().SDPMid,
			"sdpMLineIndex": *candidate.ToJSON().SDPMLineIndex,
		})
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("🔌 Publisher connection state for peer %s: %s", p.ID, state.String())
	})

	offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}
	if err := pc.SetRemoteDescription(offer); err != nil {
		return "", fmt.Errorf("set remote description: %w", err)
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("create answer: %w", err)
	}

	if err := pc.SetLocalDescription(answer); err != nil {
		return "", fmt.Errorf("set local description: %w", err)
	}

	log.Printf("✅ Publisher offer handled for peer %s", p.ID)
	return answer.SDP, nil
}

func (p *Peer) relayTrack(remoteTrack *webrtc.TrackRemote, localTrack *webrtc.TrackLocalStaticRTP) {
	isVideo := remoteTrack.Kind() == webrtc.RTPCodecTypeVideo
	log.Printf("🚀 RTP RELAY: Started relaying %s packets from peer %s", remoteTrack.Kind(), p.ID)

	defer func() {
		log.Printf("🛑 Track relay stopped for peer %s, track %s", p.ID, remoteTrack.ID())
	}()

	for {
		select {
		case <-p.done:
			return
		default:
		}

		rtp, _, err := remoteTrack.ReadRTP()
		if err != nil {
			select {
			case <-p.done:
				return
			default:
				log.Printf("❌ RTP read error for peer %s: %v", p.ID, err)
				return
			}
		}

		if isVideo && !p.IsCameraEnabled() {
			continue
		}

		if err := localTrack.WriteRTP(rtp); err != nil {
			select {
			case <-p.done:
				return
			default:
				log.Printf("❌ Error writing RTP: %v", err)
			}
		}
	}
}

func (p *Peer) addTrackToOtherPeers(localTrack *webrtc.TrackLocalStaticRTP, remoteTrack *webrtc.TrackRemote) {
	if p.Room == nil {
		return
	}

	peers := p.Room.GetPeers()
	log.Printf("📊 DISTRIBUTION: Adding %s track from peer %s to %d other peers",
		localTrack.Kind(), p.ID, len(peers)-1)

	for _, otherPeer := range peers {
		if otherPeer.ID == p.ID {
			continue
		}

		log.Printf("🔗 SUBSCRIBER: Adding %s track to peer %s", localTrack.Kind(), otherPeer.ID)
		otherPeer.AddTrackToSubscriber(localTrack)
	}
}

func (p *Peer) AddTrackToSubscriber(localTrack *webrtc.TrackLocalStaticRTP) {
	pc := p.getOrCreateSubscriber()
	if pc == nil {
		return
	}

	rtpSender, err := pc.AddTrack(localTrack)
	if err != nil {
		log.Printf("❌ Error adding track to peer %s: %v", p.ID, err)
		return
	}

	log.Printf("✅ Track added to peer %s", p.ID)

	if localTrack.Kind() == webrtc.RTPCodecTypeVideo {
		p.requestKeyframe(localTrack)
	}

	go p.handleRTCP(rtpSender, localTrack)
	p.ScheduleRenegotiation()
}

func (p *Peer) requestKeyframe(track *webrtc.TrackLocalStaticRTP) {
	if p.Room == nil {
		return
	}

	p.Room.peersMu.RLock()
	originalReceiver := p.Room.trackReceivers[track]
	ssrc := p.Room.trackSSRC[track]
	p.Room.peersMu.RUnlock()

	if originalReceiver == nil {
		return
	}

	if transport := originalReceiver.Transport(); transport != nil {
		pli := &rtcp.PictureLossIndication{MediaSSRC: ssrc}
		if _, err := transport.WriteRTCP([]rtcp.Packet{pli}); err != nil {
			log.Printf("⚠️ Failed to send PLI: %v", err)
		}
	}
}

func (p *Peer) handleRTCP(rtpSender *webrtc.RTPSender, track *webrtc.TrackLocalStaticRTP) {
	rtcpBuf := make([]byte, 1500)
	for {
		select {
		case <-p.done:
			return
		default:
		}

		n, _, err := rtpSender.Read(rtcpBuf)
		if err != nil {
			return
		}

		pkts, err := rtcp.Unmarshal(rtcpBuf[:n])
		if err != nil {
			continue
		}

		p.forwardRTCP(pkts, track)
	}
}

func (p *Peer) forwardRTCP(pkts []rtcp.Packet, track *webrtc.TrackLocalStaticRTP) {
	if p.Room == nil {
		return
	}

	p.Room.peersMu.RLock()
	originalReceiver := p.Room.trackReceivers[track]
	ssrc := p.Room.trackSSRC[track]
	p.Room.peersMu.RUnlock()

	if originalReceiver == nil {
		return
	}

	transport := originalReceiver.Transport()
	if transport == nil {
		return
	}

	for _, pkt := range pkts {
		switch pktVar := pkt.(type) {
		case *rtcp.PictureLossIndication:
			pktVar.MediaSSRC = ssrc
			transport.WriteRTCP([]rtcp.Packet{pktVar})
		case *rtcp.FullIntraRequest:
			transport.WriteRTCP([]rtcp.Packet{pktVar})
		}
	}
}

func (p *Peer) getOrCreateSubscriber() *webrtc.PeerConnection {
	p.subMu.Lock()
	defer p.subMu.Unlock()

	if p.Subscriber != nil {
		return p.Subscriber
	}

	config := p.config.GetWebRTCConfig()
	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		log.Printf("❌ Error creating subscriber PC for peer %s: %v", p.ID, err)
		return nil
	}

	pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	})
	pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	})
	log.Printf("✅ Created unified subscriber connection")

	p.Subscriber = pc

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		p.subMu.Lock()
		p.subscriberICE = append(p.subscriberICE, candidate.ToJSON())
		p.subMu.Unlock()
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("🔌 Subscriber connection state for peer %s: %s", p.ID, state.String())
	})

	pc.OnSignalingStateChange(func(state webrtc.SignalingState) {
		log.Printf("📡 Subscriber signaling state for peer %s: %s", p.ID, state.String())

		if state == webrtc.SignalingStateStable {
			p.renegotiationMu.Lock()
			needsRenegotiation := p.pendingRenegotiation
			p.renegotiationMu.Unlock()

			if needsRenegotiation {
				log.Printf("📡 Performing pending renegotiation for peer %s", p.ID)
				p.renegotiationMu.Lock()
				p.pendingRenegotiation = false
				p.isNegotiating = true
				p.renegotiationMu.Unlock()
				go p.performRenegotiation()
			}
		}
	})

	log.Printf("✅ Subscriber created for peer %s", p.ID)

	return pc
}

func (p *Peer) ScheduleRenegotiation() {
	p.renegotiationMu.Lock()
	defer p.renegotiationMu.Unlock()

	if p.isNegotiating {
		p.pendingRenegotiation = true
		return
	}

	p.subMu.Lock()
	pc := p.Subscriber
	p.subMu.Unlock()

	if pc == nil {
		return
	}

	if pc.SignalingState() != webrtc.SignalingStateStable {
		p.pendingRenegotiation = true
		return
	}

	if p.renegotiationTimer != nil {
		p.renegotiationTimer.Stop()
	}

	p.renegotiationTimer = time.AfterFunc(renegotiationDebounce, func() {
		p.renegotiationMu.Lock()
		p.isNegotiating = true
		p.pendingRenegotiation = false
		p.renegotiationTimer = nil
		p.renegotiationMu.Unlock()

		p.performRenegotiation()
	})
}

func (p *Peer) performRenegotiation() {
	defer func() {
		p.renegotiationMu.Lock()
		p.isNegotiating = false
		p.renegotiationMu.Unlock()
	}()

	p.subMu.Lock()
	pc := p.Subscriber
	p.subMu.Unlock()

	if pc == nil {
		return
	}

	if pc.SignalingState() != webrtc.SignalingStateStable {
		p.ScheduleRenegotiation()
		return
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		log.Printf("❌ Error creating offer for peer %s: %v", p.ID, err)
		return
	}

	if err := pc.SetLocalDescription(offer); err != nil {
		log.Printf("❌ Error setting local description for peer %s: %v", p.ID, err)
		return
	}

	p.client.SendNotification(protocol.NotifySubscriberOffer, map[string]interface{}{
		"sdp":  offer.SDP,
		"type": "offer",
	})

	log.Printf("📤 Sent subscriber offer to peer %s", p.ID)
}

func (p *Peer) removeTracksFromSubscriber(tracks []*webrtc.TrackLocalStaticRTP) {
	p.subMu.Lock()
	pc := p.Subscriber
	p.subMu.Unlock()

	if pc == nil || len(tracks) == 0 {
		return
	}

	log.Printf("🧹 Removing %d tracks from peer %s", len(tracks), p.ID)

	senders := pc.GetSenders()
	for _, track := range tracks {
		for _, sender := range senders {
			if sender.Track() != nil && sender.Track().ID() == track.ID() {
				if err := pc.RemoveTrack(sender); err != nil {
					log.Printf("⚠️ Error removing track: %v", err)
				}
				break
			}
		}
	}

	p.ScheduleRenegotiation()
}

func (p *Peer) HandleSubscriberAnswer(sdp string) error {
	p.subMu.Lock()
	pc := p.Subscriber
	p.subMu.Unlock()

	if pc == nil {
		return fmt.Errorf("subscriber connection not created")
	}

	answer := webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp}
	if err := pc.SetRemoteDescription(answer); err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}

	log.Printf("✅ Subscriber answer handled for peer %s", p.ID)

	p.subMu.Lock()
	bufferedICE := p.subscriberICE
	p.subscriberICE = nil
	p.subMu.Unlock()

	if len(bufferedICE) > 0 {
		for _, ice := range bufferedICE {
			p.client.SendNotification(protocol.NotifyIceCandidate, map[string]interface{}{
				"target":        "subscriber",
				"candidate":     ice.Candidate,
				"sdpMid":        *ice.SDPMid,
				"sdpMLineIndex": *ice.SDPMLineIndex,
			})
		}
	}

	return nil
}

func (p *Peer) AddICECandidate(target string, candidate string) error {
	ice := webrtc.ICECandidateInit{Candidate: candidate}

	switch target {
	case "publisher":
		p.publisherMu.RLock()
		pc := p.Publisher
		p.publisherMu.RUnlock()
		if pc == nil {
			return fmt.Errorf("publisher not initialized")
		}
		return pc.AddICECandidate(ice)

	case "subscriber":
		p.subMu.Lock()
		pc := p.Subscriber
		p.subMu.Unlock()
		if pc == nil {
			return fmt.Errorf("subscriber connection not found")
		}
		return pc.AddICECandidate(ice)

	default:
		return fmt.Errorf("invalid target: %s", target)
	}
}

func (p *Peer) closeConnections() {
	p.doneOnce.Do(func() {
		close(p.done)
	})

	p.renegotiationMu.Lock()
	if p.renegotiationTimer != nil {
		p.renegotiationTimer.Stop()
		p.renegotiationTimer = nil
	}
	p.renegotiationMu.Unlock()

	p.publisherMu.Lock()
	if p.Publisher != nil {
		p.Publisher.Close()
		p.Publisher = nil
	}
	p.publishedTracks = nil
	p.publisherMu.Unlock()

	p.subMu.Lock()
	if p.Subscriber != nil {
		p.Subscriber.Close()
		p.Subscriber = nil
	}
	p.subscriberICE = nil
	p.subMu.Unlock()

	log.Printf("🧹 Peer %s connections closed", p.ID)
}

func (p *Peer) Close() {
	if p.Room != nil {
		p.Room.RemovePeer(p)
		return
	}
	p.closeConnections()
}

func (p *Peer) setRoom(room *Room) {
	p.Room = room
}

func (p *Peer) SetCameraEnabled(enabled bool) {
	p.cameraMu.Lock()
	wasEnabled := p.cameraEnabled
	p.cameraEnabled = enabled
	p.cameraMu.Unlock()

	if wasEnabled == enabled {
		return
	}

	if p.Room != nil {
		p.notifyOthersAboutCameraState(enabled)
	}
}

func (p *Peer) notifyOthersAboutCameraState(enabled bool) {
	peers := p.Room.GetPeers()
	for _, otherPeer := range peers {
		if otherPeer.ID == p.ID {
			continue
		}

		otherPeer.GetClient().SendNotification(protocol.NotifyCameraStateChanged, map[string]interface{}{
			"peerId":  p.ID,
			"enabled": enabled,
		})
	}
}

func (p *Peer) IsCameraEnabled() bool {
	p.cameraMu.RLock()
	defer p.cameraMu.RUnlock()
	return p.cameraEnabled
}
