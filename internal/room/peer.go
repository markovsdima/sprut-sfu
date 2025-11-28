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

	subMu sync.Mutex

	renegotiationMu      sync.Mutex
	pendingRenegotiation bool // Single flag for renegotiation needed
	isNegotiating        bool // Single flag for active negotiation

	// Single buffer for subscriber ICE
	subscriberICE []webrtc.ICECandidateInit

	renegotiationTimer *time.Timer

	// Camera state management
	// Клиент говорит, что выключил трек. Сервер перестает пересылать пустые кадры(лишние пакеты)
	cameraEnabled bool
	cameraMu      sync.RWMutex
}

func NewPeer(id string, client SignalingClient, cfg *config.Config) *Peer {
	return &Peer{
		ID:                   id,
		client:               client,
		publishedTracks:      make([]*webrtc.TrackLocalStaticRTP, 0),
		config:               cfg,
		subscriberICE:        make([]webrtc.ICECandidateInit, 0),
		pendingRenegotiation: false,
		isNegotiating:        false,
		renegotiationTimer:   nil,
		cameraEnabled:        true,
	}
}

// GetClient возвращает SignalingClient (нужен для уведомлений)
func (p *Peer) GetClient() SignalingClient {
	return p.client
}

// GetPublishedTracks возвращает треки, опубликованные этим пиром
func (p *Peer) GetPublishedTracks() []*webrtc.TrackLocalStaticRTP {
	return p.publishedTracks
}

// HandlePublisherOffer processes client's offer to publish media
func (p *Peer) HandlePublisherOffer(sdp string) (string, error) {
	config := p.config.GetWebRTCConfig()

	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return "", fmt.Errorf("create peer connection: %w", err)
	}

	p.Publisher = pc

	// OnTrack fires when client starts sending media
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("📻 New track from peer %s: kind=%s, id=%s", p.ID, track.Kind(), track.ID())

		codec := track.Codec()
		log.Printf("🎵 TRACK INFO: %s | Codec: %s | SampleRate: %d | Channels: %d",
			track.Kind(), codec.MimeType, codec.ClockRate, codec.Channels)

		// Create local track for relaying to other participants
		localTrack, err := webrtc.NewTrackLocalStaticRTP(
			track.Codec().RTPCodecCapability,
			track.ID(),
			p.ID, // Use peer ID as stream ID for client-side grouping
		)
		if err != nil {
			log.Printf("❌ Error creating local track: %v", err)
			return
		}

		p.publishedTracks = append(p.publishedTracks, localTrack)

		// Store original receiver and SSRC for keyframe requests/forwarding
		p.Room.trackReceivers[localTrack] = receiver
		p.Room.trackSSRC[localTrack] = uint32(track.SSRC())

		// Core SFU logic: relay RTP packets from publisher to subscribers
		go p.relayTrack(track, localTrack)

		log.Printf("📡 DISTRIBUTING: Adding %s track from peer %s to other peers",
			track.Kind(), p.ID)
		p.addTrackToOtherPeers(localTrack)
	})

	// Publisher ICE candidates
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

	// Publisher connection state
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("🔌 Publisher connection state for peer %s: %s", p.ID, state.String())
	})

	// Устанавливаем remote description (offer от клиента)
	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdp,
	}

	if err := pc.SetRemoteDescription(offer); err != nil {
		return "", fmt.Errorf("set remote description: %w", err)
	}

	// Создаём answer - ответ на offer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("create answer: %w", err)
	}

	// Устанавливаем local description
	if err := pc.SetLocalDescription(answer); err != nil {
		return "", fmt.Errorf("set local description: %w", err)
	}

	log.Printf("✅ Publisher offer handled for peer %s", p.ID)

	return answer.SDP, nil
}

// HandleSubscriberAnswer обрабатывает answer от клиента для subscriber
func (p *Peer) HandleSubscriberAnswer(sdp string) error {
	p.subMu.Lock()
	pc := p.Subscriber
	p.subMu.Unlock()

	if pc == nil {
		return fmt.Errorf("subscriber connection not created")
	}

	answer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  sdp,
	}

	if err := pc.SetRemoteDescription(answer); err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}

	log.Printf("✅ Subscriber answer handled for peer %s", p.ID)

	// Send buffered ICE after remote desc
	p.subMu.Lock()
	if len(p.subscriberICE) > 0 {
		log.Printf("🧊 Sending %d buffered ICE candidates for peer %s", len(p.subscriberICE), p.ID)
		for _, ice := range p.subscriberICE {
			p.client.SendNotification("iceCandidate", map[string]interface{}{
				"target":        "subscriber",
				"candidate":     ice.Candidate,
				"sdpMid":        *ice.SDPMid,
				"sdpMLineIndex": *ice.SDPMLineIndex,
			})
		}
		p.subscriberICE = nil // Clear buffer
	}
	p.subMu.Unlock()

	return nil
}

// AddICECandidate добавляет ICE candidate в соответствующий PeerConnection
func (p *Peer) AddICECandidate(target string, candidate string) error {
	ice := webrtc.ICECandidateInit{Candidate: candidate}

	switch target {
	case "publisher":
		if p.Publisher == nil {
			return fmt.Errorf("publisher not initialized")
		}
		return p.Publisher.AddICECandidate(ice)
	case "subscriber":
		p.subMu.Lock()
		defer p.subMu.Unlock()
		if p.Subscriber == nil {
			return fmt.Errorf("subscriber connection not found")
		}
		if err := p.Subscriber.AddICECandidate(ice); err != nil {
			return fmt.Errorf("failed to add ICE candidate to subscriber: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("invalid target: %s", target)
	}
}

// relayTrack - это сердце SFU
// Читает RTP пакеты из remote track (от клиента) и пишет в local track
// Local track автоматически отправляет пакеты всем подписчикам
func (p *Peer) relayTrack(remoteTrack *webrtc.TrackRemote, localTrack *webrtc.TrackLocalStaticRTP) {
	isVideo := remoteTrack.Kind() == webrtc.RTPCodecTypeVideo

	log.Printf("🚀 RTP RELAY: Started relaying %s packets from peer %s to subscribers",
		remoteTrack.Kind(), p.ID)

	defer func() {
		log.Printf("🛑 Track relay stopped for peer %s, track %s", p.ID, remoteTrack.ID())
	}()

	// RTP packets reading loop
	for {
		rtp, _, err := remoteTrack.ReadRTP()
		if err != nil {
			return
		}

		// Для видео треков проверяем состояние камеры
		if isVideo && !p.IsCameraEnabled() {
			continue // Камера выключена - пропускаем пакет
		}

		if err := localTrack.WriteRTP(rtp); err != nil {
			log.Printf("❌ Error writing RTP: %v", err)
		}
	}
}

// addTrackToOtherPeers добавляет трек этого peer'а в subscriber'ы других участников
func (p *Peer) addTrackToOtherPeers(track *webrtc.TrackLocalStaticRTP) {
	if p.Room == nil {
		log.Printf("⚠️  No room for peer %s, cannot distribute track", p.ID)
		return
	}

	peers := p.Room.GetPeers()
	log.Printf("📊 DISTRIBUTION: Adding %s track from peer %s to %d other peers",
		track.Kind(), p.ID, len(peers)-1)

	// Добавляем трек каждому другому peer'у
	for _, otherPeer := range peers {
		if otherPeer.ID == p.ID {
			continue // Пропускаем самого себя
		}

		log.Printf("🔗 SUBSCRIBER: Adding %s track to peer %s", track.Kind(), otherPeer.ID)
		otherPeer.addTrackToThisSubscriber(track)
	}
}

// addTrackToThisSubscriber добавляет трек от конкретного peer'а к subscriber соединению этого peer'а
func (p *Peer) addTrackToThisSubscriber(track *webrtc.TrackLocalStaticRTP) {
	// Создаём или получаем subscriber соединение
	subscriberPC := p.getOrCreateSubscriber()
	if subscriberPC == nil {
		return
	}

	// Добавляем трек в subscriber PeerConnection
	rtpSender, err := subscriberPC.AddTrack(track)
	if err != nil {
		log.Printf("❌ Error adding track to subscriber for peer %s: %v", p.ID, err)
		return
	}

	log.Printf("✅ Track added to subscriber for peer %s", p.ID)

	// Request initial keyframe if video track
	if track.Kind() == webrtc.RTPCodecTypeVideo {
		if originalReceiver := p.Room.trackReceivers[track]; originalReceiver != nil {
			if transport := originalReceiver.Transport(); transport != nil {
				pli := &rtcp.PictureLossIndication{
					MediaSSRC: p.Room.trackSSRC[track],
				}
				if _, err := transport.WriteRTCP([]rtcp.Packet{pli}); err != nil {
					log.Printf("⚠️ Failed to send PLI for track in peer %s: %v", p.ID, err)
				} else {
					log.Printf("📡 Sent PLI keyframe request for video track to publisher")
				}
			} else {
				log.Printf("⚠️ Transport not ready for PLI send in peer %s", p.ID)
			}
		}
	}

	// Обрабатываем RTCP пакеты
	go func() {
		rtcpBuf := make([]byte, 1500)
		for {
			n, _, rtcpErr := rtpSender.Read(rtcpBuf)
			if rtcpErr != nil {
				return
			}

			// Unmarshal and forward PLI/FIR to original publisher
			pkts, err := rtcp.Unmarshal(rtcpBuf[:n])
			if err != nil {
				log.Printf("⚠️ RTCP unmarshal error: %v", err)
				continue
			}

			for _, pkt := range pkts {
				switch pktVar := pkt.(type) {
				case *rtcp.PictureLossIndication:
					// Forward PLI to original receiver (adjust SSRC if needed)
					pktVar.MediaSSRC = p.Room.trackSSRC[track]
					if originalReceiver := p.Room.trackReceivers[track]; originalReceiver != nil {
						if transport := originalReceiver.Transport(); transport != nil {
							if _, err := transport.WriteRTCP([]rtcp.Packet{pktVar}); err != nil {
								log.Printf("⚠️ Failed to forward PLI: %v", err)
							}
						}
					}
				case *rtcp.FullIntraRequest:
					// Similar forwarding for FIR
					if originalReceiver := p.Room.trackReceivers[track]; originalReceiver != nil {
						if transport := originalReceiver.Transport(); transport != nil {
							if _, err := transport.WriteRTCP([]rtcp.Packet{pktVar}); err != nil {
								log.Printf("⚠️ Failed to forward FIR: %v", err)
							}
						}
					}
				}
			}
		}
	}()

	// Планируем renegotiation для этого subscriber соединения
	p.ScheduleRenegotiation()
}

func (p *Peer) AddTrackToSubscriber(track *webrtc.TrackLocalStaticRTP) {
	p.addTrackToThisSubscriber(track)
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

	// Обработчик ICE кандидатов для этого subscriber соединения
	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}

		// Буферизуем ICE кандидаты до установки remote description
		p.subMu.Lock()
		p.subscriberICE = append(p.subscriberICE, candidate.ToJSON())
		p.subMu.Unlock()

		log.Printf("🕒 Buffered ICE candidate for peer %s", p.ID)
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("🔌 Subscriber connection state for peer %s: %s", p.ID, state.String())
	})

	pc.OnSignalingStateChange(func(state webrtc.SignalingState) {
		log.Printf("📡 Subscriber signaling state for peer %s: %s", p.ID, state.String())

		// Когда состояние становится стабильным - запускаем pending renegotiation
		if state == webrtc.SignalingStateStable {
			p.renegotiationMu.Lock()
			needsRenegotiation := p.pendingRenegotiation
			p.renegotiationMu.Unlock()

			if needsRenegotiation {
				log.Printf("📡 Signaling state stable for peer %s, performing pending renegotiation", p.ID)
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

// scheduleRenegotiation планирует renegotiation для subscriber соединения
func (p *Peer) ScheduleRenegotiation() {
	p.renegotiationMu.Lock()
	defer p.renegotiationMu.Unlock()

	// Если уже идет negotiation - просто помечаем pending
	if p.isNegotiating {
		log.Printf("🔄 Renegotiation in progress, marking as pending for peer %s", p.ID)
		p.pendingRenegotiation = true
		return
	}

	// Проверяем состояние
	p.subMu.Lock()
	pc := p.Subscriber
	p.subMu.Unlock()

	if pc == nil {
		return
	}

	// Если состояние не стабильное - помечаем pending и выходим
	if pc.SignalingState() != webrtc.SignalingStateStable {
		log.Printf("⏳ Subscriber not stable for peer %s, state: %s. Will renegotiate when stable.",
			p.ID, pc.SignalingState().String())
		p.pendingRenegotiation = true
		return
	}

	// Debounce logic
	// ==============
	// Cancel previous timer if exists
	if p.renegotiationTimer != nil {
		p.renegotiationTimer.Stop()
		log.Printf("🔄 Resetting renegotiation timer for peer %s", p.ID)
	}

	// Set new timer
	p.renegotiationTimer = time.AfterFunc(renegotiationDebounce, func() {
		p.renegotiationMu.Lock()
		p.isNegotiating = true
		p.pendingRenegotiation = false
		p.renegotiationTimer = nil
		p.renegotiationMu.Unlock()

		log.Printf("⏰ Debounce timer fired, performing renegotiation for peer %s", p.ID)
		p.performRenegotiation()
	})

	log.Printf("⏱️  Scheduled debounced renegotiation in 150ms for peer %s", p.ID)
}

// performRenegotiation выполняет renegotiation для subscriber соединения
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
		log.Printf("⚠️  Subscriber not initialized for peer %s", p.ID)
		return
	}

	// Проверяем состояние перед созданием offer
	currentState := pc.SignalingState()
	if currentState != webrtc.SignalingStateStable {
		log.Printf("❌ Cannot renegotiate for peer %s: state is %s, not stable", p.ID, currentState.String())
		// Планируем повторную попытку
		p.ScheduleRenegotiation()
		return
	}

	log.Printf("🔄 Starting renegotiation for peer %s", p.ID)

	// Создаём offer с новыми треками
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		log.Printf("❌ Error creating subscriber offer for peer %s: %v", p.ID, err)
		return
	}

	// Устанавливаем local description
	if err := pc.SetLocalDescription(offer); err != nil {
		log.Printf("❌ Error setting local description for peer %s: %v", p.ID, err)
		return
	}

	// Отправляем offer клиенту
	p.client.SendNotification("subscriberOffer", map[string]interface{}{
		"sdp":  offer.SDP,
		"type": "offer",
	})

	log.Printf("📤 Sent subscriber offer to peer %s", p.ID)
}

// closeConnections закрывает только соединения, БЕЗ обращения к Room
// Вызывается из Room.RemovePeer() и Room.Close()
func (p *Peer) closeConnections() {
	// Отменяем таймер если есть
	p.renegotiationMu.Lock()
	if p.renegotiationTimer != nil {
		p.renegotiationTimer.Stop()
		p.renegotiationTimer = nil
	}
	p.renegotiationMu.Unlock()

	// Закрываем publisher
	if p.Publisher != nil {
		p.Publisher.Close()
		p.Publisher = nil
	}

	// Закрываем subscriber соединение
	p.subMu.Lock()
	if p.Subscriber != nil {
		p.Subscriber.Close()
		p.Subscriber = nil
	}
	p.subMu.Unlock()

	// Очистка треков и ICE буфера
	p.publishedTracks = nil
	p.subscriberICE = nil

	log.Printf("🧹 Peer %s connections closed", p.ID)
}

// Close - полный cleanup включая удаление из Room
// Используется когда peer закрывается напрямую (НЕ через Room.RemovePeer)
func (p *Peer) Close() {
	// Если peer в комнате - используем штатный путь
	if p.Room != nil {
		log.Printf("⚠️  Peer.Close() called directly, using Room.RemovePeer instead")
		p.Room.RemovePeer(p)
		return
	}

	// Если peer не в комнате (edge case) - просто закрываем соединения
	p.closeConnections()
}

func (p *Peer) setRoom(room *Room) {
	p.Room = room
}

// removeTracksFromSubscriber removes specified tracks from this peer's subscriber connection
func (p *Peer) removeTracksFromSubscriber(tracks []*webrtc.TrackLocalStaticRTP) {
	p.subMu.Lock()
	pc := p.Subscriber
	p.subMu.Unlock()

	if pc == nil {
		return // No subscriber connection yet
	}

	removedCount := 0

	// Get all senders
	senders := pc.GetSenders()

	for _, track := range tracks {
		// Find and remove corresponding sender
		for _, sender := range senders {
			if sender.Track() != nil && sender.Track().ID() == track.ID() {
				if err := pc.RemoveTrack(sender); err != nil {
					log.Printf("⚠️ Failed to remove track %s from peer %s subscriber: %v",
						track.ID(), p.ID, err)
				} else {
					removedCount++
					log.Printf("✅ Removed track %s from peer %s subscriber", track.ID(), p.ID)
				}
				break
			}
		}
	}

	// Trigger renegotiation if any tracks were removed
	if removedCount > 0 {
		log.Printf("🔄 Scheduling renegotiation for peer %s after removing %d tracks", p.ID, removedCount)
		p.ScheduleRenegotiation()
	}
}

// MARK: - Camera state management

func (p *Peer) SetCameraEnabled(enabled bool) {
	p.cameraMu.Lock()
	wasEnabled := p.cameraEnabled
	p.cameraEnabled = enabled
	p.cameraMu.Unlock()

	if wasEnabled == enabled {
		return // No change
	}

	log.Printf("📹 Camera state changed for peer %s: %v -> %v", p.ID, wasEnabled, enabled)

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

// IsCameraEnabled проверяет включена ли камера
func (p *Peer) IsCameraEnabled() bool {
	p.cameraMu.RLock()
	defer p.cameraMu.RUnlock()
	return p.cameraEnabled
}
