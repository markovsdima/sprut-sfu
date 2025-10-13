package room

import (
	"fmt"
	"log"
	"simple-sfu/internal/config"
	"sync"

	"github.com/pion/webrtc/v4"
)

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
	Subscriber *webrtc.PeerConnection // sends media to client

	publishedTracks []*webrtc.TrackLocalStaticRTP
	config          *config.Config

	subMu           sync.Mutex
	pendingSubICE   []webrtc.ICECandidateInit
	renegotiationMu sync.Mutex
}

func NewPeer(id string, client SignalingClient, cfg *config.Config) *Peer {
	return &Peer{
		ID:              id,
		client:          client,
		publishedTracks: make([]*webrtc.TrackLocalStaticRTP, 0),
		config:          cfg,
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
			track.StreamID(),
		)
		if err != nil {
			log.Printf("❌ Error creating local track: %v", err)
			return
		}

		p.publishedTracks = append(p.publishedTracks, localTrack)

		// Core SFU logic: relay RTP packets from publisher to subscribers
		go p.relayTrack(track, localTrack)

		log.Printf("📡 DISTRIBUTING: Adding %s track from peer %s to other peers",
			track.Kind(), p.ID)
		p.addTrackToOtherPeers(localTrack)

	})

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}

		p.client.SendNotification("iceCandidate", map[string]interface{}{
			"target":        "publisher",
			"candidate":     candidate.ToJSON().Candidate,
			"sdpMid":        *candidate.ToJSON().SDPMid,
			"sdpMLineIndex": *candidate.ToJSON().SDPMLineIndex,
		})
	})

	// OnConnectionStateChange отслеживает состояние WebRTC соединения
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("🔌 Publisher connection state for peer %s: %s", p.ID, state.String())

		// Если соединение закрылось, можно очистить ресурсы
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			log.Printf("⚠️  Publisher connection closed for peer %s", p.ID)
		}
	})

	// Устанавливаем remote description (offer от клиента)
	// Offer содержит информацию о медиа, которое клиент хочет отправлять
	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdp,
	}

	if err := pc.SetRemoteDescription(offer); err != nil {
		return "", fmt.Errorf("set remote description: %w", err)
	}

	// Создаём answer - ответ на offer
	// Answer содержит информацию о том, какое медиа сервер готов принимать
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

// HandleSubscriberAnswer обрабатывает answer от клиента на subscriber offer
func (p *Peer) HandleSubscriberAnswer(sdp string) error {
	if p.Subscriber == nil {
		return fmt.Errorf("subscriber connection not created")
	}

	answer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  sdp,
	}

	if err := p.Subscriber.SetRemoteDescription(answer); err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}

	log.Printf("✅ Subscriber answer handled for peer %s", p.ID)

	return nil
}

// AddICECandidate добавляет ICE candidate в соответствующий PeerConnection
func (p *Peer) AddICECandidate(target string, candidate string) error {
	ice := webrtc.ICECandidateInit{Candidate: candidate}

	switch target {
	case "publisher":
		pc := p.Publisher
		if pc == nil {
			return fmt.Errorf("publisher not initialized")
		}
		return pc.AddICECandidate(ice)

	case "subscriber":
		var pending []webrtc.ICECandidateInit
		var pc *webrtc.PeerConnection

		// блокируем только для работы с состоянием
		p.subMu.Lock()
		if p.Subscriber == nil {
			// Subscriber ещё не создан — просто добавляем в очередь
			p.pendingSubICE = append(p.pendingSubICE, ice)
			log.Printf("🕒 Subscriber not ready, queued ICE candidate for peer %s", p.ID)
			p.subMu.Unlock()
			return nil
		}

		// Subscriber уже есть — забираем отложенные кандидаты и текущий кандидат
		pending = append(p.pendingSubICE, ice)
		p.pendingSubICE = nil
		pc = p.Subscriber
		p.subMu.Unlock()

		// Добавляем кандидаты вне блокировки
		for _, queued := range pending {
			if err := pc.AddICECandidate(queued); err != nil {
				log.Printf("❌ Error adding queued ICE candidate: %v", err)
			}
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
	log.Printf("🚀 RTP RELAY: Started relaying %s packets from peer %s to subscribers",
		remoteTrack.Kind(), p.ID)

	defer func() {
		log.Printf("🛑 Track relay stopped for peer %s, track %s", p.ID, remoteTrack.ID())
	}()

	// Бесконечный цикл чтения RTP пакетов
	for {
		// Читаем RTP пакет от клиента
		// RTP (Real-time Transport Protocol) - протокол для передачи аудио/видео
		rtp, _, err := remoteTrack.ReadRTP()
		if err != nil {
			// Если ошибка чтения - трек закрыт
			return
		}

		// Пишем пакет в локальный трек
		// Это автоматически отправит пакет всем, кто подписан на этот трек
		if err := localTrack.WriteRTP(rtp); err != nil {
			log.Printf("❌ Error writing RTP: %v", err)
			// Не выходим из цикла при ошибках записи
			// Некоторые subscriber'ы могут отключиться
		}
	}
}

// addTrackToOtherPeers добавляет трек этого peer'а в subscriber'ы других участников
func (p *Peer) addTrackToOtherPeers(track *webrtc.TrackLocalStaticRTP) {
	if p.Room == nil {
		log.Printf("⚠️  No room for peer %s, cannot distribute track", p.ID)
		return
	}

	log.Printf("📤 Adding track from peer %s to other peers", p.ID)
	peers := p.Room.GetPeers()
	log.Printf("📊 DISTRIBUTION: Adding %s track from peer %s to %d other peers",
		track.Kind(), p.ID, len(peers)-1)

	// Получаем всех остальных peer'ов в комнате
	for _, otherPeer := range p.Room.GetPeers() {
		if otherPeer.ID == p.ID {
			continue // Пропускаем самого себя
		}

		log.Printf("🔗 SUBSCRIBER: Adding %s track to peer %s", track.Kind(), otherPeer.ID)
		// Добавляем трек в subscriber другого peer'а
		otherPeer.AddTrackToSubscriber(track)
	}
}

// addTrackToSubscriber добавляет трек в subscriber connection
func (p *Peer) AddTrackToSubscriber(track *webrtc.TrackLocalStaticRTP) {
	// Проверяем нужно ли создать Subscriber
	p.subMu.Lock()
	needCreate := (p.Subscriber == nil)
	p.subMu.Unlock()

	// Создаём Subscriber ВНЕ блокировки (не блокируем другие горутины)
	if needCreate {
		if err := p.createSubscriber(); err != nil {
			log.Printf("❌ Error creating subscriber for peer %s: %v", p.ID, err)
			return
		}
	}

	// Берём ссылку под блокировкой
	p.subMu.Lock()
	pc := p.Subscriber
	p.subMu.Unlock()

	// Дополнительная проверка на случай гонки
	if pc == nil {
		log.Printf("⚠️  Subscriber is nil after creation for peer %s", p.ID)
		return
	}

	// Добавляем трек в subscriber PeerConnection
	rtpSender, err := pc.AddTrack(track)
	if err != nil {
		log.Printf("❌ Error adding track to subscriber: %v", err)
		return
	}

	log.Printf("✅ Track added to subscriber for peer %s", p.ID)

	// Обрабатываем RTCP пакеты
	go func() {
		rtcpBuf := make([]byte, 1500)
		for {
			if _, _, rtcpErr := rtpSender.Read(rtcpBuf); rtcpErr != nil {
				return
			}
		}
	}()

	p.renegotiateSubscriber()
}

// createSubscriber создаёт subscriber PeerConnection
// Subscriber - это соединение, через которое peer ПОЛУЧАЕТ медиа от других участников
func (p *Peer) createSubscriber() error {
	config := p.config.GetWebRTCConfig()

	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("create peer connection: %w", err)
	}

	p.Subscriber = pc

	// Обработчик ICE кандидатов для subscriber
	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}

		p.client.SendNotification("iceCandidate", map[string]interface{}{
			"target":        "subscriber",
			"candidate":     candidate.ToJSON().Candidate,
			"sdpMid":        *candidate.ToJSON().SDPMid,
			"sdpMLineIndex": *candidate.ToJSON().SDPMLineIndex,
		})
	})

	// Обработчик состояния соединения
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("🔌 Subscriber connection state for peer %s: %s", p.ID, state.String())
	})

	log.Printf("✅ Subscriber created for peer %s", p.ID)

	return nil
}

// renegotiateSubscriber создаёт новый offer и отправляет клиенту
// Вызывается когда добавляется новый трек
func (p *Peer) renegotiateSubscriber() {
	if p.Subscriber == nil {
		log.Printf("⚠️  Subscriber not initialized for peer %s", p.ID)
		return
	}

	// Создаём offer с новыми треками
	offer, err := p.Subscriber.CreateOffer(nil)
	if err != nil {
		log.Printf("❌ Error creating subscriber offer: %v", err)
		return
	}

	// Устанавливаем local description
	if err := p.Subscriber.SetLocalDescription(offer); err != nil {
		log.Printf("❌ Error setting local description: %v", err)
		return
	}

	// Отправляем offer клиенту
	// Клиент должен ответить answer'ом
	p.client.SendNotification("subscriberOffer", map[string]interface{}{
		"sdp":  offer.SDP,
		"type": "offer",
	})

	log.Printf("📤 Sent subscriber offer to peer %s", p.ID)
}

// Close закрывает все соединения peer'а
func (p *Peer) Close() {
	if p.Publisher != nil {
		p.Publisher.Close()
		p.Publisher = nil
	}

	if p.Subscriber != nil {
		p.Subscriber.Close()
		p.Subscriber = nil
	}

	p.publishedTracks = nil
	log.Printf("🧹 Peer %s closed", p.ID)
}

func (p *Peer) setRoom(room *Room) {
	p.Room = room
}
