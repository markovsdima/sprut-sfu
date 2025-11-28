package signaling

import (
	"encoding/json"
	"log"
	"simple-sfu/internal/config"
	"simple-sfu/internal/room"
	"simple-sfu/pkg/protocol"
)

// Client представляет одного подключённого клиента
// Он связывает WebSocket соединение с WebRTC peer'ом
type Client struct {
	conn        *Connection    // WebSocket соединение
	roomManager *room.Manager  // Менеджер комнат
	config      *config.Config // Конфигурация
	peer        *room.Peer     // WebRTC peer (создаётся при join)
	currentRoom *room.Room     // Текущая комната
}

func NewClient(conn *Connection, roomManager *room.Manager, cfg *config.Config) *Client {
	return &Client{
		conn:        conn,
		roomManager: roomManager,
		config:      cfg,
	}
}

// SendNotification реализует интерфейс SignalingClient из room.Peer
// Отправляет уведомление клиенту (без ID запроса)
func (c *Client) SendNotification(method string, params interface{}) {
	notification := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	}
	c.conn.WriteNotify(notification)
}

// handleMessage обрабатывает входящие JSON-RPC сообщения
func (c *Client) HandleMessage(msg []byte) {
	var req protocol.Request
	if err := json.Unmarshal(msg, &req); err != nil {
		log.Printf("❌ Invalid JSON: %v", err)
		return
	}

	log.Printf("📨 Received: method=%s, id=%d, peer=%s", req.Method, req.ID, c.conn.PeerID)

	// Маршрутизируем запрос к нужному обработчику
	switch req.Method {
	case protocol.MethodRoomJoin:
		c.handleJoin(req)
	case protocol.MethodRoomLeave:
		c.handleLeave(req)
	case protocol.MethodPublisherOffer:
		c.handlePublisherOffer(req)
	case protocol.MethodSubscriberAnswer:
		c.handleSubscriberAnswer(req)
	case protocol.MethodIceCandidate:
		c.handleICECandidate(req)
	case protocol.MethodCameraState:
		c.handleCameraState(req)
	default:
		c.sendError(req.ID, protocol.ErrCodeMethodNotFound, "method not found: "+req.Method)
	}
}

// handleJoin обрабатывает подключение к комнате
func (c *Client) handleJoin(req protocol.Request) {
	var params protocol.JoinRoomParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		c.sendError(req.ID, protocol.ErrCodeInvalidParams, "invalid params")
		return
	}

	log.Printf("👤 Peer %s joining room %s (name: %s)", params.PeerID, params.RoomID, params.DisplayName)

	// Получаем или создаём комнату
	r := c.roomManager.GetOrCreate(params.RoomID)

	// Создаём Peer объект
	peer := room.NewPeer(params.PeerID, c, c.config) // передаём себя как SignalingClient
	c.peer = peer
	c.currentRoom = r

	// Получаем список уже существующих участников
	existingPeerIDs := r.GetPeerIDs()

	// Добавляем peer в комнату
	r.AddPeer(peer)

	// Распределяем существующие медиапотоки новому клиенту
	log.Printf("🔄 DISTRIBUTING: Adding existing tracks from %d peers to new peer %s", len(existingPeerIDs), params.PeerID)

	// Проходим по всем существующим пирам и добавляем их медиапотоки к новому клиенту
	addedTracks := false
	for _, existingPeerID := range existingPeerIDs {
		existingPeer := r.GetPeers()[existingPeerID]
		if existingPeer == nil {
			continue
		}

		// Добавляем все треки существующего пира к новому клиенту
		for _, track := range existingPeer.GetPublishedTracks() {
			log.Printf("🔗 SUBSCRIBER: Adding %s track from existing peer %s to new peer %s",
				track.Kind(), existingPeerID, params.PeerID)
			peer.AddTrackToSubscriber(track)
			addedTracks = true
		}
	}

	// Если добавлены треки, планируем renegotiation (только один раз)
	if addedTracks {
		peer.ScheduleRenegotiation()
	}

	// Уведомляем всех остальных участников о новом peer
	c.notifyOthersAboutJoin(params.PeerID, params.DisplayName)

	// Отправляем ответ клиенту со списком существующих участников
	c.sendSuccess(req.ID, protocol.JoinRoomResult{
		Success: true,
		Peers:   existingPeerIDs,
	})
}

// handleLeave обрабатывает выход из комнаты
func (c *Client) handleLeave(req protocol.Request) {
	if c.currentRoom == nil || c.peer == nil {
		c.sendError(req.ID, protocol.ErrCodeNotInRoom, "not in room")
		return
	}

	log.Printf("👋 Peer %s leaving room", c.peer.ID)

	// Уведомляем других участников
	c.notifyOthersAboutLeave(c.peer.ID)

	// Удаляем peer из комнаты
	c.currentRoom.RemovePeer(c.peer)

	// Если комната пустая, удаляем её
	if len(c.currentRoom.GetPeerIDs()) == 0 {
		c.roomManager.Remove(c.currentRoom.GetName())
	}

	c.peer = nil
	c.currentRoom = nil

	c.sendSuccess(req.ID, protocol.LeaveRoomResult{Success: true})
}

// handlePublisherOffer обрабатывает offer от клиента для публикации медиа
func (c *Client) handlePublisherOffer(req protocol.Request) {
	if c.peer == nil {
		c.sendError(req.ID, protocol.ErrCodeNotInRoom, "join room first")
		return
	}

	var params protocol.PublisherOfferParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		c.sendError(req.ID, protocol.ErrCodeInvalidParams, "invalid params")
		return
	}

	// ✅ params.SDP уже строка
	answerSDP, err := c.peer.HandlePublisherOffer(params.SDP)
	if err != nil {
		log.Printf("❌ Error handling publisher offer: %v", err)
		c.sendError(req.ID, protocol.ErrCodeSDPError, err.Error())
		return
	}

	// ✅ Отправляем просто строку
	c.sendSuccess(req.ID, protocol.PublisherOfferResult{
		SDP:  answerSDP,
		Type: "answer",
	})
}

// handleSubscriberAnswer обрабатывает answer от клиента на subscriber offer
func (c *Client) handleSubscriberAnswer(req protocol.Request) {
	if c.peer == nil {
		c.sendError(req.ID, protocol.ErrCodeNotInRoom, "join room first")
		return
	}

	var params protocol.SubscriberAnswerParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		c.sendError(req.ID, protocol.ErrCodeInvalidParams, "invalid params")
		return
	}

	if err := c.peer.HandleSubscriberAnswer(params.SDP); err != nil {
		log.Printf("❌ Error handling subscriber answer: %v", err)
		c.sendError(req.ID, protocol.ErrCodeSDPError, err.Error())
		return
	}

	c.sendSuccess(req.ID, protocol.SubscriberAnswerResult{Success: true})
}

// handleICECandidate обрабатывает ICE кандидаты от клиента
func (c *Client) handleICECandidate(req protocol.Request) {
	if c.peer == nil {
		c.sendError(req.ID, protocol.ErrCodeNotInRoom, "join room first")
		return
	}

	var params protocol.IceCandidateParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		c.sendError(req.ID, protocol.ErrCodeInvalidParams, "invalid params")
		return
	}

	// Добавляем ICE candidate в нужный PeerConnection
	if err := c.peer.AddICECandidate(params.Target, params.Candidate); err != nil {
		log.Printf("❌ Error adding ICE candidate: %v", err)
		c.sendError(req.ID, protocol.ErrCodeICEError, err.Error())
		return
	}

	c.sendSuccess(req.ID, protocol.IceCandidateResult{Success: true})
}

// notifyOthersAboutJoin уведомляет других участников о новом peer
func (c *Client) notifyOthersAboutJoin(peerID, displayName string) {
	if c.currentRoom == nil {
		return
	}

	for _, otherPeer := range c.currentRoom.GetPeers() {
		if otherPeer.ID == peerID {
			continue
		}

		// Отправляем уведомление через интерфейс SignalingClient
		if client, ok := otherPeer.GetClient().(*Client); ok {
			log.Printf("🔔 Notifying peer %s about new peer %s", otherPeer.ID, peerID)
			client.SendNotification(protocol.NotifyPeerJoined, protocol.PeerJoinedNotify{
				PeerID:      peerID,
				DisplayName: displayName,
			})
		}
	}
}

// notifyOthersAboutLeave уведомляет других участников об уходе peer
func (c *Client) notifyOthersAboutLeave(peerID string) {
	if c.currentRoom == nil {
		return
	}

	for _, otherPeer := range c.currentRoom.GetPeers() {
		if otherPeer.ID == peerID {
			continue
		}

		if client, ok := otherPeer.GetClient().(*Client); ok {
			client.SendNotification(protocol.NotifyPeerLeft, protocol.PeerLeftNotify{
				PeerID: peerID,
			})
		}
	}
}

// sendSuccess отправляет успешный ответ
func (c *Client) sendSuccess(id int, result interface{}) {
	resultJSON, _ := json.Marshal(result)
	c.conn.WriteResponse(protocol.Response{
		Jsonrpc: "2.0",
		ID:      id,
		Result:  resultJSON,
	})
}

// sendError отправляет ответ с ошибкой
func (c *Client) sendError(id int, code int, message string) {
	c.conn.WriteResponse(protocol.Response{
		Jsonrpc: "2.0",
		ID:      id,
		Error: &protocol.ResponseError{
			Code:    code,
			Message: message,
		},
	})
}

// cleanup очищает ресурсы при отключении клиента
func (c *Client) cleanup() {
	if c.peer != nil && c.currentRoom != nil {
		log.Printf("🧹 Cleaning up peer %s", c.peer.ID)

		// Уведомляем других
		c.notifyOthersAboutLeave(c.peer.ID)

		// Удаляем из комнаты
		c.currentRoom.RemovePeer(c.peer)

		// Если комната пустая, удаляем её
		if len(c.currentRoom.GetPeerIDs()) == 0 {
			c.roomManager.Remove(c.currentRoom.GetName())
		}
	}
}

func (c *Client) handleCameraState(req protocol.Request) {
	if c.peer == nil {
		c.sendError(req.ID, protocol.ErrCodeNotInRoom, "join room first")
		return
	}

	var params protocol.CameraStateParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		c.sendError(req.ID, protocol.ErrCodeInvalidParams, "invalid params")
		return
	}

	log.Printf("📹 Peer %s camera state: %v", c.peer.ID, params.Enabled)

	c.peer.SetCameraEnabled(params.Enabled)

	c.sendSuccess(req.ID, protocol.CameraStateResult{Success: true})
}
