package signaling

import (
	"log"
	"net/http"
	"simple-sfu/internal/config"
	"simple-sfu/internal/room"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type Server struct {
	roomManager *room.Manager
	upgrader    websocket.Upgrader
	config      *config.Config
}

func NewServer(roomManager *room.Manager, upgrader websocket.Upgrader, cfg *config.Config) *Server {
	return &Server{
		roomManager: roomManager,
		upgrader:    upgrader,
		config:      cfg,
	}
}

func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Установка соединения
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	// Создаём connection wrapper
	peerID := r.URL.Query().Get("peerId")
	if peerID == "" {
		peerID = generatePeerID()
	}

	connection := NewConnection(peerID, conn)

	// Создаём клиент для обработки бизнес-логики
	client := NewClient(connection, s.roomManager, s.config)

	defer client.cleanup()

	// Обрабатываем сообщения через клиент
	connection.ReadLoop(client.HandleMessage)
}

// generatePeerID генерирует уникальный ID для peer'а
func generatePeerID() string {
	return uuid.New().String()
}
