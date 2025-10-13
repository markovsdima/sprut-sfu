package main

import (
	"log"
	"net/http"
	"os"
	"simple-sfu/internal/config"
	"simple-sfu/internal/room"
	"simple-sfu/internal/signaling"
	"strconv"

	"github.com/gorilla/websocket"
)

func main() {
	cfg := config.MustLoad()
	roomManager := room.NewManager()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	signalingServer := signaling.NewServer(roomManager, upgrader, cfg)

	// PORT env var overrides config
	addr := cfg.Server.Host + ":" + strconv.Itoa(cfg.Server.Port)
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}

	http.HandleFunc("/ws", signalingServer.HandleWebSocket)

	log.Printf("🚀 SFU Server starting on %s", addr)
	log.Printf("📋 Config loaded: Max participants per room: %d", cfg.Rooms.MaxParticipants)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("❌ ListenAndServe failed: %v", err)
	}
}
