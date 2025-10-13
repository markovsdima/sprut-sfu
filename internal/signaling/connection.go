package signaling

// Обёртка над WebSocket соединением

import (
	"log"
	"simple-sfu/pkg/protocol"
	"sync"

	"github.com/gorilla/websocket"
)

type Connection struct {
	PeerID  string
	WS      *websocket.Conn
	writeMu sync.Mutex
}

func NewConnection(peerID string, ws *websocket.Conn) *Connection {
	return &Connection{
		PeerID: peerID,
		WS:     ws,
	}
}

func (c *Connection) ReadLoop(handle func([]byte)) {
	for {
		_, msg, err := c.WS.ReadMessage()
		if err != nil {
			log.Printf("connection closed for peer %s: %v", c.PeerID, err)
			break
		}
		handle(msg)
	}
}

// Отправка JSON-RPC ответа или уведомления
// ---

func (c *Connection) WriteResponse(resp protocol.Response) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.WS.WriteJSON(resp); err != nil {
		log.Printf("write response error: %v", err)
	}
}

func (c *Connection) WriteNotify(notify any) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.WS.WriteJSON(notify); err != nil {
		log.Printf("write notify error: %v", err)
	}
}
