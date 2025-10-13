package protocol

import "encoding/json"

// Base JSON-RPC 2.0 structures

type Request struct {
	Jsonrpc string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      int             `json:"id,omitempty"`
}

type Response struct {
	Jsonrpc string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *ResponseError  `json:"error,omitempty"`
	ID      int             `json:"id,omitempty"`
}

type ResponseError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Client -> Server
// ================

// Join room
type JoinRoomParams struct {
	Auth        string `json:"auth"`
	RoomID      string `json:"roomId"`
	PeerID      string `json:"peerId"`
	DisplayName string `json:"displayName"`
}

type JoinRoomResult struct {
	Success bool     `json:"success"`
	Peers   []string `json:"peers"`
}

// Send Publisher Offer
type PublisherOfferParams struct {
	SDP  string `json:"sdp"`
	Type string `json:"type"` // "offer"
}

type PublisherOfferResult struct {
	SDP  string `json:"sdp"`
	Type string `json:"type"` // "answer"
}

// Send Subscriber Answer
type SubscriberAnswerParams struct {
	SDP  string `json:"sdp"`
	Type string `json:"type"` // "answer"
}

type SubscriberAnswerResult struct {
	Success bool `json:"success"`
}

// Send ICE Candidate
type IceCandidateParams struct {
	Target        string `json:"target"` // "publisher" or "subscriber"
	Candidate     string `json:"candidate"`
	SDPMid        string `json:"sdpMid"`
	SDPMLineIndex uint16 `json:"sdpMLineIndex"`
}

type IceCandidateResult struct {
	Success bool `json:"success"`
}

// Leave room
type LeaveRoomParams struct {
	RoomID string `json:"roomId"`
}

type LeaveRoomResult struct {
	Success bool `json:"success"`
}

// Server -> Client
// ================

type SubscriberOfferNotify struct {
	SDP  json.RawMessage `json:"sdp"`
	Type string          `json:"type"` // "offer"
}

type ICECandidateNotify struct {
	Target        string `json:"target"` // "publisher" or "subscriber"
	Candidate     string `json:"candidate"`
	SDPMid        string `json:"sdpMid"`
	SDPMLineIndex uint16 `json:"sdpMLineIndex"`
}

type PeerJoinedNotify struct {
	PeerID      string `json:"peerId"`
	DisplayName string `json:"displayName"`
}

type PeerLeftNotify struct {
	PeerID string `json:"peerId"`
}

type ErrorNotify struct { // peer_disconnected, room_full, etc.
	Code    int    `json:"code"`
	Message string `json:"message"`
}

const (
	// JSON-RPC стандартные
	ErrCodeParseError     = -32700 // невалидный JSON
	ErrCodeInvalidRequest = -32600 // невалидный Request объект
	ErrCodeMethodNotFound = -32601 // метод не существует
	ErrCodeInvalidParams  = -32602 // невалидные параметры
	ErrCodeInternalError  = -32603 // внутренняя ошибка

	// Кастомные для SFU (начинаем с -32000)
	ErrCodeRoomNotFound  = -32000 // комната не найдена
	ErrCodeRoomFull      = -32001 // комната полна
	ErrCodePeerNotFound  = -32002 // участник не найден
	ErrCodeAlreadyInRoom = -32003 // уже в комнате
	ErrCodeNotInRoom     = -32004 // не в комнате
	ErrCodeSDPError      = -32005 // ошибка обработки SDP
	ErrCodeICEError      = -32006 // ошибка ICE
	ErrCodeMediaError    = -32007 // ошибка медиа-потока
)
