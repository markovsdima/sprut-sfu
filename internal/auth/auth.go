package auth

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

type TokenVerifier struct {
	secretKey []byte
}

func NewTokenVerifier(secret string) *TokenVerifier {
	return &TokenVerifier{
		secretKey: []byte(secret),
	}
}

type Claims struct {
	jwt.RegisteredClaims
	RoomID string `json:"room_uid"`
	PeerID string `json:"user_uid"`
}

func (tv *TokenVerifier) Verify(tokenStr string, roomID string, peerID string) error {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return tv.secretKey, nil
	})
	if err != nil {
		return fmt.Errorf("token parse error: %w", err)
	}

	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		if claims.RoomID != roomID {
			return fmt.Errorf("room ID mismatch")
		}
		if claims.PeerID != peerID {
			return fmt.Errorf("peer ID mismatch")
		}
		return nil
	} else {
		return fmt.Errorf("invalid token claims")
	}
}
