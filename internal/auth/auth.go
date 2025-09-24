package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

const ACCESS_KEY = "access_key"

const JWTAlg = "HS256"

type ACLOperation string

const ALLOW_CREATE ACLOperation = "allow_create"
const ALLOW_READ ACLOperation = "allow_read"
const ALLOW_UPDATE ACLOperation = "allow_update"
const ALLOW_DELETE ACLOperation = "allow_delete"

type JwtHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ"`
}

type JwtPayload struct {
	Issuer    string `json:"iss"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
	Subject   string `json:"sub"`
}

var GetAccessKey = func(ctx context.Context) (string, bool) {
	s, ok := ctx.Value(ACCESS_KEY).(string)
	return s, ok
}

func UnmarshalJWT(jwt string) (*JwtHeader, *JwtPayload, error) {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return nil, nil, errors.New("invalid jwt")
	}
	headerString, payloadString, _ := parts[0], parts[1], parts[2]

	headerBytes, err := base64.RawURLEncoding.DecodeString(headerString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode header: %w", err)
	}
	var header JwtHeader
	err = json.Unmarshal(headerBytes, &header)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal header: %w", err)
	}

	if header.Alg != JWTAlg {
		return nil, nil, fmt.Errorf("invalid alg: %s, must be %s", header.Alg, JWTAlg)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(payloadString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode payload: %w", err)
	}
	var payload JwtPayload
	err = json.Unmarshal(payloadBytes, &payload)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	return &header, &payload, nil

}

func VerifyJWT(hs256key string, jwtstring string) error {
	token, err := jwt.Parse(jwtstring, func(token *jwt.Token) (interface{}, error) {
		// Validate the signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v, expected %s", token.Header["alg"], JWTAlg)
		}
		if strings.ToUpper(token.Header["alg"].(string)) != JWTAlg {
			return nil, fmt.Errorf("unexpected signing method: %v, expected %s", token.Header["alg"], JWTAlg)
		}
		return hs256key, nil
	})

	// Check for parsing errors, which includes signature validation
	if err != nil {
		return fmt.Errorf("token parsing and validation error: %w", err)
	}

	// Check if the token is valid (this is already done by Parse, but it's a good practice to check)
	if !token.Valid {
		return fmt.Errorf("token is invalid")
	}

	return nil
}
