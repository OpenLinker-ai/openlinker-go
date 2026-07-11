package openlinker

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
)

const maxIdempotencyKeyBytes = 255

var errInvalidIdempotencyKey = errors.New("openlinker: idempotency key must contain 1 to 255 printable ASCII bytes without surrounding spaces")

func resolveRunIdempotencyKey(explicit string) (string, error) {
	key := explicit
	if key == "" {
		random := make([]byte, 32)
		if _, err := rand.Read(random); err != nil {
			return "", fmt.Errorf("openlinker: generate idempotency key: %w", err)
		}
		key = "olrun_" + base64.RawURLEncoding.EncodeToString(random)
	}
	if err := validateIdempotencyKey(key); err != nil {
		return "", err
	}
	return key, nil
}

func validateIdempotencyKey(key string) error {
	if len(key) == 0 || len(key) > maxIdempotencyKeyBytes || key[0] == ' ' || key[len(key)-1] == ' ' {
		return errInvalidIdempotencyKey
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x20 || key[i] > 0x7e {
			return errInvalidIdempotencyKey
		}
	}
	return nil
}
