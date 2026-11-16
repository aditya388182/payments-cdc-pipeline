package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
)

func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return b
}

func randomID() string { return hex.EncodeToString(randomBytes(12)) }

// RandomToken returns a URL-safe random string with n bytes of entropy.
func RandomToken(n int) string { return base64.RawURLEncoding.EncodeToString(randomBytes(n)) }

// HashToken is how secrets (refresh tokens, one-time codes) are stored: only their SHA-256, never the value.
func HashToken(t string) string {
	h := sha256.Sum256([]byte(t))
	return hex.EncodeToString(h[:])
}
