package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

var (
	ErrInvalidKey        = errors.New("encryption key must be exactly 32 bytes")
	ErrInvalidCiphertext = errors.New("invalid encrypted secret")
)

// SecretManager encrypts and decrypts webhook signing secrets.
type SecretManager struct {
	aead cipher.AEAD
}

// NewSecretManager creates a manager using a 32-byte AES-256 key.
// The caller is responsible for securely loading and storing the key.
func NewSecretManager(key []byte) (*SecretManager, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKey
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create AES-GCM: %w", err)
	}

	return &SecretManager{aead: aead}, nil
}

// Encrypt protects a secret with AES-256-GCM.
// The nonce is prepended to the ciphertext.
func (m *SecretManager) Encrypt(plaintext []byte) ([]byte, error) {
	if m == nil || m.aead == nil {
		return nil, errors.New("secret manager is not configured")
	}

	nonce := make([]byte, m.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate encryption nonce: %w", err)
	}

	ciphertext := m.aead.Seal(nil, nonce, plaintext, nil)
	result := make([]byte, 0, len(nonce)+len(ciphertext))
	result = append(result, nonce...)
	result = append(result, ciphertext...)

	return result, nil
}

// Decrypt authenticates and decrypts a previously encrypted secret.
func (m *SecretManager) Decrypt(encrypted []byte) ([]byte, error) {
	if m == nil || m.aead == nil {
		return nil, errors.New("secret manager is not configured")
	}

	nonceSize := m.aead.NonceSize()
	if len(encrypted) < nonceSize+m.aead.Overhead() {
		return nil, ErrInvalidCiphertext
	}

	nonce := encrypted[:nonceSize]
	ciphertext := encrypted[nonceSize:]

	plaintext, err := m.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, ErrInvalidCiphertext
	}

	return plaintext, nil
}
