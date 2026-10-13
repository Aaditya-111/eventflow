package security

import (
	"bytes"
	"errors"
	"testing"
)

func testKey() []byte {
	return bytes.Repeat([]byte{7}, 32)
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	manager, err := NewSecretManager(testKey())
	if err != nil {
		t.Fatal(err)
	}

	original := []byte("webhook-signing-secret")
	encrypted, err := manager.Encrypt(original)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(encrypted, original) {
		t.Fatal("encrypted data must not equal plaintext")
	}

	decrypted, err := manager.Decrypt(encrypted)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decrypted, original) {
		t.Fatal("decrypted secret does not match original")
	}
}

func TestEncryptUsesDifferentNonces(t *testing.T) {
	manager, err := NewSecretManager(testKey())
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("same-secret")
	first, err := manager.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(first, second) {
		t.Fatal("encrypting the same secret twice should produce different ciphertext")
	}
}

func TestNewSecretManagerRejectsInvalidKeyLength(t *testing.T) {
	_, err := NewSecretManager([]byte("too-short"))
	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
}

func TestDecryptRejectsTamperedCiphertext(t *testing.T) {
	manager, err := NewSecretManager(testKey())
	if err != nil {
		t.Fatal(err)
	}

	encrypted, err := manager.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}

	encrypted[len(encrypted)-1] ^= 0xff

	_, err = manager.Decrypt(encrypted)
	if !errors.Is(err, ErrInvalidCiphertext) {
		t.Fatalf("expected ErrInvalidCiphertext, got %v", err)
	}
}

func TestDecryptRejectsTruncatedCiphertext(t *testing.T) {
	manager, err := NewSecretManager(testKey())
	if err != nil {
		t.Fatal(err)
	}

	_, err = manager.Decrypt([]byte{1, 2, 3})
	if !errors.Is(err, ErrInvalidCiphertext) {
		t.Fatalf("expected ErrInvalidCiphertext, got %v", err)
	}
}
