package worker

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
)

func NewDeliveryWorkerID() (string, error) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "eventflow"
	}

	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generate delivery worker ID: %w", err)
	}

	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(suffix[:])), nil
}
