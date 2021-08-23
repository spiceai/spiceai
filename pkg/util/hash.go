package util

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

func ComputeHash(reader io.Reader) ([]byte, error) {
	h := sha256.New()
	if _, err := io.Copy(h, reader); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

func ComputeFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}

	defer file.Close()

	hash, err := ComputeHash(file)
	if err != nil {
		return "", err
	}

	hashInBytes := hash[:16]
	encodedHash := hex.EncodeToString(hashInBytes)

	return encodedHash, nil
}
