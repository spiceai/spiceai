package util

import (
	"bytes"
)

// Returns new hash if b has changed from a
func ComputeNewHash(a []byte, hashA []byte, b []byte) ([]byte, error) {
	if a == nil && b == nil {
		return nil, nil
	}

	if b == nil {
		return nil, nil
	}

	reader := bytes.NewReader(b)
	hashB, err := ComputeHash(reader)
	if err != nil {
		return nil, err
	}

	isSame := bytes.Compare(hashA, hashB) == 0
	if isSame {
		return nil, nil
	}

	return hashB, nil
}
