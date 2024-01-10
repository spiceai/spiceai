package util

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type computeNewHashTestCase struct {
	a              []byte
	b              []byte
	expectedResult string
}

func TestBytes(t *testing.T) {
	testCases := []computeNewHashTestCase{
		{
			[]byte("hello"),
			[]byte("hello"),
			"",
		},
		{
			nil,
			nil,
			"",
		},
		{
			[]byte("hello"),
			nil,
			"",
		},
		{
			nil,
			[]byte("hello"),
			"2cf24dba5fb0a30e",
		},
		{
			[]byte("hello"),
			[]byte("hello, world"),
			"09ca7e4eaa6e8ae9",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("ComputeNewHash() %s %s -> %s", string(tc.a), string(tc.b), tc.expectedResult), testComputeNewHashFunc(tc))
	}
}

// Tests "ComputeNewHash()"
func testComputeNewHashFunc(tc computeNewHashTestCase) func(*testing.T) {
	return func(t *testing.T) {
		reader := bytes.NewReader(tc.a)
		aHash, err := ComputeHash(reader)
		if err != nil {
			t.Error(err)
		}

		result, err := ComputeNewHash(tc.a, aHash, tc.b)
		assert.NoError(t, err)
		encoded := ""
		if result != nil {
			encoded = hex.EncodeToString(result[:8])
		}
		assert.Equal(t, tc.expectedResult, encoded)
	}
}
