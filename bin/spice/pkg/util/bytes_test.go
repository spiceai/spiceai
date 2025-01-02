/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
