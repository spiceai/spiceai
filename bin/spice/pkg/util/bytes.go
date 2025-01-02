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

	isSame := bytes.Equal(hashA, hashB)
	if isSame {
		return nil, nil
	}

	return hashB, nil
}
