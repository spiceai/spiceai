/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

// Package tracing provides utilities for W3C Trace Context propagation.
// See: https://www.w3.org/TR/trace-context/
package tracing

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// GenerateTraceID generates a random 128-bit trace ID as a 32-character hex string.
// The trace ID is suitable for use in W3C Trace Context traceparent headers.
func GenerateTraceID() string {
	bytes := make([]byte, 16)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// FormatTraceparent formats a W3C Trace Context traceparent header value.
// Format: version-traceid-parentid-flags
// Example: "00-{trace_id}-{parent_id}-01"
//
// The version is always "00" (current spec version).
// The parent_id is randomly generated as a 64-bit span ID.
// The flags are set to "01" indicating the trace is sampled.
func FormatTraceparent(traceID string) string {
	parentID := make([]byte, 8)
	_, _ = rand.Read(parentID)
	return fmt.Sprintf("00-%s-%s-01", traceID, hex.EncodeToString(parentID))
}
