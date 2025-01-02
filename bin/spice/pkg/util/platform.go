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
	"fmt"
	"runtime"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/version"
)

func IsWindows() bool {
	return runtime.GOOS == "windows"
}

func GetSpiceUserAgent(client string) string {
	// get OS type, release and machine type
	// get Go version for SDK version

	if client == "" {
		client = "spice"
	}

	osType := runtime.GOOS
	switch osType {
	case "darwin":
		osType = "Darwin"
	case "linux":
		osType = "Linux"
	case "windows":
		osType = "Windows"
	case "freebsd":
		osType = "FreeBSD"
	case "openbsd":
		osType = "OpenBSD"
	case "android":
		osType = "Android"
	case "ios":
		osType = "iOS"
	}

	osMachine := runtime.GOARCH
	switch osMachine {
	case "amd64":
		osMachine = "x86_64"
	case "386":
		osMachine = "i386"
	case "arm64":
		osMachine = "aarch64"
	}

	osVersion := GetOSRelease()

	userAgent := fmt.Sprintf("%s/%s (%s/%s %s)", client, version.Version(), osType, osVersion, osMachine)

	// strip any non-printable ASCII characters
	return RemoveNonPrintableASCII(userAgent)
}

func RemoveNonPrintableASCII(str string) string {
	var builder strings.Builder

	for _, ch := range str {
		if ch >= 32 && ch <= 126 { // printable ASCII characters range from 32 to 126
			builder.WriteRune(ch)
		}
	}

	return builder.String()
}
