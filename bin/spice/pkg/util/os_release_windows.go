//go:build windows
// +build windows

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
	"unsafe"

	"golang.org/x/sys/windows"
)

func GetOSRelease() string {
	// Define the structure that will hold the version info
	type OsVersionInfoExW struct {
		DwOSVersionInfoSize uint32
		DwMajorVersion      uint32
		DwMinorVersion      uint32
		DwBuildNumber       uint32
		DwPlatformId        uint32
		SzCSDVersion        [128]uint16
	}

	// Load the ntdll.dll using the windows package
	ntdll := windows.NewLazySystemDLL("ntdll.dll")
	rtlGetVersion := ntdll.NewProc("RtlGetVersion")

	var osVersion OsVersionInfoExW
	osVersion.DwOSVersionInfoSize = uint32(unsafe.Sizeof(osVersion))

	// Call the RtlGetVersion function
	_, _, _ = rtlGetVersion.Call(uintptr(unsafe.Pointer(&osVersion)))

	// Format the version information
	return fmt.Sprintf("%d.%d.%d", osVersion.DwMajorVersion, osVersion.DwMinorVersion, osVersion.DwBuildNumber)
}
