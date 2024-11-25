//go:build windows
// +build windows

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
