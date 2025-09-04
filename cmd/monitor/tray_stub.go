//go:build !windows
// +build !windows

package main

// StartTray is a no-op on non-Windows platforms; present to satisfy cross-platform builds.
func StartTray(onQuit func()) {
	// Not supported on this platform. If ever called, just invoke onQuit to exit gracefully.
	if onQuit != nil {
		onQuit()
	}
}
