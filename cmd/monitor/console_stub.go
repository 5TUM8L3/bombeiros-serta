//go:build !windows
// +build !windows

package main

// hideConsoleWindow is a no-op on non-Windows platforms.
func hideConsoleWindow() {}
