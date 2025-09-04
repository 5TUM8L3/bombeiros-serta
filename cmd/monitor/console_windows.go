//go:build windows

package main

import "syscall"

// hideConsoleWindow detaches from the console and hides any visible console window.
func hideConsoleWindow() {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	user32 := syscall.NewLazyDLL("user32.dll")
	freeConsole := kernel32.NewProc("FreeConsole")
	getConsoleWindow := kernel32.NewProc("GetConsoleWindow")
	showWindow := user32.NewProc("ShowWindow")

	// First, try to detach from any attached console so our process no longer owns/shares it.
	_, _, _ = freeConsole.Call()

	// Then, best-effort hide if a window is still associated.
	hwnd, _, _ := getConsoleWindow.Call()
	if hwnd != 0 {
		const SW_HIDE = 0
		showWindow.Call(hwnd, uintptr(SW_HIDE))
	}
}
