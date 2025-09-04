//go:build windows

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/getlantern/systray"
)

// StartTray starts a minimal Windows system tray with a Quit option.
func StartTray(onQuit func()) {
	systray.Run(func() {
		systray.SetTitle("Bombeiros Monitor")
		systray.SetTooltip("Monitor de ocorrências — a correr em segundo plano")
		mQuit := systray.AddMenuItem("Sair", "Fechar o monitor")
		go func() {
			for {
				select {
				case <-mQuit.ClickedCh:
					if onQuit != nil {
						onQuit()
					}
					systray.Quit()
					return
				case <-time.After(24 * time.Hour):
					// keep goroutine alive
				}
			}
		}()
	}, func() {
		fmt.Fprintln(os.Stderr, "Tray terminated")
	})
}
