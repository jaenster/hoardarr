//go:build !embed

package hoardarr

import (
	"os"
)

func init() {
	if info, err := os.Stat("frontend/dist"); err == nil && info.IsDir() {
		FrontendFS = os.DirFS("frontend/dist")
	}
}
