//go:build embed

package hoardarr

import (
	"embed"
	"io/fs"
)

//go:embed all:frontend/dist
var embeddedFS embed.FS

func init() {
	sub, err := fs.Sub(embeddedFS, "frontend/dist")
	if err != nil {
		panic(err)
	}
	FrontendFS = sub
}
