package hoardarr

import "io/fs"

// FrontendFS is the filesystem holding the built React app.
//
// Populated by one of:
//   - assets_embed.go (build tag `embed`): from go:embed of frontend/dist
//   - assets_dev.go   (default):           from disk if frontend/dist exists, else nil
//
// When nil, the server serves a dev placeholder explaining how to build.
var FrontendFS fs.FS
