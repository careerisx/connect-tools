package web

import (
	"embed"
	"io/fs"
)

const distributionDirectory = "dist"

//go:embed all:dist
var embeddedAssets embed.FS

// Assets returns the Vite distribution embedded from internal/web/dist.
func Assets() fs.FS {
	assets, err := fs.Sub(embeddedAssets, distributionDirectory)
	if err != nil {
		panic("embedded web distribution is unavailable: " + err.Error())
	}
	return assets
}
