package llmr

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var DashboardAssets embed.FS

func DashboardHandler() http.Handler {
	sub, err := fs.Sub(DashboardAssets, "static")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}
