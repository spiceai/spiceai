package dashboard

import (
	"embed"
	"path"
	"path/filepath"
	"strings"

	"github.com/valyala/fasthttp"
)

// fasthttp doesn't yet support serving embed.FS files, so serve each file manually for now
// GitHub Isssue: https://github.com/valyala/fasthttp/issues/974
// More info on go:embed at https://pkg.go.dev/embed@master

//go:embed build/index.html
var contentIndexHtml []byte

//go:embed build/manifest.json
var contentManifestJson []byte

//go:embed build/static/js/*
var jsFiles embed.FS

//go:embed build/static/css/*
var cssFiles embed.FS

//go:embed build/static/media/*
var mediaFiles embed.FS

type DashboardEmbedded struct{}

func NewDashboardEmbedded() *DashboardEmbedded {
	return &DashboardEmbedded{}
}

func (d *DashboardEmbedded) IndexHandler(ctx *fasthttp.RequestCtx) {
	contentType := GetContentType("html")
	ctx.Response.Header.SetContentType(contentType)
	ctx.Response.SetBody(contentIndexHtml)
}

func (d *DashboardEmbedded) ManifestJsonHandler(ctx *fasthttp.RequestCtx) {
	contentType := GetContentType("json")
	ctx.Response.Header.SetContentType(contentType)
	ctx.Response.SetBody(contentManifestJson)
}

func (d *DashboardEmbedded) JsHandler(ctx *fasthttp.RequestCtx) {
	d.fileHandler(ctx, jsFiles, "js")
}

func (d *DashboardEmbedded) CssHandler(ctx *fasthttp.RequestCtx) {
	d.fileHandler(ctx, cssFiles, "css")
}

func (d *DashboardEmbedded) MediaHandler(ctx *fasthttp.RequestCtx) {
	ext := strings.TrimPrefix(path.Ext(ctx.UserValue("file").(string)), ".")
	d.fileHandler(ctx, mediaFiles, ext)
}

func (d *DashboardEmbedded) fileHandler(ctx *fasthttp.RequestCtx, fs embed.FS, fileType string) {
	filePath := ctx.UserValue("file").(string)

	subfolder := fileType
	if fileType == "svg" || fileType == ".md" {
		subfolder = "media"
	}

	fullFilePath := filepath.Join("build", "static", subfolder, filePath)

	fileContent, err := fs.ReadFile(fullFilePath)
	if err != nil {
		ctx.Response.SetStatusCode(404)
		ctx.Response.SetBody([]byte(err.Error()))
		return
	}

	contentType := GetContentType(fileType)

	ctx.Response.Header.SetContentType(contentType)
	ctx.Response.SetBody(fileContent)
}
