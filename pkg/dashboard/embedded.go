package dashboard

import (
	"embed"
	"path/filepath"

	"github.com/valyala/fasthttp"
)

// fasthttp doesn't yet support serving embed.FS files, so serve each file manually for now
// GitHub Isssue: https://github.com/valyala/fasthttp/issues/974
// More info on go:embed at https://pkg.go.dev/embed@master

//go:embed html/index.html
var contentIndexHtml []byte

//go:embed js/*
var jsFiles embed.FS

type DashboardEmbedded struct{}

func NewDashboardEmbedded() *DashboardEmbedded {
	return &DashboardEmbedded{}
}

func (d *DashboardEmbedded) IndexHandler(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.SetContentType("text/html")
	ctx.Response.SetBody(contentIndexHtml)
}

func (d *DashboardEmbedded) JsHandler(ctx *fasthttp.RequestCtx) {
	jsFile := ctx.UserValue("jsFile").(string)

	jsFileContent, err := jsFiles.ReadFile(filepath.Join("js", jsFile))
	if err != nil {
		ctx.Response.SetStatusCode(500)
		ctx.Response.SetBody([]byte(err.Error()))
		return
	}

	ctx.Response.Header.SetContentType("application/javascript")
	ctx.Response.SetBody(jsFileContent)
}
