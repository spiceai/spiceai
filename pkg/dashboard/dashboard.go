package dashboard

import (
	"github.com/valyala/fasthttp"
)

type Dashboard interface {
	IndexHandler(ctx *fasthttp.RequestCtx)
	ManifestJsonHandler(ctx *fasthttp.RequestCtx)
	JsHandler(ctx *fasthttp.RequestCtx)
	CssHandler(ctx *fasthttp.RequestCtx)
	MediaHandler(ctx *fasthttp.RequestCtx)
}

func GetContentType(fileType string) string {
	switch fileType {
	case "html":
		return "text/html; charset=utf-8"
	case "js":
		return "application/javascript"
	case "json":
		return "application/json"
	case "css":
		return "text/css; charset=utf-8"
	case "svg":
		return "image/svg+xml"
	case "text":
		return "text/plain; charset=utf-8"
	case "md":
		return "text/markdown; charset=utf-8"
	}
	return ""
}
