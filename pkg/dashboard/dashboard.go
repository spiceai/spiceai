package dashboard

import (
	"github.com/valyala/fasthttp"
)

type Dashboard interface {
	IndexHandler(ctx *fasthttp.RequestCtx)
	JsHandler(ctx *fasthttp.RequestCtx)
}
