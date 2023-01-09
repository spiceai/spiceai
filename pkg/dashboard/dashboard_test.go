package dashboard_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fasthttp/router"
	"github.com/spiceai/spiceai/pkg/dashboard"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func TestDashboard(t *testing.T) {
	t.Run("DashboardIndexHandler() - GET returns dashboard content", testDashboardIndexHandler())
	t.Run("DashboardJsHandler() - GET returns JS content", testDashboardJsHandler())
	t.Run("DashboardCssHandler() - GET returns CSS content", testDashboardCssHandler())
	t.Run("DashboardMediaHandler() - GET returns SVG content", testDashboardMediaHandler("svg", "image/svg+xml"))
}

func testDashboardIndexHandler() func(*testing.T) {
	return func(t *testing.T) {
		r, err := http.NewRequest("GET", "http://test/", nil)
		assert.NoError(t, err)

		server := dashboard.NewDashboardEmbedded()

		res, err := serve("/", server.IndexHandler, r)
		assert.NoError(t, err)

		assert.EqualValues(t, 200, res.StatusCode)
		assert.EqualValues(t, "text/html; charset=utf-8", res.Header.Get("Content-Type"))

		body, err := io.ReadAll(res.Body)
		assert.NoError(t, err)

		assert.Contains(t, string(body), "Spice.ai")
	}
}

func testDashboardJsHandler() func(*testing.T) {
	return func(t *testing.T) {
		url, err := getFirstStaticAssetUrl("js")
		if err != nil {
			t.Error(err)
		}

		r, err := http.NewRequest("GET", url, nil)
		assert.NoError(t, err)

		server := dashboard.NewDashboardEmbedded()

		res, err := serve("/js/{file}", server.JsHandler, r)
		assert.NoError(t, err)

		assert.EqualValues(t, 200, res.StatusCode)
		assert.EqualValues(t, "application/javascript", res.Header.Get("Content-Type"))

		body, err := io.ReadAll(res.Body)
		assert.NoError(t, err)

		assert.Contains(t, string(body), "chunk.js")
	}
}

func testDashboardCssHandler() func(*testing.T) {
	return func(t *testing.T) {
		url, err := getFirstStaticAssetUrl("css")
		if err != nil {
			t.Error(err)
		}

		r, err := http.NewRequest("GET", url, nil)
		assert.NoError(t, err)

		server := dashboard.NewDashboardEmbedded()

		res, err := serve("/css/{file}", server.CssHandler, r)
		assert.NoError(t, err)

		assert.EqualValues(t, 200, res.StatusCode)
		assert.EqualValues(t, "text/css; charset=utf-8", res.Header.Get("Content-Type"))

		body, err := io.ReadAll(res.Body)
		assert.NoError(t, err)

		assert.Contains(t, string(body), ".css")
	}
}

func testDashboardMediaHandler(mediaType string, contentType string) func(*testing.T) {
	return func(t *testing.T) {
		url, err := getFirstStaticAssetUrl(mediaType)
		if err != nil {
			t.Error(err)
		}

		r, err := http.NewRequest("GET", url, nil)
		assert.NoError(t, err)

		server := dashboard.NewDashboardEmbedded()

		res, err := serve("/media/{file}", server.MediaHandler, r)
		assert.NoError(t, err)

		assert.EqualValues(t, 200, res.StatusCode)
		assert.EqualValues(t, contentType, res.Header.Get("Content-Type"))

		body, err := io.ReadAll(res.Body)
		assert.NoError(t, err)

		assert.Contains(t, string(body), mediaType)
	}
}

func serve(route string, handler fasthttp.RequestHandler, req *http.Request) (*http.Response, error) {
	r := router.New()
	r.GET(route, handler)

	ln := fasthttputil.NewInmemoryListener()
	defer ln.Close()

	go func() {
		err := fasthttp.Serve(ln, r.Handler)
		if err != nil {
			panic(fmt.Errorf("Failed to serve: %v", err))
		}
	}()

	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return ln.Dial()
			},
		},
	}

	return client.Do(req)
}

func getFirstStaticAssetUrl(typeName string) (string, error) {
	directoryName := typeName
	if typeName == "svg" || typeName == "md" {
		directoryName = "media"
	}

	path := filepath.Join("build", "static", directoryName)
	assets, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}
	if len(assets) == 0 {
		return "", fmt.Errorf("Expected %s assets in static directory. Is dashboard built?", typeName)
	}

	// Look for first matching chunk
	var filename string
	var suffix string
	if typeName == "js" {
		suffix = fmt.Sprintf(".chunk.%s", typeName)
	} else {
		suffix = fmt.Sprintf(".%s", typeName)
	}

	for _, asset := range assets {
		if strings.HasSuffix(asset.Name(), suffix) {
			filename = asset.Name()
			break
		}
	}

	return fmt.Sprintf("http://test/%s/%s", directoryName, filepath.Base(filename)), nil
}
