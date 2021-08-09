package dashboard_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"testing"

	"github.com/fasthttp/router"
	"github.com/spiceai/spice/pkg/dashboard"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func TestDashboard(t *testing.T) {
	t.Run("DashboardIndexHandler() - GET returns dashboard content", testDashboardIndexHandler())
	t.Run("DashboardAppHandler() - GET returns JS content", testDashboardAppHandler())
}

func testDashboardIndexHandler() func(*testing.T) {
	return func(t *testing.T) {
		r, err := http.NewRequest("GET", "http://test/", nil)
		assert.NoError(t, err)

		res, err := serve("/", dashboard.DashboardIndexHandler, r)
		assert.NoError(t, err)

		body, err := ioutil.ReadAll(res.Body)
		assert.NoError(t, err)

		assert.Contains(t, string(body), "Spice AI")
	}
}

func testDashboardAppHandler() func(*testing.T) {
	return func(t *testing.T) {
		r, err := http.NewRequest("GET", "http://test/js/app.js", nil)
		assert.NoError(t, err)

		res, err := serve("/js/{jsFile}", dashboard.DashboardAppHandler, r)
		assert.NoError(t, err)

		body, err := ioutil.ReadAll(res.Body)
		assert.NoError(t, err)

		assert.EqualValues(t, 200, res.StatusCode)
		assert.EqualValues(t, "application/javascript", res.Header.Get("Content-Type"))
		assert.Contains(t, string(body), "SPICE_VERSION")
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
