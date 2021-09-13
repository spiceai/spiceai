package http

import (
	"fmt"
	net_http "net/http"
	"runtime"

	"github.com/spiceai/spiceai/pkg/version"
)

var _userAgent string

func Get(url string) (*net_http.Response, error) {
	req, err := net_http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", userAgent())

	resp, err := net_http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func userAgent() string {
	if _userAgent == "" {
		_userAgent = fmt.Sprintf("Spice.ai/%s %s/%s (%s)", version.Version(), version.Component(), version.Version(), runtime.GOOS)
	}
	return _userAgent
}
