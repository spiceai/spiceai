package http

import (
	"fmt"
	"io/ioutil"
	"log"
	net_http "net/http"
	"runtime"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/spiceai/spiceai/pkg/version"
)

var _userAgent string
var client *retryablehttp.Client

func RetryableClient() *retryablehttp.Client {
	if client == nil {
		client = retryablehttp.NewClient()
		client.Logger = log.New(ioutil.Discard, "", 0)
	}
	return client
}

func Get(url string, accept string) (*net_http.Response, error) {
	req, err := retryablehttp.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	return do(req, accept)
}

func do(req *retryablehttp.Request, accept string) (*net_http.Response, error) {
	req.Header.Set("User-Agent", userAgent())
	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	resp, err := RetryableClient().Do(req)
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
