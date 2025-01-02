/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package http

import (
	"fmt"
	"io"
	"log"
	net_http "net/http"
	"runtime"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/spiceai/spiceai/bin/spice/pkg/version"
)

var _userAgent string
var client *retryablehttp.Client

func RetryableClient() *retryablehttp.Client {
	if client == nil {
		client = retryablehttp.NewClient()
		client.Logger = log.New(io.Discard, "", 0)
	}
	return client
}

func Get(url string, accept string, headers map[string]string) (*net_http.Response, error) {
	req, err := retryablehttp.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	return do(req, accept, headers)
}

func do(req *retryablehttp.Request, accept string, headers map[string]string) (*net_http.Response, error) {
	req.Header.Set("User-Agent", userAgent())
	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := RetryableClient().Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func userAgent() string {
	if _userAgent == "" {
		_userAgent = fmt.Sprintf("Spice.ai/spice %s/%s (%s)", version.Version(), version.Version(), runtime.GOOS)
	}
	return _userAgent
}
