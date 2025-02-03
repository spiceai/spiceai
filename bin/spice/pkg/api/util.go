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

package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

const (
	GET  = "GET"
	POST = "POST"
)

func doRuntimeApiRequest[T interface{}](rtcontext *context.RuntimeContext, method, path string, body *string) (T, error) {
	url := fmt.Sprintf("%s%s", rtcontext.HttpEndpoint(), path)
	var resp *http.Response
	var err error

	switch method {
	case GET:
		var request *http.Request
		request, err = http.NewRequest("GET", url, nil)
		if err != nil {
			return *new(T), fmt.Errorf("error creating request: %w", err)
		}
		headers := rtcontext.GetHeaders()
		for key, value := range headers {
			request.Header.Set(key, value)
		}
		request.Header.Set("Content-Type", "application/json")
		resp, err = rtcontext.Client().Do(request)
	case POST:
		var reader io.Reader
		var request *http.Request
		if body != nil {
			reader = strings.NewReader(*body)
		}
		request, err = http.NewRequest("POST", url, reader)
		if err != nil {
			return *new(T), fmt.Errorf("error creating request: %w", err)
		}
		headers := rtcontext.GetHeaders()
		for key, value := range headers {
			request.Header.Set(key, value)
		}
		request.Header.Set("Content-Type", "application/json")
		resp, err = rtcontext.Client().Do(request)
	default:
		return *new(T), fmt.Errorf("unsupported method: %s", method)
	}

	if err != nil {
		if strings.HasSuffix(err.Error(), "connection refused") {
			return *new(T), rtcontext.RuntimeUnavailableError()
		}
		return *new(T), fmt.Errorf("error performing request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return *new(T), fmt.Errorf("Unauthorized")
	}

	var result T
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return *new(T), fmt.Errorf("error decoding response: %w", err)
	}
	return result, nil
}

func GetData[T interface{}](rtcontext *context.RuntimeContext, path string) ([]T, error) {
	result, err := doRuntimeApiRequest[[]T](rtcontext, GET, path, nil)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func GetDataSingle[T interface{}](rtcontext *context.RuntimeContext, path string) (T, error) {
	result, err := doRuntimeApiRequest[T](rtcontext, GET, path, nil)
	if err != nil {
		return *new(T), err
	}
	return result, nil
}

func PostRuntime[T interface{}](rtcontext *context.RuntimeContext, path string, body *string) (T, error) {
	return doRuntimeApiRequest[T](rtcontext, POST, path, body)
}

func WriteDataTable[T interface{}](rtcontext *context.RuntimeContext, path string, t T) error {

	items, err := doRuntimeApiRequest[[]T](rtcontext, GET, path, nil)

	if err != nil {
		return fmt.Errorf("error fetching runtime information: %w", err)
	}

	var table []interface{}
	for _, s := range items {
		table = append(table, s)
	}

	util.WriteTable(table)

	return nil
}
