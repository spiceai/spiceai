/*
Copyright 2024 The Spice.ai OSS Authors

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
	"net/http"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

const (
	GET  = "GET"
	POST = "POST"
)

func doRuntimeApiRequest[T interface{}](rtcontext *context.RuntimeContext, method, path string) (T, error) {
	url := fmt.Sprintf("%s%s", rtcontext.HttpEndpoint(), path)
	var resp *http.Response
	var err error

	switch method {
	case GET:
		resp, err = http.Get(url)
	case POST:
		resp, err = http.Post(url, "application/json", nil)
	default:
		return *new(T), fmt.Errorf("Unsupported method: %s", method)
	}

	if err != nil {
		if strings.HasSuffix(err.Error(), "connection refused") {
			return *new(T), rtcontext.RuntimeUnavailableError()
		}
		return *new(T), fmt.Errorf("Error performing request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	var result T
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return *new(T), fmt.Errorf("Error decoding response: %w", err)
	}
	return result, nil
}

func GetData[T interface{}](rtcontext *context.RuntimeContext, path string) ([]T, error) {
	result, err := doRuntimeApiRequest[[]T](rtcontext, GET, path)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func DoRuntimePostRequest[T interface{}](rtcontext *context.RuntimeContext, path string) (T, error) {
	return doRuntimeApiRequest[T](rtcontext, POST, path)
}

func WriteDataTable[T interface{}](rtcontext *context.RuntimeContext, path string, t T) error {

	items, err := doRuntimeApiRequest[[]T](rtcontext, GET, path)

	if err != nil {
		return fmt.Errorf("Error fetching runtime information: %w", err)
	}

	var table []interface{}
	for _, s := range items {
		table = append(table, s)
	}

	util.WriteTable(table)

	return nil
}
