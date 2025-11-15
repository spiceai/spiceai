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
	"log/slog"
	"net/http"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

func doRuntimeApiRequest[T interface{}](rtcontext *context.RuntimeContext, method, path string, body *string) (T, error) {
	var resp *http.Response
	var err error

	var reader io.Reader
	if body != nil {
		reader = strings.NewReader(*body)
	}

	switch method {
	case http.MethodGet:
		resp, err = rtcontext.Do(http.MethodGet, path, nil)
	case http.MethodDelete:
		resp, err = rtcontext.Do(http.MethodDelete, path, nil)
	case http.MethodPost:
		if body != nil {
			resp, err = rtcontext.Do(http.MethodPost, path, reader, "Content-Type", "application/json")
		} else {
			resp, err = rtcontext.Do(http.MethodPost, path, reader)
		}
	case http.MethodPatch:
		if body != nil {
			resp, err = rtcontext.Do(http.MethodPatch, path, reader, "Content-Type", "application/json")
		} else {
			resp, err = rtcontext.Do(http.MethodPatch, path, reader)
		}
	default:
		return *new(T), fmt.Errorf("unsupported method: %s", method)
	}

	if err != nil {
		if strings.HasSuffix(err.Error(), "connection refused") {
			return *new(T), rtcontext.RuntimeUnavailableError()
		}
		return *new(T), fmt.Errorf("error performing request to %s%s: %w", rtcontext.HttpEndpoint(), path, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Error("closing response body", "error", err)
		}
	}()

	if resp.StatusCode == http.StatusUnauthorized {
		return *new(T), fmt.Errorf("unauthorized: invalid or missing Spice API key")
	}

	if resp.StatusCode == http.StatusNotFound {
		bodyBytes, err := io.ReadAll(resp.Body)
		bodyString := ""
		if err == nil {
			bodyString = string(bodyBytes)
		}

		return *new(T), fmt.Errorf("not found: %s", bodyString)
	}

	var result T
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return *new(T), fmt.Errorf("error decoding response: %w", err)
	}
	return result, nil
}

func GetData[T interface{}](rtcontext *context.RuntimeContext, path string) ([]T, error) {
	result, err := doRuntimeApiRequest[[]T](rtcontext, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func GetDataSingle[T interface{}](rtcontext *context.RuntimeContext, path string) (T, error) {
	result, err := doRuntimeApiRequest[T](rtcontext, http.MethodGet, path, nil)
	if err != nil {
		return *new(T), err
	}
	return result, nil
}

func PostRuntime[T interface{}](rtcontext *context.RuntimeContext, path string, body *string) (T, error) {
	return doRuntimeApiRequest[T](rtcontext, http.MethodPost, path, body)
}

func PatchRuntime[T interface{}](rtcontext *context.RuntimeContext, path string, body *string) (T, error) {
	return doRuntimeApiRequest[T](rtcontext, http.MethodPatch, path, body)
}

func DeleteRuntime[T interface{}](rtcontext *context.RuntimeContext, path string) (T, error) {
	return doRuntimeApiRequest[T](rtcontext, http.MethodDelete, path, nil)
}

func WriteDataTable[T interface{}](rtcontext *context.RuntimeContext, path string, t T) error {

	items, err := doRuntimeApiRequest[[]T](rtcontext, http.MethodGet, path, nil)

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
