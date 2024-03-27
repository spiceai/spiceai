/*
Copyright 2021-2024 The Spice.ai OSS Authors

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

func GetData[T interface{}](rtcontext *context.RuntimeContext, path string) ([]T, error) {
	url := fmt.Sprintf("%s%s", rtcontext.HttpEndpoint(), path)
	resp, err := http.Get(url)
	if err != nil {
		if strings.HasSuffix(err.Error(), "connection refused") {
			return nil, rtcontext.RuntimeUnavailableError()
		}
		return nil, fmt.Errorf("error fetching %s: %w", url, err)
	}
	defer resp.Body.Close()

	var components []T
	err = json.NewDecoder(resp.Body).Decode(&components)
	if err != nil {
		return nil, fmt.Errorf("error decoding items: %w", err)
	}
	return components, nil
}

func WriteDataTable[T interface{}](rtcontext *context.RuntimeContext, path string, t T) error {
	url := fmt.Sprintf("%s%s", rtcontext.HttpEndpoint(), path)
	resp, err := http.Get(url)
	if err != nil {
		if strings.HasSuffix(err.Error(), "connection refused") {
			return rtcontext.RuntimeUnavailableError()
		}
		return fmt.Errorf("Error fetching %s: %w", url, err)
	}
	defer resp.Body.Close()

	var datasets []T
	err = json.NewDecoder(resp.Body).Decode(&datasets)
	if err != nil {
		return fmt.Errorf("Error decoding items: %w", err)
	}

	var table []interface{}
	for _, s := range datasets {
		table = append(table, s)
	}

	util.WriteTable(table)

	return nil
}
