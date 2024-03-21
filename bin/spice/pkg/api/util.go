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
