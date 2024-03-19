package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

func GetData[T interface{}](rtcontext *context.RuntimeContext, path string, t T) ([]T, error) {
	url := fmt.Sprintf("%s%s", rtcontext.HttpEndpoint(), path)
	resp, err := http.Get(url)
	if err != nil {
		if strings.HasSuffix(err.Error(), "connection refused") {
			return nil, rtcontext.RuntimeUnavailableError()
		}
		return nil, fmt.Errorf("error fetching %s: %w", url, err)
	}
	defer resp.Body.Close()

	var data []T
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, fmt.Errorf("error decoding items: %w", err)
	}
	return data, nil
}

func WriteDataTable[T interface{}](rtcontext *context.RuntimeContext, path string, t T) error {
	if data, err := GetData[T](rtcontext, path, t); err != nil {
		return err
	} else {
		table := make([]interface{}, len(data))
		for i, v := range data {
			table[i] = v
		}
		util.WriteTable(table)
	}
	return nil
}
