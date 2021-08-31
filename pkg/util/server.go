package util

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
)

func IsRuntimeServerHealthy(serverBaseUrl string, httpClient *http.Client) error {
	url := fmt.Sprintf("%s/health", serverBaseUrl)
	resp, err := httpClient.Get(url)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil || string(body) != "ok" {
		return errors.New(string(body))
	}

	return nil
}

func IsAIEngineServerHealthy(client aiengine_pb.AIEngineClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := client.GetHealth(ctx, &aiengine_pb.HealthRequest{})
	if err != nil {
		return err
	}

	if resp.Error {
		return errors.New(resp.Result)
	}

	if resp.Result != "ok" {
		return errors.New(resp.Result)
	}

	return nil
}
