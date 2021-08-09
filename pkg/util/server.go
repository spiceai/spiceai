package util

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

func IsServerHealthy(serverBaseUrl string, httpClient *http.Client) error {
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
