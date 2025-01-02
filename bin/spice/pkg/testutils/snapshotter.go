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

package testutils

import (
	"encoding/json"

	"github.com/bradleyjkemp/cupaloy"
)

type Snapshotter struct {
	config *cupaloy.Config
}

func NewSnapshotter(subdirectory string) *Snapshotter {
	return &Snapshotter{
		config: cupaloy.New(cupaloy.SnapshotSubdirectory(subdirectory)),
	}
}

func (s Snapshotter) SnapshotT(t cupaloy.TestingT, i ...interface{}) {
	s.config.SnapshotT(t, i...)
}

func (s Snapshotter) SnapshotTJson(t cupaloy.TestingT, i interface{}) {
	json, err := getJson(i)
	if err != nil {
		t.Fatal(err)
	}
	s.config.SnapshotT(t, json)
}

func (s Snapshotter) SnapshotMultiJson(name string, i interface{}) error {
	json, err := getJson(i)
	if err != nil {
		return err
	}
	return s.config.SnapshotMulti(name+".json", json)
}

func getJson(data interface{}) (string, error) {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}
