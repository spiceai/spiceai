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
