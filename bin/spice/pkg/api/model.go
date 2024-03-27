/*
Copyright 2021-2024 The Spice Authors

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

type Model struct {
	Name     string   `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	From     string   `json:"from,omitempty" csv:"from" yaml:"from,omitempty"`
	Datasets []string `json:"datasets,omitempty" csv:"datasets" yaml:"datasets,omitempty"`
	Status   string   `json:"status,omitempty" csv:"status,omitempty" yaml:"status,omitempty"`
}
