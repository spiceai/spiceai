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

type Worker struct {
	Name        string `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Description string `json:"description,omitempty" csv:"description" yaml:"description,omitempty"`
	Type        string `json:"type,omitempty" csv:"type" yaml:"type,omitempty"`
	IsLlm       bool   `json:"is_llm,omitempty" csv:"is_llm" yaml:"is_llm,omitempty"`
}

type WorkerResponse struct {
	Object string   `json:"object,omitempty" csv:"object" yaml:"object,omitempty"`
	Data   []Worker `json:"data,omitempty" csv:"data" yaml:"data,omitempty"`
}
