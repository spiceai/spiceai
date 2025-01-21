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

package constants

import "fmt"

type Flavor int

const (
	// The default flavor will install the AI-enabled runtime on new installs.
	// For upgrades, it will upgrade whatever flavor is currently installed.
	FlavorDefault Flavor = iota
	// FlavorCore will install the core runtime that only includes data components.
	FlavorCore
	// FlavorAI will install the AI-enabled runtime. This is a superset of the core runtime.
	FlavorAI
)

func ParseFlavor(s string) (Flavor, error) {
	switch s {
	case "":
		return FlavorDefault, nil
	case "ai":
		return FlavorAI, nil
	}

	// The core flavor can't be specified by the user - its only used internally when handling upgrades.
	return FlavorDefault, fmt.Errorf("unknown flavor: %s, valid flavors are: \"\", ai", s)
}

func (flavor Flavor) IsValid() bool {
	return flavor == FlavorDefault || flavor == FlavorAI || flavor == FlavorCore
}
