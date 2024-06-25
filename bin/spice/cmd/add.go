/*
Copyright 2024 The Spice.ai OSS Authors

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

package cmd

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/registry"
	"github.com/spiceai/spiceai/bin/spice/pkg/spec"
	"github.com/spiceai/spiceai/bin/spice/pkg/spicepod"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
	"gopkg.in/yaml.v3"
)

var addCmd = &cobra.Command{
	Use:   "add",
	Short: "Add Spicepod - adds a Spicepod to the project",
	Args:  cobra.MinimumNArgs(1),
	Example: `
spice add spiceai/quickstart
`,
	Run: func(cmd *cobra.Command, args []string) {
		podPath := args[0]

		cmd.Printf("Getting Spicepod %s ...\n", podPath)

		r := registry.GetRegistry(podPath)
		downloadPath, err := r.GetPod(podPath)
		if err != nil {
			var itemNotFound *registry.RegistryItemNotFound
			if errors.As(err, &itemNotFound) {
				cmd.Printf("No Spicepod found at '%s'.\n", podPath)
			} else {
				cmd.Println(err)
			}
			return
		}

		relativePath := context.NewContext().GetSpiceAppRelativePath(downloadPath)

		spicepodBytes, err := os.ReadFile("spicepod.yaml")
		if err != nil {
			if os.IsNotExist(err) {
				wd, err := os.Getwd()
				if err != nil {
					cmd.PrintErrf("Error getting current working directory: %s\n", err.Error())
					os.Exit(1)
				}
				name := path.Base(wd)
				spicepodPath, err := spicepod.CreateManifest(name, ".")
				if err != nil {
					cmd.PrintErrf("Error creating spicepod.yaml: %s\n", err.Error())
					os.Exit(1)
				}
				cmd.Println(aurora.BrightGreen(fmt.Sprintf("%s initialized!", spicepodPath)))
				spicepodBytes, err = os.ReadFile("spicepod.yaml")
				if err != nil {
					cmd.PrintErrf("Error reading spicepod.yaml: %s\n", err.Error())
					os.Exit(1)
				}
			} else {
				cmd.Println(err)
				os.Exit(1)
			}
		}

		var spicePod spec.SpicepodSpec
		err = yaml.Unmarshal(spicepodBytes, &spicePod)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		var podReferenced bool
		for _, dependency := range spicePod.Dependencies {
			if dependency == podPath {
				podReferenced = true
				break
			}
		}

		if !podReferenced {
			spicePod.Dependencies = append(spicePod.Dependencies, podPath)
			spicepodBytes, err = yaml.Marshal(spicePod)
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			err = os.WriteFile("spicepod.yaml", spicepodBytes, 0766)
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}
		}

		cmd.Printf("Added %s\n", relativePath)

		err = checkLatestCliReleaseVersion()
		if err != nil && util.IsDebug() {
			cmd.PrintErrf("failed to check for latest CLI release version: %s\n", err.Error())
		}
	},
}

func init() {
	addCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(addCmd)
}
