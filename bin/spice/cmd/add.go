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

package cmd

import (
	"errors"
	"fmt"
	"os"
	"path"

	"log/slog"

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
	Run: getAddOrConnectCmdHandler(false),
}

func init() {
	RootCmd.AddCommand(addCmd)
}

func getAddOrConnectCmdHandler(connect bool) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		ctx := context.NewContext()
		err := ctx.Init()
		if err != nil {
			slog.Error("could not initialize runtime context", "error", err)
			os.Exit(1)
		}

		podPath := args[0]

		slog.Info(fmt.Sprintf("Getting Spicepod %s ...\n", podPath))

		if connect {
			if ctx.GetApiKey() == "" {
				slog.Error("A valid Spice.ai Cloud Platform API key was not provided. Run `spice login` to authenticate before proceeding.")
				os.Exit(1)
			}

			headers := map[string]string{
				"Spice-Target-Source": "spice.ai",
				"X-API-Key":           ctx.GetApiKey(),
			}
			ctx.AddHeaders(headers)
		}

		r := registry.GetRegistry(podPath)
		downloadPath, err := r.GetPod(ctx, podPath)
		if err != nil {
			var itemNotFound *registry.RegistryItemNotFound
			if errors.As(err, &itemNotFound) {
				slog.Error(fmt.Sprintf("No Spicepod found at '%s'.\n", podPath))
			} else {
				slog.Error(err.Error())
			}
			return
		}

		relativePath := context.NewContext().GetSpiceAppRelativePath(downloadPath)

		spicepodBytes, err := os.ReadFile("spicepod.yaml")
		if err != nil {
			if os.IsNotExist(err) {
				wd, err := os.Getwd()
				if err != nil {
					slog.Error("getting current working directory", "error", err)
					os.Exit(1)
				}
				name := path.Base(wd)
				spicepodPath, err := spicepod.CreateManifest(name, ".")
				if err != nil {
					slog.Error("creating spicepod.yaml", "error", err)
					os.Exit(1)
				}
				slog.Info(fmt.Sprintf("%s", aurora.BrightGreen(fmt.Sprintf("%s initialized!", spicepodPath))))
				spicepodBytes, err = os.ReadFile("spicepod.yaml")
				if err != nil {
					slog.Error("reading spicepod.yaml", "error", err)
					os.Exit(1)
				}
			} else {
				slog.Error("reading spicepod.yaml", "error", err)
				os.Exit(1)
			}
		}

		var spicePod spec.SpicepodSpec
		err = yaml.Unmarshal(spicepodBytes, &spicePod)
		if err != nil {
			slog.Error("unmarshalling spicepod.yaml", "error", err)
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
				slog.Error("marshalling spicepod.yaml with dependencies", "error", err)
				os.Exit(1)
			}

			err = os.WriteFile("spicepod.yaml", spicepodBytes, 0766)
			if err != nil {
				slog.Error("writing spicepod.yaml with dependencies", "error", err)
				os.Exit(1)
			}
		}

		slog.Info(fmt.Sprintf("added %s\n", relativePath))

		err = checkLatestCliReleaseVersion()
		if err != nil && util.IsDebug() {
			slog.Error("failed to check for latest CLI release version", "error", err)
		}
	}
}
