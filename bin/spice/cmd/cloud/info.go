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

package cloud

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

// RegionsCmd lists available deployment regions
var RegionsCmd = &cobra.Command{
	Use:     "regions",
	Aliases: []string{"region", "list-regions"},
	Short:   "List available deployment regions",
	Example: `
spice cloud regions
spice cloud regions --env dev
`,
	Run: func(cmd *cobra.Command, args []string) {
		env, _ := cmd.Flags().GetString("env")

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		result, err := client.ListRegions(env)
		if err != nil {
			slog.Error("Failed to list regions", "error", err)
			os.Exit(1)
		}

		if len(result.Regions) == 0 {
			cmd.Println("No regions available")
			return
		}

		var table []interface{}
		for _, r := range result.Regions {
			isDefault := ""
			if r.IsDefault {
				isDefault = "*"
			}
			status := "available"
			if r.Disabled {
				status = "disabled"
			}
			table = append(table, regionTableRow{
				Region:   r.Region,
				Name:     r.Name,
				Provider: r.ProviderName,
				Status:   status,
				Default:  isDefault,
			})
		}
		util.WriteTable(table)

		if result.Default != "" {
			cmd.Printf("\n* Default region: %s\n", result.Default)
		}
	},
}

type regionTableRow struct {
	Region   string `csv:"REGION"`
	Name     string `csv:"NAME"`
	Provider string `csv:"PROVIDER"`
	Status   string `csv:"STATUS"`
	Default  string `csv:"DEFAULT"`
}

// ImagesCmd lists available container images
var ImagesCmd = &cobra.Command{
	Use:     "images",
	Aliases: []string{"image", "list-images", "container-images"},
	Short:   "List available Spice.ai container images",
	Example: `
spice cloud images
spice cloud images --channel enterprise
`,
	Run: func(cmd *cobra.Command, args []string) {
		channel, _ := cmd.Flags().GetString("channel")

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		result, err := client.ListContainerImages(channel)
		if err != nil {
			slog.Error("Failed to list container images", "error", err)
			os.Exit(1)
		}

		if len(result.Images) == 0 {
			cmd.Println("No container images available")
			return
		}

		var table []interface{}
		for _, img := range result.Images {
			isDefault := ""
			if img.Tag == result.Default {
				isDefault = "*"
			}
			table = append(table, imageTableRow{
				Tag:     img.Tag,
				Name:    img.Name,
				Channel: img.Channel,
				Default: isDefault,
			})
		}
		util.WriteTable(table)

		if result.Default != "" {
			cmd.Printf("\n* Default image: %s\n", result.Default)
		}
	},
}

type imageTableRow struct {
	Tag     string `csv:"TAG"`
	Name    string `csv:"NAME"`
	Channel string `csv:"CHANNEL"`
	Default string `csv:"DEFAULT"`
}

func init() {
	// Regions flags
	RegionsCmd.Flags().String("env", "", "Filter by environment (prod or dev)")

	// Images flags
	ImagesCmd.Flags().String("channel", "stable", "Release channel (stable or enterprise)")
}
