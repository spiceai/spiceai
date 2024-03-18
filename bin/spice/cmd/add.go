package cmd

import (
	"errors"
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/registry"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
	"gopkg.in/yaml.v2"
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
			cmd.Println(err)
			os.Exit(1)
		}

		var spicePod api.Spicepod
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
