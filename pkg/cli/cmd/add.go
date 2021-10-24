package cmd

import (
	"errors"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/registry"
)

var addCmd = &cobra.Command{
	Use:   "add",
	Short: "Add Pod - adds a pod to the project",
	Args:  cobra.MinimumNArgs(1),
	Example: `
spice add samples/LogPruner
`,
	Run: func(cmd *cobra.Command, args []string) {
		podPath := args[0]

		cmd.Printf("Getting Pod %s ...\n", podPath)

		r := registry.GetRegistry(podPath)
		downloadPath, err := r.GetPod(podPath)
		if err != nil {
			var itemNotFound *registry.RegistryItemNotFound
			if errors.As(err, &itemNotFound) {
				cmd.Printf("No pod found with the name '%s'.\n", podPath)
			} else {
				cmd.Println(err)
			}
			return
		}

		relativePath := context.CurrentContext().GetSpiceAppRelativePath(downloadPath)

		cmd.Printf("Added %s\n", relativePath)
	},
}

func init() {
	addCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(addCmd)
}
