package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/spec"
	"gopkg.in/yaml.v2"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize Pod - initializes a new pod in the project",
	Example: `
spice init <pod name>
spice init trader
`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		podName := args[0]
		podPath := "./spicepod.yaml"
		if _, err := os.Stat(podPath); !os.IsNotExist(err) {
			cmd.Println("Pod manifest already exists. Replace (y/n)?")
			var confirm string
			fmt.Scanf("%s", &confirm)
			if strings.ToLower(strings.TrimSpace(confirm)) != "y" {
				return
			}
		}

		skeletonPod := &spec.PodSpec{
			Name: podName,
		}

		skeletonPodContentBytes, err := yaml.Marshal(skeletonPod)
		if err != nil {
			cmd.Println(err)
			return
		}

		err = os.WriteFile(podPath, skeletonPodContentBytes, 0766)
		if err != nil {
			cmd.Println(err)
			return
		}

		cmd.Printf("Spice pod manifest initialized!\n")
	},
}

func init() {
	initCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(initCmd)
}
