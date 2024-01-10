package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
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
		podManifestFileName := fmt.Sprintf("%s.yaml", strings.ToLower(podName))

		rtcontext := context.NewContext()
		podsPath := rtcontext.PodsDir()
		podManifestPath := filepath.Join(podsPath, podManifestFileName)
		appRelativeManifestPath := rtcontext.GetSpiceAppRelativePath(podManifestPath)

		if _, err := os.Stat(podManifestPath); !os.IsNotExist(err) {
			cmd.Printf("Pod manifest already exists at %s. Replace (y/n)? \n", appRelativeManifestPath)
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

		err = os.MkdirAll(podsPath, 0766)
		if err != nil {
			cmd.Println(err)
			return
		}

		err = os.WriteFile(podManifestPath, skeletonPodContentBytes, 0766)
		if err != nil {
			cmd.Println(err)
			return
		}

		cmd.Printf("Spice pod manifest initialized at %s!\n", appRelativeManifestPath)
	},
}

func init() {
	initCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(initCmd)
}
