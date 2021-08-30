package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spiceai/spice/pkg/context"
	"github.com/spiceai/spice/pkg/spec"
	"github.com/spiceai/spice/pkg/util"
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

		rtcontext := context.CurrentContext()
		podsPath := rtcontext.PodsDir()
		podManifestPath := filepath.Join(podsPath, podManifestFileName)
		appRelativeManifestPath := rtcontext.GetSpiceAppRelativePath(podManifestPath)

		if _, err := os.Stat(podManifestPath); !os.IsNotExist(err) {
			fmt.Printf("Pod manifest already exists at %s. Replace (y/n)? \n", appRelativeManifestPath)
			var confirm string
			fmt.Scanf("%s", &confirm)
			if strings.ToLower(strings.TrimSpace(confirm)) != "y" {
				return
			}
		}

		var rewardContent interface{} = "uniform"

		skeletonPod := &spec.PodSpec{
			Name:        podName,
			DataSources: make([]spec.DataSourceSpec, 1),
			Actions:     make([]spec.PodActionSpec, 1),
			Training: &spec.TrainingSpec{
				Rewards: rewardContent,
			},
		}

		skeletonPodContentBytes, err := yaml.Marshal(skeletonPod)
		if err != nil {
			fmt.Println(err)
			return
		}

		// HACKHACK: In place of properly marshalling comments
		skeletonPodContent := string(skeletonPodContentBytes)

		dataSourcesComment := "# Add a list of datasources here or run 'spice datasource add <datasource_publisher/datasource_id>'\n"
		skeletonPodContent, _ = util.AddElementToString(skeletonPodContent, dataSourcesComment, "datasources:", true)

		actionsComment := "# Add a list of actions here or run 'spice action add <action_id>'\n"
		skeletonPodContent, _ = util.AddElementToString(skeletonPodContent, actionsComment, "actions:", true)

		rewardsComment := "  # For custom rewards, replace 'uniform' with a list of rewards here or run 'spice reward set <action_id> <expression>'\n"
		skeletonPodContent, _ = util.AddElementToString(skeletonPodContent, rewardsComment, "  rewards: uniform", false)

		err = os.MkdirAll(podsPath, 0766)
		if err != nil {
			fmt.Println(err)
			return
		}

		err = os.WriteFile(podManifestPath, []byte(skeletonPodContent), 0766)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("Spice pod manifest initialized at %s!\n", appRelativeManifestPath)
	},
}

func init() {
	initCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(initCmd)
}
