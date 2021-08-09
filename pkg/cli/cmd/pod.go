package cmd

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/context"
	"github.com/spiceai/spice/pkg/pods"
	"github.com/spiceai/spice/pkg/registry"
	"github.com/spiceai/spice/pkg/spec"
	"github.com/spiceai/spice/pkg/util"
	"gopkg.in/yaml.v2"
)

var podCmd = &cobra.Command{
	Use:   "pod",
	Short: "Pod actions",
	Example: `
spice pod install samples/CartPole
`,
}

var podInitCmd = &cobra.Command{
	Use:   "init",
	Short: "Initializes a skeleton Spice Pod",
	Long:  "Initializes a skeleton Spice Pod",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		context.SetContext(context.BareMetal)
		podName := args[0]
		podManifestFileName := fmt.Sprintf("%s.yaml", strings.ToLower(podName))

		podsPath := config.PodsManifestsPath()
		podManifestPath := filepath.Join(podsPath, podManifestFileName)
		appRelativeManifestPath := config.GetSpiceAppRelativePath(podManifestPath)

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
	Example: `
spice pod init trader	
`,
}

var podAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Adds a Spice Pod into your app",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		context.SetContext(context.BareMetal)
		podPath := args[0]

		fmt.Printf("Getting Pod %s ...\n", podPath)

		r := registry.GetRegistry(podPath)
		downloadPath, err := r.GetPod(podPath)
		if err != nil {
			var itemNotFound *registry.RegistryItemNotFound
			if errors.As(err, &itemNotFound) {
				fmt.Printf("No pod found with the name '%s'.\n", podPath)
			} else {
				fmt.Println(err)
			}
			return
		}

		relativePath := config.GetSpiceAppRelativePath(downloadPath)

		fmt.Printf("Added %s\n", relativePath)
	},
	Example: `
spice pod add samples/CartPole-V1
`,
}

var podTrainCmd = &cobra.Command{
	Use:   "train",
	Short: "Starts training run for this pod",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		context.SetContext(context.BareMetal)
		podName := args[0]

		podPath := pods.FindFirstManifestPath()
		if podPath == "" {
			fmt.Println("No pods detected!")
			return
		}

		pod, err := pods.LoadPodFromManifest(podPath)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		if pod.Name != podName {
			fmt.Printf("the pod %s does not exist\n", podName)
			return
		}

		v := viper.New()
		runtimeConfig, err := config.LoadRuntimeConfiguration(v)
		if err != nil {
			fmt.Println("failed to load runtime configuration")
			return
		}

		serverBaseUrl := runtimeConfig.ServerBaseUrl()

		err = util.IsServerHealthy(serverBaseUrl, http.DefaultClient)
		if err != nil {
			fmt.Printf("failed to reach %s. is the spice runtime running?", serverBaseUrl)
			return
		}

		trainUrl := fmt.Sprintf("%s/api/v0.1/pods/%s/train", serverBaseUrl, pod.Name)
		response, err := http.DefaultClient.Post(trainUrl, "application/json", nil)
		if err != nil {
			fmt.Printf("failed to start training: %s", err.Error())
			return
		}

		if response.StatusCode != 200 {
			fmt.Printf("failed to start training: %s", response.Status)
			return
		}

		fmt.Println(aurora.Green("training started!"))
	},
	Example: `
spice pod train CartPole-V1
`,
}

func init() {
	podCmd.AddCommand(podInitCmd)
	podCmd.AddCommand(podAddCmd)
	podCmd.AddCommand(podTrainCmd)
	podCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(podCmd)
}
