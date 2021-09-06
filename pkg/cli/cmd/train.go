package cmd

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/pkg/cli/runtime"
	"github.com/spiceai/spiceai/pkg/config"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/util"
)

var trainCmd = &cobra.Command{
	Use:   "train",
	Short: "Train Pod - start a pod training run",
	Example: `
spice train LogPruner
spice train logpruner.yaml
`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		podNameOrPath := args[0]

		podPath := podNameOrPath
		podName := podNameOrPath
		_, err := os.Stat(podNameOrPath)
		if err != nil {
			podPath = pods.FindFirstManifestPath()
		} else {
			err := runtime.Run(contextFlag, podPath)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
			return
		}

		if podPath == "" || podName == "" {
			fmt.Println("pod not found")
			return
		}

		pod, err := pods.LoadPodFromManifest(podPath)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		if pod.Name != podName {
			fmt.Printf("the pod '%s' does not exist\n", podNameOrPath)
			return
		}

		v := viper.New()
		appDir := context.CurrentContext().AppDir()
		runtimeConfig, err := config.LoadRuntimeConfiguration(v, appDir)
		if err != nil {
			fmt.Println("failed to load runtime configuration")
			return
		}

		serverBaseUrl := runtimeConfig.ServerBaseUrl()

		err = util.IsRuntimeServerHealthy(serverBaseUrl, http.DefaultClient)
		if err != nil {
			fmt.Printf("failed to reach %s. is the spice runtime running?\n", serverBaseUrl)
			return
		}

		trainUrl := fmt.Sprintf("%s/api/v0.1/pods/%s/train", serverBaseUrl, pod.Name)
		response, err := http.DefaultClient.Post(trainUrl, "application/json", nil)
		if err != nil {
			fmt.Printf("failed to start training: %s\n", err.Error())
			return
		}

		if response.StatusCode != 200 {
			if response.StatusCode == 404 {
				fmt.Printf("failed to start training.  the pod '%s' cannot be found.  has it been added?", podNameOrPath)
				return
			}

			body, err := ioutil.ReadAll(response.Body)
			defer response.Body.Close()

			if err != nil {
				fmt.Printf("failed to start training: %s\n", err.Error())
				return
			}

			if len(body) > 0 {
				fmt.Printf("failed to start training: %s\n", body)
			} else {
				fmt.Printf("failed to start training: %s\n", response.Status)
			}

			return
		}

		fmt.Println(aurora.Green("training started!"))
	},
}

func init() {
	trainCmd.Flags().StringVar(&contextFlag, "context", "docker", "Runs Spice.ai in the given context, either 'docker' or 'metal'")
	RootCmd.AddCommand(trainCmd)
}
