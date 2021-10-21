package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/pkg/api"
	"github.com/spiceai/spiceai/pkg/config"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/util"
)

var podsCmd = &cobra.Command{
	Use:     "pods",
	Aliases: []string{"pods"},
	Short:   "Retrieve pods",
	Example: `
spice pods list
`,
}

var podsListCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists currently loaded pods from the runtime",
	Example: `
spice pods list
`,
	Run: func(cmd *cobra.Command, args []string) {
		v := viper.New()
		appDir := context.CurrentContext().AppDir()
		runtimeConfig, err := config.LoadRuntimeConfiguration(v, appDir)
		if err != nil {
			cmd.Println("failed to load runtime configuration")
			return
		}

		serverBaseUrl := runtimeConfig.ServerBaseUrl()

		err = util.IsRuntimeServerHealthy(serverBaseUrl, http.DefaultClient)
		if err != nil {
			cmd.Printf("failed to reach %s. is the spice runtime running?\n", serverBaseUrl)
			return
		}

		listUrl := fmt.Sprintf("%s/api/v0.1/pods", serverBaseUrl)

		response, err := http.DefaultClient.Get(listUrl)
		if err != nil {
			cmd.Printf("failed to get currently loaded pods from runtime: %s\n", err.Error())
			return
		}

		if response.StatusCode != 200 {
			cmd.Printf("failed to get currently loaded pods from runtime: %s\n", response.Status)
			return
		}

		body, _ := ioutil.ReadAll(response.Body)
		if err != nil {
			cmd.Printf("failed to get currently loaded pods from runtime: %s\n", err.Error())
			return
		}

		pods := make([]*api.Pod, 0)
		err = json.Unmarshal(body, &pods)

		if err != nil {
			cmd.Printf("failed to get currently loaded pods from runtime: %s\n", err.Error())
			return
		}

		defer response.Body.Close()

		sort.SliceStable(pods, func(i, j int) bool {
			return strings.Compare(pods[i].Name, pods[j].Name) == -1
		})
		err = util.MarshalAndPrintTable(cmd.OutOrStdout(), pods)

		if err != nil {
			cmd.Printf("failed to get currently loaded pods from runtime: %s\n", err.Error())
			return
		}
	},
}

func init() {
	podsCmd.AddCommand(podsListCmd)
	podsCmd.Flags().BoolP("help", "h", false, "Prints this help message")
	podsListCmd.Flags().BoolP("help", "h", false, "Prints this help message")
	RootCmd.AddCommand(podsCmd)
}
