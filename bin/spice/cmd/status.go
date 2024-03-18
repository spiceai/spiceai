package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Spice runtime status",
	Example: `
spice status
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()

		statusUrl := fmt.Sprintf("%s/v1/status", rtcontext.HttpEndpoint())
		resp, err := http.Get(statusUrl)
		if err != nil {
			if strings.HasSuffix(err.Error(), "connection refused") {
				cmd.PrintErrln(rtcontext.RuntimeUnavailableError().Error())
				os.Exit(1)
			}
			cmd.PrintErrf("Error getting status: %s\n", err.Error())
			os.Exit(1)
		}
		defer resp.Body.Close()

		var status []api.Service
		err = json.NewDecoder(resp.Body).Decode(&status)
		if err != nil {
			cmd.PrintErrf("Error decoding status: %s\n", err.Error())
			os.Exit(1)
		}

		var statusTable []interface{}
		for _, s := range status {
			statusTable = append(statusTable, s)
		}

		util.WriteTable(statusTable)
	},
}

func init() {
	RootCmd.AddCommand(statusCmd)
}
