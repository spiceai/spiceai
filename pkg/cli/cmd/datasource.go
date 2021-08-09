package cmd

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spiceai/spice/pkg/context"
	"github.com/spiceai/spice/pkg/pods"
	"github.com/spiceai/spice/pkg/registry"
	"github.com/spiceai/spice/pkg/util"
	"gopkg.in/yaml.v2"
)

var datasourceCmd = &cobra.Command{
	Use:   "datasource",
	Short: "Datasource actions",
	Example: `
spice datasource add coinbase/btcusd
`,
}

var datasourceAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a datasource to your Spice pod",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		context.SetContext(context.BareMetal)
		dataSourcePath := args[0]

		podPath := pods.FindFirstManifestPath()
		if podPath == "" {
			fmt.Println("No pods detected!")
			return
		}

		pod, err := pods.LoadPodFromManifest(podPath)
		if err != nil {
			fmt.Println(err.Error())
		}

		fmt.Printf("Getting datasource %s ...\n", dataSourcePath)

		r := registry.GetRegistry(dataSourcePath)
		dataSource, err := r.GetDataSource(dataSourcePath)
		if err != nil {
			var itemNotFound *registry.RegistryItemNotFound
			if errors.As(err, &itemNotFound) {
				fmt.Printf("No datasource found with the name '%s'.\n", dataSourcePath)
			} else {
				fmt.Println(err)
			}
			return
		}

		content, err := yaml.Marshal(dataSource)
		if err != nil {
			fmt.Println(err)
			return
		}

		// TODO: Indentation should really be done by utils.AddElementToString
		element := strings.Builder{}
		element.WriteString("  - datasource:\n")
		for _, l := range strings.Split(string(content), "\n") {
			element.WriteString("    ")
			element.WriteString(l)
			element.WriteString("\n")
		}

		podContent, err := ioutil.ReadFile(podPath)
		if err != nil {
			log.Fatal(err.Error())
		}

		modifiedString, addedToContent := util.AddElementToString(string(podContent), strings.TrimRight(element.String(), "\n"), "datasources:", false)

		if !addedToContent {
			fmt.Printf("Data source '%s' already present in pod %s.\n", dataSourcePath, pod.Name)
			return
		}

		err = util.WriteToExistingFile(podPath, []byte(modifiedString))
		if err != nil {
			log.Fatal(err.Error())
		}

		fmt.Printf("Data source '%s' added to pod %s.\n", dataSourcePath, pod.Name)
	},
	Example: `
spice datasource add coinbase/btcusd	
`,
}

func init() {
	datasourceCmd.AddCommand(datasourceAddCmd)
	datasourceCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(datasourceCmd)
}
