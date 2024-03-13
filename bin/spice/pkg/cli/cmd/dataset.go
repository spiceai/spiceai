package cmd

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"gopkg.in/yaml.v2"
)

var datasetCmd = &cobra.Command{
	Use:   "dataset",
	Short: "Dataset operations",
}

var configureCmd = &cobra.Command{
	Use:   "configure",
	Short: "Configure a dataset",
	Example: `
spice dataset configure

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {
		if fi, err := os.Stat("spicepod.yaml"); os.IsNotExist(err) || fi.IsDir() {
			cmd.Println(aurora.BrightRed("No spicepod.yaml found. Run spice init <app> first."))
			os.Exit(1)
		}

		reader := bufio.NewReader(os.Stdin)

		cwd, err := os.Getwd()
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		defaultDatasetName := path.Base(cwd)
		cmd.Printf("dataset name: (%s) ", defaultDatasetName)
		datasetName, err := reader.ReadString('\n')
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		datasetName = strings.TrimSuffix(datasetName, "\n")
		if datasetName == "" {
			datasetName = defaultDatasetName
		}

		cmd.Print("description: ")
		description, err := reader.ReadString('\n')
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		description = strings.TrimSuffix(description, "\n")

		cmd.Print("from: ")
		from, err := reader.ReadString('\n')
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		from = strings.TrimSuffix(from, "\n")

		params := map[string]string{}
		datasetPrefix := strings.Split(from, ":")[0]
		if datasetPrefix == api.DATA_SOURCE_DREMIO || datasetPrefix == api.DATA_SOURCE_DATABRICKS {
			cmd.Print("endpoint: ")
			endpoint, err := reader.ReadString('\n')
			if err != nil {
				cmd.Println(err.Error())
				os.Exit(1)
			}
			endpoint = strings.TrimSuffix(endpoint, "\n")

			params["endpoint"] = endpoint
		}

		cmd.Print("locally accelerate (y/n)? (y) ")
		locallyAccelerateStr, err := reader.ReadString('\n')
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		locallyAccelerateStr = strings.TrimSuffix(locallyAccelerateStr, "\n")
		accelerateDataset := locallyAccelerateStr == "" || strings.ToLower(locallyAccelerateStr) == "y"

		dataset := api.Dataset{
			From:        from,
			Name:        datasetName,
			Description: description,
			Params:      params,
		}

		if accelerateDataset {
			dataset.Acceleration = &api.Acceleration{
				Enabled:         accelerateDataset,
				RefreshInterval: time.Second * 10,
				RefreshMode:     api.REFRESH_MODE_FULL,
			}
		}

		datasetBytes, err := yaml.Marshal(dataset)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		dirPath := fmt.Sprintf("datasets/%s", dataset.Name)
		err = os.MkdirAll(dirPath, 0766)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		filePath := fmt.Sprintf("%s/dataset.yaml", dirPath)
		err = os.WriteFile(filePath, datasetBytes, 0766)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		spicepodBytes, err := os.ReadFile("spicepod.yaml")
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		var spicePod api.Pod
		err = yaml.Unmarshal(spicepodBytes, &spicePod)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		var datasetReferenced bool
		for _, dataset := range spicePod.Datasets {
			if dataset.Ref == dirPath {
				datasetReferenced = true
				break
			}
		}

		if !datasetReferenced {
			spicePod.Datasets = append(spicePod.Datasets, &api.Reference{
				Ref: dirPath,
			})
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

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Saved %s", filePath)))
	},
}

func init() {
	configureCmd.Flags().BoolP("help", "h", false, "Print this help message")
	datasetCmd.AddCommand(configureCmd)

	datasetCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(datasetCmd)
}
