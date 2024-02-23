package cmd

import (
	"bufio"
	"fmt"
	"os"
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
		reader := bufio.NewReader(os.Stdin)

		cmd.Print("What is the dataset name? ")
		datasetName, err := reader.ReadString('\n')
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		datasetName = strings.TrimSuffix(datasetName, "\n")

		cmd.Print("\nWhere is your dataset located? ")
		datasetLocation, err := reader.ReadString('\n')
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		datasetLocation = strings.TrimSuffix(datasetLocation, "\n")

		params := map[string]string{}
		if strings.Split(datasetLocation, ":")[0] == api.DATA_SOURCE_DREMIO {

			cmd.Print("\nWhat is your dremio endpoint? ")
			endpoint, err := reader.ReadString('\n')
			if err != nil {
				cmd.Println(err.Error())
				os.Exit(1)
			}
			endpoint = strings.TrimSuffix(endpoint, "\n")

			params["endpoint"] = endpoint
		}

		cmd.Print("\nLocally accelerate this dataset (y/n)? ")
		accelerateDatasetString, err := reader.ReadString('\n')
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		accelerateDatasetString = strings.TrimSuffix(accelerateDatasetString, "\n")
		accelerateDataset := strings.ToLower(accelerateDatasetString) == "y"

		dataset := api.Dataset{
			From:   datasetLocation,
			Name:   datasetName,
			Params: params,
			Acceleration: &api.Acceleration{
				Enabled:         accelerateDataset,
				RefreshInterval: time.Second * 10,
				RefreshMode:     api.REFRESH_MODE_FULL,
			},
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

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Dataset settings written to `%s`!", filePath)))
	},
}

func init() {
	configureCmd.Flags().BoolP("help", "h", false, "Print this help message")
	datasetCmd.AddCommand(configureCmd)

	datasetCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(datasetCmd)
}
