package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
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

	datasetSourcePrompt:
		cmd.Println("\nWhere is your dataset located?")
		for i, option := range api.DATA_SOURCES {
			cmd.Printf("\t[%d] %s\n", i, api.DataSourceToHumanReadable(option))
		}
		datasetOptionString, err := reader.ReadString('\n')
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		datasetOptionString = strings.TrimSuffix(datasetOptionString, "\n")
		datasetOption, err := strconv.ParseInt(datasetOptionString, 10, 64)
		if err != nil || datasetOption < 0 || datasetOption >= int64(len(api.DATA_SOURCES)) {
			cmd.Println(aurora.BrightRed("Invalid input"))
			goto datasetSourcePrompt
		}
		datasetSource := api.DATA_SOURCES[datasetOption]

		cmd.Print("\nLocally accelerate this dataset (y/n)? ")
		accelerateDatasetString, err := reader.ReadString('\n')
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		accelerateDatasetString = strings.TrimSuffix(accelerateDatasetString, "\n")
		accelerateDataset := strings.ToLower(accelerateDatasetString) == "y"

		dataset := api.Dataset{
			Name:   datasetName,
			Type:   api.DATASET_TYPE_OVERWRITE,
			Source: datasetSource,
			Acceleration: &api.Acceleration{
				Enabled: accelerateDataset,
				Refresh: time.Hour,
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
			if dataset.From == dirPath {
				datasetReferenced = true
				break
			}
		}

		if !datasetReferenced {
			spicePod.Datasets = append(spicePod.Datasets, &api.Reference{
				From: dirPath,
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
