/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	"github.com/spiceai/spiceai/bin/spice/pkg/spec"
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
		datasetName = strings.TrimSpace(strings.TrimSuffix(datasetName, "\n"))
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
		from = strings.TrimSpace(strings.TrimSuffix(from, "\n"))

		params := map[string]string{}
		datasetPrefix := strings.Split(from, ":")[0]
		if datasetPrefix == spec.DATA_SOURCE_DREMIO || datasetPrefix == spec.DATA_SOURCE_DATABRICKS {
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

		dataset := spec.DatasetSpec{
			From:        from,
			Name:        datasetName,
			Description: description,
			Params:      params,
		}

		if accelerateDataset {
			dataset.Acceleration = &spec.AccelerationSpec{
				Enabled:         accelerateDataset,
				RefreshInterval: time.Second * 10,
				RefreshMode:     spec.REFRESH_MODE_FULL,
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

		var spicePod spec.SpicepodSpec
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
			spicePod.Datasets = append(spicePod.Datasets, &spec.Reference{
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
