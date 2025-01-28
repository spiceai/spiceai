/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
	"log/slog"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/spec"
	"gopkg.in/yaml.v3"
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

# See more at: https://spiceai.org/docs/
`,
	Run: func(cmd *cobra.Command, args []string) {
		if fi, err := os.Stat("spicepod.yaml"); os.IsNotExist(err) || fi.IsDir() {
			slog.Error(fmt.Sprintf("%s", aurora.BrightRed("No spicepod.yaml found. Run spice init <app> first.")))
			os.Exit(1)
		}

		reader := bufio.NewReader(os.Stdin)

		cwd, err := os.Getwd()
		if err != nil {
			slog.Error("getting current working directory", "error", err)
			os.Exit(1)
		}

		defaultDatasetName := path.Base(cwd)
		cmd.Printf("dataset name: (%s) ", defaultDatasetName)
		datasetName, err := reader.ReadString('\n')
		if err != nil {
			slog.Error("Error reading dataset name", "error", err)
			os.Exit(1)
		}
		datasetName = strings.TrimSpace(strings.TrimSuffix(datasetName, "\n"))
		if datasetName == "" {
			datasetName = defaultDatasetName
		}

		match, err := regexp.MatchString("^[a-zA-Z0-9_-]+$", datasetName)
		if err != nil {
			slog.Error("Error validating dataset name", "error", err)
			os.Exit(1)
		}

		if !match {
			slog.Error(fmt.Sprintf("%v", aurora.BrightRed("Dataset name can only contain letters, numbers, underscores, and hyphens")))
			os.Exit(1)
		}

		if strings.Contains(datasetName, "-") {
			// warn that dataset name with hyphen should be quoted in queries
			slog.Warn(fmt.Sprintf("%v", aurora.BrightYellow(fmt.Sprintf("Dataset names with hyphens should be quoted in queries:\ni.e. SELECT * FROM \"%s\"", datasetName))))
		}

		cmd.Print("description: ")
		description, err := reader.ReadString('\n')
		if err != nil {
			slog.Error("Error reading stdin", "error", err)
			os.Exit(1)
		}
		description = strings.TrimSuffix(description, "\n")

		cmd.Print("from: ")
		from, err := reader.ReadString('\n')
		if err != nil {
			slog.Error("Error reading stdin", "error", err)
			os.Exit(1)
		}
		from = strings.TrimSpace(strings.TrimSuffix(from, "\n"))

		params := map[string]string{}
		datasetPrefix := strings.Split(from, ":")[0]
		if datasetPrefix == spec.DATA_SOURCE_DREMIO || datasetPrefix == spec.DATA_SOURCE_DATABRICKS {
			cmd.Print("endpoint: ")
			endpoint, err := reader.ReadString('\n')
			if err != nil {
				slog.Error("Error reading stdin", "error", err)
				os.Exit(1)
			}
			endpoint = strings.TrimSuffix(endpoint, "\n")

			params[datasetPrefix+"_endpoint"] = endpoint
		}

		if datasetPrefix == spec.DATA_SOURCE_S3 || datasetPrefix == spec.DATA_SOURCE_FTP || datasetPrefix == spec.DATA_SOURCE_SFTP {
			// check if `from` ends with .csv or .parquet
			from_path := strings.ToLower(from)
			if !strings.HasSuffix(from_path, ".csv") && !strings.HasSuffix(from_path, ".parquet") {
				cmd.Print("file_format (parquet/csv) (parquet) ")
				file_format, err := reader.ReadString('\n')
				if err != nil {
					slog.Error("Error reading stdin", "error", err)
					os.Exit(1)
				}
				file_format = strings.TrimSuffix(file_format, "\n")

				if file_format == "" {
					file_format = "parquet"
				}

				if file_format != "parquet" && file_format != "csv" {
					slog.Error(fmt.Sprintf("%v\n", aurora.BrightRed("file_format must be either parquet or csv")))
					os.Exit(1)
				}

				params["file_format"] = file_format
			}
		}

		cmd.Print("locally accelerate (y/n)? (y) ")
		locallyAccelerateStr, err := reader.ReadString('\n')
		if err != nil {
			slog.Error("Error reading stdin", "error", err)
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
				Enabled:              accelerateDataset,
				RefreshCheckInterval: time.Second * 10,
				RefreshMode:          spec.REFRESH_MODE_FULL,
			}
		}

		datasetBytes, err := yaml.Marshal(dataset)
		if err != nil {
			slog.Error("marshalling dataset", "error", err)
			os.Exit(1)
		}

		dirPath := fmt.Sprintf("datasets/%s", dataset.Name)
		err = os.MkdirAll(dirPath, 0766)
		if err != nil {
			slog.Error("creating dataset directory", "error", err)
			os.Exit(1)
		}

		filePath := fmt.Sprintf("%s/dataset.yaml", dirPath)
		err = os.WriteFile(filePath, datasetBytes, 0766)
		if err != nil {
			slog.Error(fmt.Sprintf("writing dataset file to %s", filePath), "error", err)
			os.Exit(1)
		}

		spicepodBytes, err := os.ReadFile("spicepod.yaml")
		if err != nil {
			slog.Error("reading spicepod.yaml", "error", err)
			os.Exit(1)
		}

		var spicePod spec.SpicepodSpec
		err = yaml.Unmarshal(spicepodBytes, &spicePod)
		if err != nil {
			slog.Error("parsing spicepod.yaml", "error", err)
			os.Exit(1)
		}

		var datasetReferenced bool
		for _, dataset := range spicePod.Datasets {
			if dataset["ref"] == dirPath {
				datasetReferenced = true
				break
			}
		}

		if !datasetReferenced {
			spicePod.Datasets = append(spicePod.Datasets, map[string]interface{}{
				"ref": dirPath,
			})
			spicepodBytes, err = yaml.Marshal(spicePod)
			if err != nil {
				slog.Error("marshalling spicepod.yaml", "error", err)
				os.Exit(1)
			}

			err = os.WriteFile("spicepod.yaml", spicepodBytes, 0766)
			if err != nil {
				slog.Error("writing spicepod.yaml", "error", err)
				os.Exit(1)
			}
		}

		slog.Info(fmt.Sprintf("%s", aurora.BrightGreen(fmt.Sprintf("Saved %s", filePath))))
	},
}

func init() {
	datasetCmd.AddCommand(configureCmd)

	RootCmd.AddCommand(datasetCmd)
}
