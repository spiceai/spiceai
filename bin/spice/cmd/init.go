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
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/spicepod"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize Spice app - initializes a new Spice app",
	Example: `
spice init
spice init <spice app name>
spice init my_app
`,
	Run: func(cmd *cobra.Command, args []string) {
		var spicepodName string
		spicepodDir := "."

		if len(args) < 1 || args[0] == "." {
			wd, err := os.Getwd()
			if err != nil {
				cmd.PrintErrf("Error getting current working directory: %s\n", err.Error())
				return
			}
			dirName := path.Base(wd)

			cmd.Printf("name: (%s)? ", dirName)
			_, _ = fmt.Scanf("%s", &spicepodName)
			if strings.TrimSpace(spicepodName) == "" {
				spicepodName = dirName
			}
		} else {
			spicepodName = args[0]
			spicepodDir = path.Join(spicepodDir, spicepodName)
		}

		spicepodPath := path.Join(spicepodDir, "spicepod.yaml")
		if _, err := os.Stat(spicepodPath); !os.IsNotExist(err) {
			cmd.Print("spicepod.yaml already exists. Replace (y/n)? ")
			var confirm string
			_, _ = fmt.Scanf("%s", &confirm)
			if strings.ToLower(strings.TrimSpace(confirm)) != "y" {
				return
			}
		}

		spicepodPath, err := spicepod.CreateManifest(spicepodName, spicepodDir)
		if err != nil {
			cmd.PrintErrf("Error creating spicepod.yaml: %s\n", err.Error())
			return
		}

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("%s initialized!", spicepodPath)))
	},
}

func init() {
	initCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(initCmd)
}
