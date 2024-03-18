package cmd

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"gopkg.in/yaml.v2"
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

		if len(args) < 1 {
			wd, err := os.Getwd()
			if err != nil {
				cmd.PrintErrf("Error getting current working directory: %s\n", err.Error())
				return
			}
			dirName := path.Base(wd)

			cmd.Printf("name: (%s)? ", dirName)
			fmt.Scanf("%s", &spicepodName)
			if strings.TrimSpace(spicepodName) == "" {
				spicepodName = dirName
			}
		} else {
			spicepodName = args[0]
			fs, err := os.Stat(spicepodName)
			if err != nil {
				if os.IsNotExist(err) {
					err = os.Mkdir(spicepodName, 0766)
					if err != nil {
						cmd.PrintErrf("Error creating directory: %s\n", err.Error())
						return
					}
				} else {
					cmd.PrintErrf("Error checking if directory exists: %s\n", err.Error())
					return
				}
			} else {
				if !fs.IsDir() {
					cmd.PrintErrf("Error: %s exists and is not a directory\n", spicepodName)
					return
				}
			}
			spicepodDir = spicepodName
		}

		spicepodPath := path.Join(spicepodDir, "spicepod.yaml")
		if _, err := os.Stat(spicepodPath); !os.IsNotExist(err) {
			cmd.Println("spicepod.yaml already exists. Replace (y/n)?")
			var confirm string
			fmt.Scanf("%s", &confirm)
			if strings.ToLower(strings.TrimSpace(confirm)) != "y" {
				return
			}
		}

		skeletonPod := &api.Spicepod{
			Name:    spicepodName,
			Version: "v1beta1",
			Kind:    "Spicepod",
		}

		skeletonPodContentBytes, err := yaml.Marshal(skeletonPod)
		if err != nil {
			cmd.Println(err)
			return
		}

		err = os.WriteFile(spicepodPath, skeletonPodContentBytes, 0766)
		if err != nil {
			cmd.Println(err)
			return
		}

		cmd.Println(aurora.BrightGreen("spicepod.yaml initialized!"))
	},
}

func init() {
	initCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(initCmd)
}
