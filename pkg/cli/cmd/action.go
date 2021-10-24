package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/spec"
	"github.com/spiceai/spiceai/pkg/util"
	"gopkg.in/yaml.v2"
)

var actionCmd = &cobra.Command{
	Use:     "action",
	Aliases: []string{"actions"},
	Short:   "Maintains actions",
	Example: `
spice action add jump
`,
}

var actionAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add Action - adds an action to the pod",
	Example: `
spice action add <>
spice action add jump
`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		cmdActionName := args[0]

		manifests := pods.FindAllManifestPaths()
		if len(manifests) == 0 {
			cmd.Println("No pods detected!")
			return
		}

		podPath := manifests[0]

		pod, err := pods.LoadPodFromManifest(podPath)
		if err != nil {
			cmd.Println(err.Error())
			return
		}

		actions := pod.Actions()

		if _, ok := actions[cmdActionName]; ok {
			cmd.Printf("Action %s already exists in %s. Overwrite? (y/n)\n", cmdActionName, pod.Name)
			var confirm string
			fmt.Scanf("%s", &confirm)
			if strings.ToLower(strings.TrimSpace(confirm)) != "y" {
				return
			}
		}

		pod.PodSpec.Actions = append(pod.PodSpec.Actions, spec.PodActionSpec{Name: cmdActionName})

		marshalledPod, err := yaml.Marshal(pod.PodSpec)
		if err != nil {
			cmd.Println(err.Error())
			return
		}

		err = util.WriteToExistingFile(podPath, marshalledPod)
		if err != nil {
			cmd.Println(err.Error())
			return
		}

		cmd.Printf("Action '%s' added to pod %s.\n", cmdActionName, pod.Name)
	},
}

func init() {
	actionCmd.AddCommand(actionAddCmd)
	actionCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(actionCmd)
}
