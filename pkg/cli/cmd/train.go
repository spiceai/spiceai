package cmd

import (
	"github.com/spf13/cobra"
)

var (
	trainUsingBaremetal bool
)

var TrainCmd = &cobra.Command{
	Use:   "train",
	Short: "Train a single flight using a specified pod manifest.",
	Args:  cobra.ExactArgs(1),
	Example: `
spice train cartpole-v1.yaml	
`,
	Run: func(cmd *cobra.Command, args []string) {
		Run(trainUsingBaremetal, args[0])
	},
}

func init() {
	TrainCmd.Flags().BoolVarP(&trainUsingBaremetal, "baremetal", "b", false, "Starts the runtime and AI Engine in a local process, not in Docker")
	RootCmd.AddCommand(TrainCmd)
}
