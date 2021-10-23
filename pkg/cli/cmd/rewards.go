package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/spec"
	"github.com/spiceai/spiceai/pkg/util"
	"gopkg.in/yaml.v2"
)

var rewardCmd = &cobra.Command{
	Use:     "reward",
	Aliases: []string{"rewards"},
	Short:   "Maintains rewards",
	Example: `
spice reward add
`,
}

var rewardAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add Reward - adds a reward to your Spice pod",
	Example: `
spice reward add
`,
	Run: func(cmd *cobra.Command, args []string) {
		manifests := pods.FindAllManifestPaths()
		if len(manifests) == 0 {
			cmd.Println("No pods detected!")
			return
		}

		podPath := manifests[0]

		pod, err := pods.LoadPodFromManifest(podPath)
		if err != nil {
			cmd.Println(fmt.Errorf("error loading Pod %s: %w", podPath, err))
			return
		}

		// Check for existing rewards.  If they exist or are malformed, warn and do nothing.
		if pod.Training != nil {
			rewardsType := fmt.Sprintf("%T", pod.Training.Rewards)
			switch rewardsType {
			case "string":
				if pod.Training.Rewards.(string) != "uniform" {
					cmd.Println("Rewards section malformed!  'rewards' must be either 'uniform' or an array of rewards.")
					return
				}
			case "[]interface {}":
				var rewards []spec.RewardSpec
				err := viper.UnmarshalKey("training.rewards", &rewards)
				if err != nil {
					cmd.Println("Rewards section malformed!  'rewards' must be either 'uniform' or an array of rewards.")
					return
				} else if len(rewards) > 0 {
					cmd.Println("Pod already has rewards defined!")
					return
				}
			}
		}

		actions := pod.Actions()

		if len(actions) == 0 {
			cmd.Printf("No actions to add rewards to found in Pod %s\n", pod.Name)
			return
		}

		if pod.PodSpec.Training == nil {
			pod.PodSpec.Training = &spec.TrainingSpec{}
		}

		defaultRewards := []spec.RewardSpec{}
		for _, action := range pod.PodSpec.Actions {
			reward := spec.RewardSpec{Reward: action.Name, With: "reward = 1"}
			defaultRewards = append(defaultRewards, reward)
		}

		pod.Training.Rewards = defaultRewards

		marshalledPod, err := yaml.Marshal(pod.PodSpec)
		if err != nil {
			cmd.Println(fmt.Errorf(err.Error()))
			return
		}

		err = util.WriteToExistingFile(podPath, marshalledPod)
		if err != nil {
			cmd.Println(fmt.Errorf(err.Error()))
			return
		}

		fmt.Printf("Uniform rewards added to pod %s.\n", pod.Name)
	},
}

func init() {
	rewardCmd.AddCommand(rewardAddCmd)
	rewardCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(rewardCmd)
}
