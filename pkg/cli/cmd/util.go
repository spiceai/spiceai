package cmd

import (
	"fmt"

	"github.com/spf13/viper"
	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/pods"
)

func GetPodAndConfiguration(podName string) (*pods.Pod, *config.SpiceConfiguration, error) {
	podPath := pods.FindFirstManifestPath()
	if podPath == "" {
		return nil, nil, fmt.Errorf("no pods detected")
	}

	pod, err := pods.LoadPodFromManifest(podPath)
	if err != nil {
		return nil, nil, err
	}

	if pod.Name != podName {
		fmt.Printf("the pod %s does not exist\n", podName)
		return nil, nil, fmt.Errorf("the pod %s does not exist", podName)
	}

	v := viper.New()
	runtimeConfig, err := config.LoadRuntimeConfiguration(v)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load runtime configuration: %w", err)
	}

	return pod, runtimeConfig, nil
}
