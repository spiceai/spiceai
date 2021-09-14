package pods

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"

	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/util"
)

var (
	podsMutex sync.RWMutex
	pods      = make(map[string]*Pod)
)

func Pods() map[string]*Pod {
	return pods
}

func GetPod(name string) *Pod {
	podsMutex.RLock()
	defer podsMutex.RUnlock()

	return pods[name]
}

func RemovePod(name string) {
	podsMutex.Lock()
	defer podsMutex.Unlock()

	delete(pods, name)
}

func FindPod(podName string) (*Pod, error) {
	podPath := FindFirstManifestPath()
	if podPath == "" {
		return nil, fmt.Errorf("no pods detected")
	}

	pod, err := LoadPodFromManifest(podPath)
	if err != nil {
		return nil, err
	}

	if pod.Name != podName {
		fmt.Printf("the pod %s does not exist\n", podName)
		return nil, fmt.Errorf("the pod %s does not exist", podName)
	}

	return pod, nil
}

func RemovePodByManifestPath(manifestPath string) {
	relativePath := context.CurrentContext().GetSpiceAppRelativePath(manifestPath)

	podsMutex.Lock()
	defer podsMutex.Unlock()

	for _, pod := range pods {
		if pod.ManifestPath() == manifestPath {
			log.Printf("Removing pod %s: %s\n", aurora.Bold(pod.Name), aurora.Gray(12, relativePath))
			RemovePod(pod.Name)
			return
		}
	}
}

func FindFirstManifestPath() string {
	podsPath := context.CurrentContext().PodsDir()
	files, err := ioutil.ReadDir(podsPath)
	if err != nil {
		log.Fatal(err.Error())
	}

	for _, file := range files {
		extension := filepath.Ext(file.Name())
		if extension == ".yml" || extension == ".yaml" {
			return filepath.Join(podsPath, file.Name())
		}
	}

	return ""
}

func LoadPodFromManifest(manifestPath string) (*Pod, error) {
	manifestHash, err := util.ComputeFileHash(manifestPath)
	if err != nil {
		log.Printf("Error: Failed to compute hash for manifest '%s: %s\n", manifestPath, err)
		return nil, err
	}

	pod, err := loadPod(manifestPath, manifestHash)
	if err != nil {
		log.Printf("Error: Failed to load manifest '%s': %s\n", manifestPath, err)
		return nil, err
	}

	existingPod, ok := pods[pod.Name]
	if ok {
		if existingPod.Hash() == pod.Hash() {
			// Pods are the same, ignore new pod
			return existingPod, nil
		}
		// Copy data from old pod
		pod.copyData(existingPod)
	}

	podsMutex.Lock()
	pods[pod.Name] = pod
	podsMutex.Unlock()

	return pod, nil
}
