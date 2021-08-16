package runtime

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/spiceai/spice/pkg/aiengine"
	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/pods"
)

func ensurePodsPathExists() error {
	if _, err := os.Stat(config.PodsManifestsPath()); os.IsNotExist(err) {
		err := os.MkdirAll(config.PodsManifestsPath(), 0766)
		if err != nil {
			return err
		}
	}
	return nil
}

func watchPods() error {
	spiceWorkspace := config.PodsManifestsPath()
	if err := ensurePodsPathExists(); err != nil {
		// Ignore this error, just don't watch
		return nil
	}

	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Println(fmt.Errorf("error starting '%s' watcher: %w", spiceWorkspace, err))
		}
		defer watcher.Close()

		if err := watcher.Add(spiceWorkspace); err != nil {
			log.Println(fmt.Errorf("error starting '%s' watcher: %w", spiceWorkspace, err))
		}
		for {
			select {
			case event := <-watcher.Events:
				err := processNotifyEvent(event)
				if err != nil {
					log.Println(err)
				}
			case err := <-watcher.Errors:
				log.Println(fmt.Errorf("error from '%s' watcher: %w", spiceWorkspace, err))
			}
		}
	}()

	return nil
}

func processNotifyEvent(event fsnotify.Event) error {
	manifestPath := event.Name
	ext := filepath.Ext(manifestPath)
	if ext != ".yml" && ext != ".yaml" {
		// Ignore non-YAML files
		return nil
	}

	switch event.Op {
	case fsnotify.Create:
		pod, err := pods.LoadPodFromManifest(manifestPath)
		if err != nil {
			return err
		}
		err = startNewPodTraining(pod)
		if err != nil {
			return err
		}
	case fsnotify.Write:
		newPod, err := pods.LoadPodFromManifest(manifestPath)
		if err != nil {
			return err
		}
		existingPod := pods.GetPod(newPod.Name)
		if existingPod != nil && newPod.Hash() == existingPod.Hash() {
			// Nothing changed, ignore
			break
		}
		// TODO: Check if datasources have actually changed
		err = startNewPodTraining(newPod)
		if err != nil {
			return err
		}
	case fsnotify.Remove:
		pods.RemovePodByManifestPath(manifestPath)
		return nil
	}

	return nil
}

func startNewPodTraining(pod *pods.Pod) error {
	pods.CreateOrUpdatePod(pod)
	err := aiengine.InitializePod(pod)
	if err != nil {
		return err
	}

	podState, err := pod.FetchNewData()
	if err != nil {
		return err
	}

	err = aiengine.SendData(pod, podState...)
	if err != nil {
		return err
	}

	err = aiengine.StartTraining(pod)
	if err != nil {
		return err
	}

	return nil
}
