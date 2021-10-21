package runtime

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/spiceai/spiceai/pkg/aiengine"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/environment"
	"github.com/spiceai/spiceai/pkg/pods"
)

func ensurePodsPathExists() error {
	podsDir := context.CurrentContext().PodsDir()
	if _, err := os.Stat(podsDir); os.IsNotExist(err) {
		err := os.MkdirAll(podsDir, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

func watchPods() error {
	podsDir := context.CurrentContext().PodsDir()
	if err := ensurePodsPathExists(); err != nil {
		// Ignore this error, just don't watch
		return nil
	}

	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Println(fmt.Errorf("error starting '%s' watcher: %w", podsDir, err))
		}
		defer watcher.Close()

		if err := watcher.Add(podsDir); err != nil {
			log.Println(fmt.Errorf("error starting '%s' watcher: %w", podsDir, err))
		}
		for {
			select {
			case event := <-watcher.Events:
				err := processNotifyEvent(event)
				if err != nil {
					log.Println(err)
				}
			case err := <-watcher.Errors:
				log.Println(fmt.Errorf("error from '%s' watcher: %w", podsDir, err))
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
		if newPod.IsSame(existingPod) {
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

	err = environment.InitPodDataConnector(pod)
	if err != nil {
		return err
	}

	podState := pod.CachedState()
	err = aiengine.SendData(pod, podState...)
	if err != nil {
		return err
	}

	// Pass empty algorithm and negative episode number string to use pod's default
	err = aiengine.StartTraining(pod, "", -1)
	if err != nil {
		return err
	}

	return nil
}
