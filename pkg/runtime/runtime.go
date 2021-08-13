package runtime

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/logrusorgru/aurora"
	"github.com/spf13/viper"
	"github.com/spiceai/spice/pkg/aiengine"
	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/environment"
	spice_http "github.com/spiceai/spice/pkg/http"
	"github.com/spiceai/spice/pkg/pods"
	"github.com/spiceai/spice/pkg/version"
)

type SpiceRuntime struct {
	config *config.SpiceConfiguration
	viper  *viper.Viper
}

var runtime SpiceRuntime

func (r *SpiceRuntime) LoadConfig() error {
	if r.viper == nil {
		r.viper = viper.New()
		r.viper.OnConfigChange(r.configChangeHandler)
	}

	var err error
	if r.config == nil {
		r.config, err = config.LoadRuntimeConfiguration(r.viper)
	}

	return err
}

func (r *SpiceRuntime) processPodsConfig() {
	if r.config.Pods == nil {
		return
	}

	for _, f := range r.config.Pods {
		pod := pods.GetPod(f.Name)
		if pod == nil {
			continue
		}

		if f.Models != nil && f.Models.Downloader != nil {
			connectionId := f.Models.Downloader.Uses
			connection, ok := r.config.Connections[connectionId]
			if !ok {
				log.Println("Warning: Connection", f.Models.Downloader.Uses, "not found.")
				continue
			}

			// TODO: Take this from .git
			branch := "trunk"
			if f.Models.Downloader.Branch != nil {
				branch = *f.Models.Downloader.Branch
			}

			log.Printf("Checking for pod %s model updates ...", pod.Name)
			tag, err := pod.DownloadModelUpdate(connectionId, connection, branch)
			if err != nil {
				log.Println("Warning: Failed to download model update for", pod.Name)
			}

			if tag == "" {
				log.Println("No new model updates.")
				continue
			}

			err = aiengine.LoadInferencing(pod, tag)
			if err != nil {
				log.Println("Error:", "Failed to reload inferencing with tag", tag)
			}

			log.Printf("Updated pod '%s' model to '%s'\n", pod.Name, tag)
		}
	}
}

func (r *SpiceRuntime) configChangeHandler(e fsnotify.Event) {
	configPath := config.GetSpiceAppRelativePath(e.Name)
	log.Println("Detected config change to", configPath)

	var newConfig *config.SpiceConfiguration
	err := r.viper.Unmarshal(&newConfig)
	if err != nil {
		log.Printf("Warning: Ignoring invalid change to %s\n", configPath)
	}

	r.config = newConfig

	r.processPodsConfig()
}

func Run(manifestPath string) error {
	runtime = SpiceRuntime{}

	err := runtime.LoadConfig()
	if err != nil {
		return err
	}

	fmt.Println("Loading Spice runtime ...")

	aiEngineReady := make(chan bool)
	aiengine.StartServer(aiEngineReady)

	singleRun := manifestPath != ""
	singleRunComplete := make(chan bool)
	spice_http.NewServer(runtime.config.HttpPort).Start(singleRun, singleRunComplete)

	<-aiEngineReady

	fmt.Printf("- Runtime version: %s\n", version.Version())
	fmt.Println(aurora.Green(fmt.Sprintf("- Listening on http://localhost:%d", runtime.config.HttpPort)))
	fmt.Println()
	fmt.Println("Use Ctrl-C to stop")

	// If we are in "single run" mode, wait for a single training run to complete, then exit
	if singleRun {
		pod, err := initializePod(manifestPath)
		if err != nil {
			return err
		}

		err = environment.StartDataListeners(15)
		if err != nil {
			return err
		}

		err = aiengine.StartTraining(pod)
		if err != nil {
			return err
		}

		<-singleRunComplete
		fmt.Println(aurora.Green("Exiting after a single training run."))
		os.Exit(0)
	}

	err = runtime.scanForPods()
	if err != nil {
		log.Printf("error scanning for pods: %s", err.Error())
		return err
	}

	err = watchPods()
	if err != nil {
		return err
	}

	err = environment.StartDataListeners(15)
	if err != nil {
		return err
	}

	return nil
}

func (r *SpiceRuntime) scanForPods() error {
	_, err := os.Stat(config.AppSpicePath())
	if err != nil {
		// No .spice means no pods
		return nil
	}

	podsManifestDir := config.PodsManifestsPath()
	_, err = os.Stat(podsManifestDir)
	if err != nil {
		// No .spice/pods means no pods
		return nil
	}

	d, err := os.Open(podsManifestDir)
	if err != nil {
		return err
	}

	files, err := d.Readdir(-1)
	d.Close()
	if err != nil {
		return err
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		manifestPath := filepath.Join(podsManifestDir, f.Name())
		_, err = initializePod(manifestPath)
		if err != nil {
			log.Println(fmt.Errorf("error loading pod manifest %s: %w", manifestPath, err))
			continue
		}
	}

	return nil
}

func initializePod(manifestPath string) (*pods.Pod, error) {
	newPod, err := pods.LoadPodFromManifest(manifestPath)
	if err != nil {
		log.Println(fmt.Errorf("error loading pod manifest %s: %w", manifestPath, err))
		return nil, err
	}

	pods.CreateOrUpdatePod(newPod)
	err = aiengine.InitializePod(newPod)
	if err != nil {
		log.Println(fmt.Errorf("error initializing pod %s: %w", newPod.Name, err))
		return nil, err
	}

	for _, ds := range newPod.DataSources() {
		fmt.Printf("Loaded datasource %s\n", aurora.BrightCyan(ds.Name()))
	}

	return newPod, nil
}

func Shutdown() {
	log.Println("Shutting down...")

	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := aiengine.StopServer()
		if err != nil {
			// TODO: Log to verbose log
			return
		}
	}()

	wg.Wait()
}
