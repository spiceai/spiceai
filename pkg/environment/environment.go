package environment

import (
	"log"
	"time"

	"github.com/spiceai/spiceai/pkg/aiengine"
	"github.com/spiceai/spiceai/pkg/pods"
)

func StartDataListeners(intervalSecs int) error {
	_, err := FetchNewData()
	if err != nil {
		log.Println(err)
		return err
	}

	// HACKHACK: Polled fetch for now (TODO data sources subscribe with push model)
	ticker := time.NewTicker(time.Duration(intervalSecs) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				{
					_, err := FetchNewData()
					if err != nil {
						log.Println(err)
						return
					}
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	return nil
}

func FetchNewData() (bool, error) {
	for _, pod := range *pods.Pods() {
		state, err := pod.State()
		if err != nil {
			log.Printf("%v", err)
			continue
		}

		err = aiengine.SendData(pod, state...)
		if err != nil {
			log.Printf("%v", err)
			continue
		}
	}

	return true, nil
}
