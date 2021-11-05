package aiengine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spiceai/spiceai/pkg/context"
)

type LearningAlgorithm struct {
	Id   string `json:"algorithm_id"`
	Name string `json:"name"`
	DocsLink string `json:"docs_link"`
}

var (
	algorithms []*LearningAlgorithm
	algorithmsMap map[string]*LearningAlgorithm
)

func Algorithms() []*LearningAlgorithm {
	return algorithms
}

func LoadAlgorithms() error {
	algorithmsMap = make(map[string]*LearningAlgorithm)
	
	algorithmsDir := filepath.Join(context.CurrentContext().AIEngineDir(), "algorithms")

	entries, err := os.ReadDir(algorithmsDir)
	if err != nil {
		return fmt.Errorf("error reading algorithms directory '%s': %w", algorithmsDir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			algorithmJsonPath := filepath.Join(algorithmsDir, entry.Name(), fmt.Sprintf("%s.json", entry.Name()))
			if _, err := os.Stat(algorithmJsonPath); err != nil {
				continue
			}
			jsonData, err := os.ReadFile(algorithmJsonPath)
			if err != nil {
				return fmt.Errorf("error reading algorithm json file '%s': %w", algorithmJsonPath, err)
			}
			var algorithm *LearningAlgorithm
			err = json.Unmarshal(jsonData, &algorithm)
			if err != nil {
				return fmt.Errorf("error parsing algorithm json file '%s': %w", algorithmJsonPath, err)
			}
			algorithmId := strings.ToLower(entry.Name())
			algorithms = append(algorithms, algorithm)
			algorithmsMap[algorithmId] = algorithm
		}
	}

	sort.SliceStable(algorithms, func(i, j int) bool {
		return strings.Compare(algorithms[i].Name, algorithms[j].Name) < 0
	})

	return nil
}

func GetAlgorithm(id string) *LearningAlgorithm {
	return algorithmsMap[id]
}
