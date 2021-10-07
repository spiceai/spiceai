package diagnostics

import (
	"fmt"
	"os"
	"strings"

	"github.com/spiceai/spiceai/pkg/context"
)

func GenerateReport() (string, error) {
	body := strings.Builder{}

	body.WriteString("## Diagnostics Report\n\n")

	body.WriteString("Runtime Context\n")
	body.WriteString("---------------\n")
	context := context.CurrentContext()
	body.WriteString(fmt.Sprintf("name: %s\n", context.Name()))
	body.WriteString(fmt.Sprintf("app_dir: %s\n", context.AppDir()))
	body.WriteString(fmt.Sprintf("pods_dir: %s\n", context.PodsDir()))
	body.WriteString("\n\n")

	podsDirEntries, err := os.ReadDir(context.PodsDir())
	if err != nil {
		return "", err
	}

	body.WriteString(fmt.Sprintf("Pods Directory Contents (%d entries)\n", len(podsDirEntries)))
	body.WriteString("---------------\n")

	for _, entry := range podsDirEntries {
		body.WriteString(entry.Name())
		body.WriteString("\n")
	}

	body.WriteString("\n")

	return body.String(), nil
}
