package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
)

const (
	spicePodExtension = ".spicepod"
)

var (
	exportTag       string
	exportOverwrite bool
	exportOutput    string
)

var ExportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export Pod - export a Spice AI Pod",
	Example: `
spice export [pod-name] -o [path-to-export-directory]
spice export trader -o ./models
`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		podName := args[0]

		pathResult, err := getValidExportPath(podName, exportOutput)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		runtimeClient, err := NewRuntimeClient(podName)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		err = runtimeClient.ExportModel(pathResult, exportTag)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	},
}

type exportPathResult struct {
	directory string
	filename  string
}

func validateExtension(spicePodPath string) error {
	if filepath.Ext(spicePodPath) != spicePodExtension {
		return fmt.Errorf("%s: the filename should end with '%s'", aurora.Red("error"), spicePodExtension)
	}

	return nil
}

func getValidExportPath(podName string, exportPath string) (*exportPathResult, error) {
	var result exportPathResult

	var directory string
	statResult, err := os.Stat(exportPath)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		// Assume this is a file to write as a zip, unless it doesn't have an extension
		if filepath.Ext(exportPath) == "" || filepath.Ext(exportPath) == exportPath {
			return nil, fmt.Errorf("%s: the export directory '%s' doesn't exist", aurora.Red("error"), aurora.Blue(exportPath))
		}

		err = validateExtension(exportPath)
		if err != nil {
			return nil, err
		}

		// Check the parent folder exists
		parentDirectory := filepath.Dir(exportPath)
		_, err := os.Stat(parentDirectory)
		if err != nil && errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%s: the directory '%s' doesn't exist", aurora.Red("error"), aurora.Blue(parentDirectory))
		}
		directory = parentDirectory
		result.filename = filepath.Base(exportPath)

	} else if err == nil && statResult.IsDir() {
		// This is a directory to write to, generate a filename
		directory = exportPath
		result.filename = fmt.Sprintf("%s%s", podName, spicePodExtension)

		generatedModelExport := filepath.Join(exportPath, result.filename)
		_, err := os.Stat(generatedModelExport)

		// We're expecting an error, but it should be os.ErrNotExist
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}

		if err == nil && !exportOverwrite {
			return nil, fmt.Errorf("%s: not overwriting the existing model at '%s', specify --overwrite to override this behavior", aurora.Red("error"), aurora.Blue(generatedModelExport))
		}
	} else if err == nil {
		err = validateExtension(exportPath)
		if err != nil {
			return nil, err
		}

		// This is a file that already exists, check that we should overwrite
		if !exportOverwrite {
			return nil, fmt.Errorf("%s: not overwriting the existing model at '%s', specify --overwrite to override this behavior", aurora.Red("error"), aurora.Blue(exportPath))
		}
		directory = filepath.Dir(exportPath)
		result.filename = filepath.Base(exportPath)
	}

	relativeDirectory, err := getRelativePathFromCurrentDirectory(directory)
	if err != nil {
		return nil, err
	}

	result.directory = relativeDirectory

	return &result, nil
}

func getRelativePathFromCurrentDirectory(targetPath string) (string, error) {
	currentDirectory, err := os.Getwd()
	if err != nil {
		return "", err
	}

	absolutePath, err := filepath.Abs(targetPath)
	if err != nil {
		return "", err
	}

	relativeDirectory, err := filepath.Rel(currentDirectory, absolutePath)
	if err != nil {
		return "", err
	}

	if strings.HasPrefix(relativeDirectory, "..") {
		return "", fmt.Errorf("%s: the directory [%s] should be located within the current directory [%s]", aurora.Red("error"), aurora.Blue(absolutePath), aurora.Blue(currentDirectory))
	}

	return relativeDirectory, nil
}

func init() {
	ExportCmd.Flags().StringVar(&exportTag, "tag", "latest", "The tag to export the model from")
	ExportCmd.Flags().BoolVar(&exportOverwrite, "overwrite", false, "Overwrite a file that already exists")
	ExportCmd.Flags().StringVarP(&exportOutput, "output", "o", ".", "The output directory")
	RootCmd.AddCommand(ExportCmd)
}
