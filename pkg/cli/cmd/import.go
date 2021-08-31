package cmd

import (
	"fmt"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/pkg/cli/runtime"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/spiceai/spiceai/pkg/util"
	"google.golang.org/protobuf/proto"
)

var importTag string

var ImportCmd = &cobra.Command{
	Use:   "import",
	Short: "Import Pod - import a pod",
	Example: `
spice import <path-to-pod>
spice import ./models/trader.spicepod

spice import --tag [tag-name] [path-to-pod]
spice import --tag latest ./models/trader.spicepod
`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		archivePath := args[0]

		err := validateExtension(archivePath)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		relativePath, err := getRelativePathFromCurrentDirectory(archivePath)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		var init *aiengine_pb.InitRequest = nil
		err = util.ProcessAFileInZipArchive(archivePath, "init.pb", func(initBytes []byte) error {
			init = new(aiengine_pb.InitRequest)
			err = proto.Unmarshal(initBytes, init)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		if init == nil {
			fmt.Println("Invalid spicepod: " + archivePath)
			return
		}

		runtimeClient, err := runtime.NewRuntimeClient(init.Pod)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		err = runtimeClient.ImportModel(relativePath, importTag)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		fmt.Println(aurora.Green("Imported trained model!"))
	},
}

func init() {
	ImportCmd.Flags().StringVar(&importTag, "tag", "latest", "Specify which tag to import the model to")
	RootCmd.AddCommand(ImportCmd)
}
