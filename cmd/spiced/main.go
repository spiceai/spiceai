package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spiceai/spice/pkg/context"
	"github.com/spiceai/spice/pkg/loggers"
	"github.com/spiceai/spice/pkg/runtime"
	"github.com/spiceai/spice/pkg/version"
)

var (
	contextFlag string
)

func main() {
	logger := loggers.ZapLogger()
	if logger == nil {
		os.Exit(-1)
	}
	defer loggers.ZapLoggerSync()

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

var RootCmd = &cobra.Command{
	Use:   "spiced",
	Short: "Spice Runtime",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext, err := context.NewContext(contextFlag)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		err = rtcontext.Init()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		var manifestPath string
		if len(args) > 0 {
			manifestPath = args[0]
		}

		isSingleRun := manifestPath != ""

		if isSingleRun {
			err = runtime.SingleRun(manifestPath)
		} else {
			err = runtime.Run()
		}
		if err != nil {
			log.Fatalln(err)
		}
		defer runtime.Shutdown()

		if !isSingleRun {
			stop := make(chan os.Signal, 1)
			signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
			<-stop
		}
	},
}

var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(version.Version())
	},
}

func init() {
	RootCmd.Flags().StringVar(&contextFlag, "context", "metal", "Runs Spice.ai in the given context, either 'docker' or 'metal'")
	RootCmd.AddCommand(VersionCmd)
}
