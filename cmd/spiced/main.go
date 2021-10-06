package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/loggers"
	"github.com/spiceai/spiceai/pkg/runtime"
	"github.com/spiceai/spiceai/pkg/version"
)

var (
	contextFlag     string
	developmentMode bool
)

func main() {
	version.SetComponent("spiced")

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

		context.SetContext(rtcontext)

		var manifestPath string
		if len(args) > 0 {
			manifestPath = args[0]
		}

		isSingleRun := manifestPath != ""

		runtime := runtime.GetSpiceRuntime()
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
	runtime := runtime.GetSpiceRuntime()
	RootCmd.Flags().StringVar(&contextFlag, "context", "metal", "Runs Spice.ai in the given context, either 'docker' or 'metal'")
	RootCmd.Flags().BoolVarP(&developmentMode, "development", "d", false, "Runs Spice.ai in development mode.")
	err := runtime.BindFlags(RootCmd.Flags().Lookup("development"))
	if err != nil {
		fmt.Printf("error initializing: %s", err)
		return
	}
	RootCmd.AddCommand(VersionCmd)
}
