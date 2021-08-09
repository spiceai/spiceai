package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spiceai/spice/pkg/runtime"
	"github.com/spiceai/spice/pkg/version"
)

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

var RootCmd = &cobra.Command{
	Use:   "spiced",
	Short: "Spice Runtime",
	Run: func(cmd *cobra.Command, args []string) {
		err := runtime.Run()
		if err != nil {
			log.Fatalln(err)
		}
		defer runtime.Shutdown()

		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
		<-stop
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
	RootCmd.AddCommand(VersionCmd)
}
