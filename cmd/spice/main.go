package main

import (
	"github.com/spiceai/spiceai/pkg/cli/cmd"
	"github.com/spiceai/spiceai/pkg/version"
)

func main() {
	version.SetComponent("spice")
	cmd.Execute()
}
