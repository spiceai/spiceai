package cmd

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

// pub dependencies: Vec<String>,

type SpiceSecretStore string

const (
	File       SpiceSecretStore = "file"
	Env        SpiceSecretStore = "env"
	Kubernetes SpiceSecretStore = "kubernetes"
	Keyring    SpiceSecretStore = "keyring"
)

type Spicepod struct {
	Name    string `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Secrets struct {
		Store SpiceSecretStore `json:"store,omitempty" csv:"store" yaml:"store,omitempty"`
	} `json:"secrets,omitempty" csv:"secrets" yaml:"secrets,omitempty"`
	Datasets     []api.Dataset `json:"datasets,omitempty" csv:"datasets" yaml:"datasets,omitempty"`
	Models       []api.Model   `json:"models,omitempty" csv:"models" yaml:"models,omitempty"`
	Dependencies []string      `json:"dependencies,omitempty" csv:"dependencies" yaml:"dependencies,omitempty"`
}

type SpicepodRow struct {
	Name         string           `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	SecretStore  SpiceSecretStore `json:"store,omitempty" csv:"store" yaml:"store,omitempty"`
	Datasets     string           `json:"datasets,omitempty" csv:"datasets" yaml:"datasets,omitempty"`
	Models       string           `json:"models,omitempty" csv:"models" yaml:"models,omitempty"`
	Dependencies string           `json:"dependencies,omitempty" csv:"dependencies" yaml:"dependencies,omitempty"`
}

var spicepodsCmd = &cobra.Command{
	Use:   "pods",
	Short: "Lists spicepods loaded by the Spice runtime",
	Example: `
spice pods
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		data, err := api.GetData[Spicepod](rtcontext, "/v1/spicepods", Spicepod{})

		table := make([]interface{}, 0, len(data))
		for _, v := range data {
			for i := range v.Datasets {
				table = append(table, SpicepodRow{
					Name:         v.Name,
					SecretStore:  v.Secrets.Store,
					Datasets:     v.Datasets[i].CellDisplay(),
					Models:       "",
					Dependencies: strings.Join(v.Dependencies, ", "),
				})
			}
			for i := range v.Models {
				table = append(table, SpicepodRow{
					Name:         v.Name,
					SecretStore:  v.Secrets.Store,
					Datasets:     "",
					Models:       v.Models[i].CellDisplay(),
					Dependencies: strings.Join(v.Dependencies, ", "),
				})
			}

			table = append(table, SpicepodRow{
				Name:         v.Name,
				SecretStore:  v.Secrets.Store,
				Datasets:     "",
				Models:       "",
				Dependencies: strings.Join(v.Dependencies, ", "),
			})
		}

		util.WriteTable(table, 0, 1, 4)
		if err != nil {
			cmd.PrintErrln(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(spicepodsCmd)
}
