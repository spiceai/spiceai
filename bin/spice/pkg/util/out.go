/*
Copyright 2021-2024 The Spice Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func WriteTable(items []interface{}) {
	if len(items) == 0 {
		return
	}

	t := reflect.TypeOf(items[0])

	headers := make([]string, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		headers[i] = strings.TrimSuffix(t.Field(i).Name, "Enabled")
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(headers)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(true)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetTablePadding(" ")
	table.SetNoWhiteSpace(true)

	for _, item := range items {
		v := reflect.ValueOf(item)
		row := make([]string, v.NumField())
		for i := 0; i < v.NumField(); i++ {
			row[i] = fmt.Sprintf("%v", v.Field(i))
		}
		table.Append(row)
	}

	table.Render()
}
