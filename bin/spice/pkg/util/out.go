package util

import (
	"os"
	"reflect"

	"github.com/olekukonko/tablewriter"
)

func WriteTable(items []interface{}) {
	if len(items) == 0 {
		return
	}

	t := reflect.TypeOf(items[0])

	headers := make([]string, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		headers[i] = t.Field(i).Name
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
			row[i] = v.Field(i).String()
		}
		table.Append(row)
	}

	table.Render()
}
