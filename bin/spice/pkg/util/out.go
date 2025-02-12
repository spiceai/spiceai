/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
	"bufio"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/olekukonko/tablewriter"
)

func ShowSpinner(done chan bool) {
	chars := []rune{'⣾', '⣽', '⣻', '⢿', '⡿', '⣟', '⣯', '⣷'}
	for {
		for _, char := range chars {
			select {
			case <-done:
				fmt.Print("\r") // Clear the spinner
				return
			default:
				fmt.Printf("\r%c ", char)
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
}

func WriteTable(items []interface{}) {
	if len(items) == 0 {
		return
	}

	// Get all headers dynamically, flattening embedded fields
	headers := getFlattenedHeaders(reflect.TypeOf(items[0]))

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

	// Process each item
	for _, item := range items {
		row := getFlattenedValues(reflect.ValueOf(item))
		table.Append(row)
	}

	table.Render()
}

// Recursively extracts flattened headers
func getFlattenedHeaders(t reflect.Type) []string {
	var headers []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Anonymous { // Embedded struct
			headers = append(headers, getFlattenedHeaders(field.Type)...) // Recursively extract
		} else {
			headers = append(headers, strings.TrimSuffix(field.Name, "Enabled"))
		}
	}
	return headers
}

// Recursively extracts flattened values
func getFlattenedValues(v reflect.Value) []string {
	var row []string
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Kind() == reflect.Struct { // Handle embedded structs
			row = append(row, getFlattenedValues(field)...) // Recursively extract
		} else {
			row = append(row, fmt.Sprintf("%v", field.Interface()))
		}
	}
	return row
}

func MarshalAndPrintTable(writer io.Writer, in interface{}) error {
	csvContent, err := gocsv.MarshalString(in)
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(writer)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorder(false)
	table.SetHeaderLine(false)
	table.SetRowLine(false)
	table.SetCenterSeparator("")
	table.SetRowSeparator("")
	table.SetColumnSeparator("")
	scanner := bufio.NewScanner(strings.NewReader(csvContent))
	header := true

	for scanner.Scan() {
		text := strings.Split(scanner.Text(), ",")

		if header {
			table.SetHeader(text)
			header = false
		} else {
			table.Append(text)
		}
	}

	table.Render()
	return nil
}
