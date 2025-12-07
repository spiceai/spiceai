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
	"github.com/olekukonko/tablewriter/tw"
)

var (
	borderlessRendition = tw.Rendition{
		Borders: tw.BorderNone,
		Symbols: tw.NewSymbols(tw.StyleNone),
		Settings: tw.Settings{
			Lines:      tw.LinesNone,
			Separators: tw.SeparatorsNone,
		},
	}
	compactPadding = tw.Padding{
		Left:      "",
		Right:     " ",
		Overwrite: true,
	}
)

func newBorderlessTable(writer io.Writer) *tablewriter.Table {
	return tablewriter.NewTable(
		writer,
		tablewriter.WithHeaderAlignment(tw.AlignLeft),
		tablewriter.WithHeaderAutoFormat(tw.On),
		tablewriter.WithRowAlignment(tw.AlignLeft),
		tablewriter.WithRowAutoWrap(tw.WrapTruncate),
		tablewriter.WithTrimSpace(tw.Off),
		tablewriter.WithPadding(compactPadding),
		tablewriter.WithRendition(borderlessRendition),
	)
}

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

	table := newBorderlessTable(os.Stdout)
	table.Header(headers)

	// Process each item
	for _, item := range items {
		row := getFlattenedValues(reflect.ValueOf(item))
		if err := table.Append(row); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to append row: %v\n", err)
			return
		}
	}

	if err := table.Render(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to render table: %v\n", err)
	}
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

	table := newBorderlessTable(writer)
	scanner := bufio.NewScanner(strings.NewReader(csvContent))
	header := true

	for scanner.Scan() {
		text := strings.Split(scanner.Text(), ",")

		if header {
			table.Header(text)
			header = false
		} else {
			if err := table.Append(text); err != nil {
				return err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return table.Render()
}
