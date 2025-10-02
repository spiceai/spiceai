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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/dustin/go-humanize"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
	"github.com/spiceai/gospice/v7"
	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
	rtcontext "github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/display"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

// QueryExecutor defines a function that executes a SQL query with a cancellable context
type QueryExecutor func(ctx context.Context, query string) error

var sqlCmd = &cobra.Command{
	Use:   "sql",
	Short: "Start an interactive SQL query session against the Spice.ai runtime",
	Example: `
$ spice sql
Welcome to the Spice.ai SQL REPL! Type 'help' for help.

show tables;  -- list available tables
sql> show tables
+---------------+--------------------+---------------+------------+
| table_catalog | table_schema       | table_name    | table_type |
+---------------+--------------------+---------------+------------+
| datafusion    | public             | tmp_view_test | VIEW       |
| datafusion    | information_schema | tables        | VIEW       |
| datafusion    | information_schema | views         | VIEW       |
| datafusion    | information_schema | columns       | VIEW       |
| datafusion    | information_schema | df_settings   | VIEW       |
+---------------+--------------------+---------------+------------+
`,
	Args: cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, err := rtcontext.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			return
		}

		// Check if endpoint is provided - if so, use remote mode
		endpoint, err := cmd.Flags().GetString("endpoint")
		if err != nil {
			slog.Error("getting endpoint flag", "error", err)
			os.Exit(1)
		}

		// Check for --flight-endpoint flag (legacy support, treated as gRPC endpoint)
		flightEndpoint, err := cmd.Flags().GetString("flight-endpoint")
		if err != nil {
			slog.Error("getting flight-endpoint flag", "error", err)
			os.Exit(1)
		}

		if flightEndpoint != "" {
			// Treat flight-endpoint as a gRPC endpoint
			// Add grpc:// scheme if no scheme is provided
			if !strings.HasPrefix(flightEndpoint, "grpc://") && !strings.HasPrefix(flightEndpoint, "grpc+tls://") {
				flightEndpoint = "grpc://" + flightEndpoint
			}
			endpoint = flightEndpoint
		}

		// Check for --cloud flag
		if ctx.IsCloud() {
			if endpoint != "" {
				slog.Error("cannot use both --cloud and --endpoint/--flight-endpoint flags")
				os.Exit(1)
			}

			// Get API key from context or environment variable
			apiKey := os.Getenv("SPICE_API_KEY")
			if apiKey == "" {
				if cmdApiKey, err := ctx.GetApiKey(); err == nil && cmdApiKey != "" {
					apiKey = cmdApiKey
				}
			}

			if apiKey == "" {
				slog.Error("API key is required when using --cloud. Set SPICE_API_KEY environment variable or use --api-key flag.")
				os.Exit(1)
			}

			// Use cloud connection
			if err := runCloudREPL(cmd, apiKey); err != nil {
				slog.Error("running spice sql REPL", "error", err)
				os.Exit(1)
			}
			return
		}

		if endpoint != "" {
			// Determine if it's HTTP or gRPC based on the endpoint scheme
			if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
				// Use HTTP-based REPL
				if err := runHTTPREPL(cmd, ctx, endpoint); err != nil {
					slog.Error("running spice sql REPL", "error", err)
					os.Exit(1)
				}
			} else if strings.HasPrefix(endpoint, "grpc://") || strings.HasPrefix(endpoint, "grpc+tls://") {
				// Use gRPC-based REPL with gospice SDK
				if err := runGRPCREPL(cmd, ctx, endpoint); err != nil {
					slog.Error("running spice sql REPL", "error", err)
					os.Exit(1)
				}
			} else {
				slog.Error("invalid endpoint scheme. Use http://, https://, grpc://, or grpc+tls://")
				os.Exit(1)
			}
			return
		}

		// Local mode (existing behavior)
		_, err = ctx.Version()
		if err != nil {
			slog.Error("Failed to run `spice sql`: The Spice runtime is not installed. Run `spice install` and retry.")
			return
		}

		spiceArgs := []string{"--repl"}

		args = append(spiceArgs, args...)

		execCmd, err := ctx.GetRunCmd(args)
		if err != nil {
			slog.Error("getting run command", "error", err)
			os.Exit(1)
		}

		execCmd.Stderr = os.Stderr
		execCmd.Stdout = os.Stdout
		execCmd.Stdin = os.Stdin

		err = util.RunCommand(execCmd)
		if err != nil {
			slog.Error("running command", "error", err, "command", execCmd.String())
			os.Exit(1)
		}
	},
}

func runCloudREPL(cmd *cobra.Command, apiKey string) error {
	// Initialize gospice client
	spiceClient := gospice.NewSpiceClient()
	defer func() {
		if err := spiceClient.Close(); err != nil {
			slog.Error("closing Spice client", "error", err)
		}
	}()

	// Build init options for cloud
	initOpts := []gospice.SpiceClientModifier{
		gospice.WithApiKey(apiKey),
		gospice.WithSpiceCloudAddress(),
	}

	if err := spiceClient.Init(initOpts...); err != nil {
		return fmt.Errorf("initializing Spice cloud client: %w", err)
	}

	// Create gRPC query executor
	cloudExecutor := func(ctx context.Context, query string) error {
		startTime := time.Now()
		reader, err := spiceClient.Query(ctx, query)
		if err != nil {
			return formatSQLError(err)
		}
		defer reader.Release()

		// Display results in table format
		rowCount, dataSize, err := displayArrowResults(reader)
		if err != nil {
			return fmt.Errorf("displaying results: %w", err)
		}

		duration := time.Since(startTime)
		transferRate := float64(dataSize) / duration.Seconds()
		fmt.Println()
		fmt.Printf("Time: %v seconds. %d rows. %s (%s/sec).\n",
			duration.Seconds(), rowCount, humanize.IBytes(dataSize), humanize.IBytes(uint64(transferRate)))
		return nil
	}

	// Use the consolidated REPL
	return runREPL("spice-cloud", cloudExecutor)
}

func runGRPCREPL(cmd *cobra.Command, ctx *rtcontext.RuntimeContext, grpcEndpoint string) error {

	// Get API key from context or environment variable
	apiKey := os.Getenv("SPICE_API_KEY")
	if apiKey == "" {
		if cmdApiKey, err := ctx.GetApiKey(); err == nil && cmdApiKey != "" {
			apiKey = cmdApiKey
		}
	}

	// Initialize gospice client
	spiceClient := gospice.NewSpiceClient()
	defer func() {
		if err := spiceClient.Close(); err != nil {
			slog.Error("closing Spice client", "error", err)
		}
	}()

	// Strip grpc:// or grpc+tls:// scheme for gospice SDK
	flightAddress := grpcEndpoint
	flightAddress = strings.TrimPrefix(flightAddress, "grpc://")
	flightAddress = strings.TrimPrefix(flightAddress, "grpc+tls://")

	// Build init options
	initOpts := []gospice.SpiceClientModifier{gospice.WithFlightAddress(flightAddress)}
	if apiKey != "" {
		initOpts = append(initOpts, gospice.WithApiKey(apiKey))
	}

	if err := spiceClient.Init(initOpts...); err != nil {
		return fmt.Errorf("initializing Spice client: %w", err)
	}

	// Create gRPC query executor
	grpcExecutor := func(ctx context.Context, query string) error {
		startTime := time.Now()
		reader, err := spiceClient.Query(ctx, query)
		if err != nil {
			return formatSQLError(err)
		}
		defer reader.Release()

		// Display results in table format
		rowCount, dataSize, err := displayArrowResults(reader)
		if err != nil {
			return fmt.Errorf("displaying results: %w", err)
		}

		duration := time.Since(startTime)
		transferRate := float64(dataSize) / duration.Seconds()
		fmt.Println()
		fmt.Printf("Time: %v seconds. %d rows. %s (%s/sec).\n",
			duration.Seconds(), rowCount, humanize.IBytes(dataSize), humanize.IBytes(uint64(transferRate)))
		return nil
	}

	// Use the consolidated REPL
	return runREPL(grpcEndpoint, grpcExecutor)
}

// runREPL provides a common REPL experience for all SQL execution modes
func runREPL(endpoint string, executor QueryExecutor) error {
	fmt.Println("Welcome to the Spice.ai SQL REPL! Type 'help' for help.")
	fmt.Println()
	if endpoint == "spice-cloud" {
		fmt.Println("Connected to Spice Cloud")
	} else {
		fmt.Println("Connected to remote Spice instance at:", endpoint)
	}
	fmt.Println()

	// Setup liner for REPL
	line := liner.NewLiner()
	line.SetCtrlCAborts(true)
	defer func() {
		if err := line.Close(); err != nil {
			slog.Error("closing line", "error", err)
		}
	}()

	// Set up signal handling for query cancellation
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	// Mutex to protect query execution state
	var queryMutex sync.Mutex
	var cancelQuery context.CancelFunc
	var isQueryRunning bool

	// Handle signals in a goroutine
	go func() {
		for range sigChan {
			queryMutex.Lock()
			if isQueryRunning && cancelQuery != nil {
				// Cancel the running query
				cancelQuery()
				fmt.Println("\nQuery cancelled.")
			}
			queryMutex.Unlock()
		}
	}()

	for {
		query, err := line.Prompt("sql> ")
		if err == liner.ErrPromptAborted {
			break
		} else if err != nil {
			slog.Error("reading line", "error", err)
			continue
		}

		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}

		if strings.ToLower(query) == "help" {
			fmt.Println("Enter SQL queries to execute against the remote Spice instance.")
			fmt.Println("Press Ctrl+C to cancel a running query or Ctrl+D to exit.")
			continue
		}

		if strings.ToLower(query) == "exit" || strings.ToLower(query) == "quit" {
			break
		}

		line.AppendHistory(query)

		// Create a cancellable context for this query
		var queryContext context.Context
		var cancel context.CancelFunc
		queryMutex.Lock()
		queryContext, cancel = context.WithCancel(context.Background())
		cancelQuery = cancel
		isQueryRunning = true
		queryMutex.Unlock()

		// Execute query using the provided executor
		err = executor(queryContext, query)

		queryMutex.Lock()
		isQueryRunning = false
		queryCancelled := queryContext.Err() != nil
		queryMutex.Unlock()

		// Always cancel the context to prevent leak
		cancel()

		if err != nil {
			if queryCancelled {
				// Query was cancelled, continue to next prompt
				continue
			}
			fmt.Printf("\033[31mError:\033[0m %v\n", err)
			continue
		}
	}

	return nil
}

func runHTTPREPL(cmd *cobra.Command, ctx *rtcontext.RuntimeContext, httpEndpoint string) error {
	// Get API key from context or environment variable
	apiKey := os.Getenv("SPICE_API_KEY")
	if apiKey == "" {
		if cmdApiKey, err := ctx.GetApiKey(); err == nil && cmdApiKey != "" {
			apiKey = cmdApiKey
		}
	}

	// Create HTTP client
	httpClient := &http.Client{
		Timeout: 0, // No timeout for long-running queries
	}

	// Create HTTP query executor
	httpExecutor := func(ctx context.Context, query string) error {
		req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/v1/sql", httpEndpoint), strings.NewReader(query))
		if err != nil {
			return fmt.Errorf("creating request: %w", err)
		}

		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Accept", "application/json")
		if apiKey != "" {
			req.Header.Set("X-API-Key", apiKey)
		}

		startTime := time.Now()
		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				slog.Error("closing response body", "error", err)
			}
		}()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("reading response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		}

		// Parse JSON response and display as table
		rowCount, dataSize, err := displayJSONResults(body)
		if err != nil {
			return fmt.Errorf("displaying results: %w", err)
		}

		duration := time.Since(startTime)
		transferRate := float64(dataSize) / duration.Seconds()

		// Check cache status header
		cacheStatus := resp.Header.Get("Results-Cache-Status")
		cachedStr := ""
		if cacheStatus == "HIT" {
			cachedStr = " (cached)"
		}

		fmt.Println()
		fmt.Printf("Time: %v seconds. %d rows%s. %s (%s/sec).\n",
			duration.Seconds(), rowCount, cachedStr, humanize.IBytes(dataSize), humanize.IBytes(uint64(transferRate)))
		return nil
	}

	// Use the consolidated REPL
	return runREPL(httpEndpoint, httpExecutor)
}

// displayJSONResults parses JSON response and displays it as a table
func displayJSONResults(jsonData []byte) (int, uint64, error) {
	// Parse JSON response - expected format is an object with rows array and schema
	var response struct {
		RowCount int                      `json:"rowCount"`
		Schema   []map[string]interface{} `json:"schema"`
		Rows     []map[string]interface{} `json:"rows"`
	}
	if err := json.Unmarshal(jsonData, &response); err != nil {
		return 0, 0, fmt.Errorf("parsing JSON response: %w", err)
	}

	records := response.Rows
	if len(records) == 0 {
		fmt.Println("No results.")
		return 0, 0, nil
	}

	// Extract column names and types from schema if available, otherwise from first record
	var colNames []string
	var colTypes []string
	if len(response.Schema) > 0 {
		for _, field := range response.Schema {
			if name, ok := field["name"].(string); ok {
				colNames = append(colNames, name)
				// Extract type if available
				typeName := ""
				if typeObj, ok := field["type"].(map[string]interface{}); ok {
					if name, ok := typeObj["name"].(string); ok {
						typeName = name
					}
				}
				colTypes = append(colTypes, typeName)
			}
		}
	} else {
		// Fallback: extract from first record
		for colName := range records[0] {
			colNames = append(colNames, colName)
		}
		// Sort column names for consistent ordering
		sort.Strings(colNames)
		colTypes = nil // No types available
	}

	// Collect all row data as strings and calculate column widths
	type tableData struct {
		colNames []string
		rows     [][]string
	}
	var data tableData
	data.colNames = colNames
	colWidths := make([]int, len(colNames))

	// Initialize widths with column name lengths and type lengths
	for i, colName := range colNames {
		colWidths[i] = len(colName)
		if colTypes != nil && i < len(colTypes) {
			colWidths[i] = max(colWidths[i], len(colTypes[i]))
		}
	}

	// Process each record
	for _, record := range records {
		row := make([]string, len(colNames))
		for i, colName := range colNames {
			value := ""
			if val, ok := record[colName]; ok && val != nil {
				value = fmt.Sprintf("%v", val)
			}
			row[i] = value

			// Update column width if this value is wider
			if len(value) > colWidths[i] {
				colWidths[i] = len(value)
			}
		}
		data.rows = append(data.rows, row)
	}

	// Display the table (reusing the same format as Arrow results)
	display.Table(data.colNames, colTypes, data.rows, colWidths)

	// Calculate data size
	dataSize := uint64(len(jsonData))

	return len(data.rows), dataSize, nil
}

// formatSQLError makes SQL errors user-friendly by extracting the meaningful part
func formatSQLError(err error) error {
	if err == nil {
		return nil
	}

	errStr := err.Error()

	// Handle gRPC errors with "rpc error: code = InvalidArgument desc = " prefix
	if strings.Contains(errStr, "rpc error: code = InvalidArgument desc = ") {
		// Extract just the SQL error message
		parts := strings.SplitN(errStr, "rpc error: code = InvalidArgument desc = ", 2)
		if len(parts) == 2 {
			return fmt.Errorf("%s", parts[1])
		}
	}

	// Handle other gRPC error codes
	if strings.Contains(errStr, "rpc error: code = ") {
		// Extract the description part
		if idx := strings.Index(errStr, " desc = "); idx != -1 {
			return fmt.Errorf("%s", errStr[idx+8:])
		}
	}

	return err
}

func displayArrowResults(reader array.RecordReader) (int, uint64, error) {
	if reader == nil {
		return 0, 0, fmt.Errorf("nil reader")
	}

	// Collect all records and their string representations
	type recordData struct {
		colNames []string
		colTypes []string
		rows     [][]string
	}
	var allData recordData
	var colWidths []int
	var totalBytes uint64

	// Read all records and collect data
	for reader.Next() {
		record := reader.Record()

		// Calculate size of this record batch
		for i := 0; i < int(record.NumCols()); i++ {
			col := record.Column(i)
			for _, buf := range col.Data().Buffers() {
				if buf != nil {
					totalBytes += uint64(buf.Len())
				}
			}
		}

		// Initialize column names, types, and widths on first record
		if len(allData.colNames) == 0 {
			numCols := int(record.NumCols())
			allData.colNames = make([]string, numCols)
			allData.colTypes = make([]string, numCols)
			colWidths = make([]int, numCols)

			schema := record.Schema()
			for i := 0; i < numCols; i++ {
				colName := record.ColumnName(i)
				colType := schema.Field(i).Type.String()
				allData.colNames[i] = colName
				allData.colTypes[i] = colType

				// Width should accommodate both name and type
				colWidths[i] = max(len(colName), len(colType))
			}
		}

		// Process each row in the record
		for row := 0; row < int(record.NumRows()); row++ {
			rowValues := make([]string, len(allData.colNames))
			for col := 0; col < int(record.NumCols()); col++ {
				column := record.Column(col)
				value := column.ValueStr(row)
				rowValues[col] = value

				// Update column width if this value is wider
				if len(value) > colWidths[col] {
					colWidths[col] = len(value)
				}
			}
			allData.rows = append(allData.rows, rowValues)
		}
	}

	if err := reader.Err(); err != nil {
		return 0, 0, err
	}

	// No results
	if len(allData.rows) == 0 {
		fmt.Println("No results.")
		return 0, 0, nil
	}

	// Display the table using the shared function
	display.Table(allData.colNames, allData.colTypes, allData.rows, colWidths)

	return len(allData.rows), totalBytes, nil
}

func init() {
	sqlCmd.Flags().String("cache-control", "cache", "Control whether the results cache is used for queries. [possible values: cache, no-cache]")
	sqlCmd.Flags().String("endpoint", "", "Specifies the remote Spice instance endpoint. Supports http://, https://, grpc://, or grpc+tls:// schemes. If not provided, uses local spiced runtime.")
	sqlCmd.Flags().String("flight-endpoint", "", "Specifies the remote Spice instance Flight endpoint (treated as gRPC endpoint). If not provided, uses local spiced runtime.")
	// Must override `--http-endpoint` to provide socket address (i.e. 0.0.0.0:8090), not http endpoint (http://localhost:8090). `spice sql` uses flight endpoint.
	sqlCmd.PersistentFlags().String(constants.HttpEndpointKeyFlag, "0.0.0.0:8090", "HTTP endpoint of Spice")
	RootCmd.AddCommand(sqlCmd)
}
