/*
Copyright 2026 The Spice.ai OSS Authors

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
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	rtcontext "github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/display"
	"github.com/spiceai/spiceai/bin/spice/pkg/history"
)

const (
	// MaxDisplayRows is the maximum number of rows to display in results
	MaxDisplayRows = 500
	// QueryHistoryType is the history type for query REPL
	QueryHistoryType history.HistoryType = "query_history.txt"
)

// spinnerFrames contains the braille spinner characters for the progress indicator
var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// TrackedQuery represents a query being tracked in the REPL session
type TrackedQuery struct {
	QueryID     string
	SQL         string
	Status      string
	SubmittedAt time.Time
	CompletedAt *time.Time
}

// QueryREPL handles the interactive query REPL state
type QueryREPL struct {
	client         *api.QueriesClient
	liner          *liner.State
	trackedQueries map[string]*TrackedQuery
	historyMgr     *history.Manager
}

var queryCmd = &cobra.Command{
	Use:   "query [sql]",
	Short: "Submit an async query or start an interactive async query REPL",
	Long: `Submit an async SQL query or start an interactive REPL for managing async queries via the /v1/queries API.

Queries are submitted asynchronously and the CLI auto-polls for completion. Press Ctrl+C to stop
waiting for a query (the query continues running in the background).

Async queries require cluster mode with scheduler.state_location configured.`,
	Example: `
# Submit a single query and wait for results
$ spice query "SELECT * FROM my_table"

# Submit a query without waiting
$ spice query --no-wait "SELECT * FROM my_table"

# Start interactive REPL
$ spice query

# List all queries
$ spice query list

# Check status of a specific query
$ spice query status qry_01ABC123

# Get results of a completed query
$ spice query results qry_01ABC123

# Cancel a running query
$ spice query cancel qry_01ABC123
`,
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, err := rtcontext.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			os.Exit(1)
		}

		// If SQL argument provided, submit directly
		if len(args) == 1 {
			noWait, _ := cmd.Flags().GetBool("no-wait")
			timeout, _ := cmd.Flags().GetDuration("timeout")

			client := api.NewQueriesClient(ctx)
			sql := args[0]

			if err := submitAndWait(client, sql, !noWait, timeout); err != nil {
				slog.Error("submitting query", "error", err)
				os.Exit(1)
			}
			return
		}

		// No argument, start REPL
		if err := runQueryREPL(ctx); err != nil {
			slog.Error("running query REPL", "error", err)
			os.Exit(1)
		}
	},
}

var queryListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all queries",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, err := rtcontext.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			os.Exit(1)
		}

		status, _ := cmd.Flags().GetString("status")
		limit, _ := cmd.Flags().GetInt("limit")

		client := api.NewQueriesClient(ctx)

		resp, err := client.List(context.Background(), status, limit)
		if err != nil {
			slog.Error("listing queries", "error", err)
			os.Exit(1)
		}

		displayQueryList(resp.Queries)
	},
}

var queryStatusCmd = &cobra.Command{
	Use:   "status <query_id>",
	Short: "Check the status of a query",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, err := rtcontext.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			os.Exit(1)
		}

		client := api.NewQueriesClient(ctx)
		queryID := args[0]

		info, err := client.GetQuery(context.Background(), queryID)
		if err != nil {
			slog.Error("getting query status", "error", err)
			os.Exit(1)
		}

		displayQueryInfo(info)
	},
}

var queryResultsCmd = &cobra.Command{
	Use:   "results <query_id>",
	Short: "Fetch and display results of a completed query",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, err := rtcontext.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			os.Exit(1)
		}

		client := api.NewQueriesClient(ctx)
		queryID := args[0]

		if err := displayResults(client, queryID); err != nil {
			slog.Error("getting query results", "error", err)
			os.Exit(1)
		}
	},
}

var queryCancelCmd = &cobra.Command{
	Use:   "cancel <query_id>",
	Short: "Cancel a running query",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, err := rtcontext.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			os.Exit(1)
		}

		client := api.NewQueriesClient(ctx)
		queryID := args[0]

		info, err := client.Cancel(context.Background(), queryID)
		if err != nil {
			slog.Error("cancelling query", "error", err)
			os.Exit(1)
		}

		fmt.Printf("Query %s cancelled (status: %s)\n", info.QueryID, info.Status.State)
	},
}

func runQueryREPL(ctx *rtcontext.RuntimeContext) error {
	fmt.Println("Welcome to the Spice.ai async query REPL.")
	fmt.Println("Type SQL to submit a query, or .help for commands.")
	fmt.Println()

	// Initialize history manager
	historyMgr, err := history.NewManager(QueryHistoryType)
	if err != nil {
		slog.Warn("failed to initialize query history", "error", err)
		historyMgr = nil
	}

	// Setup liner for REPL
	line := liner.NewLiner()
	line.SetCtrlCAborts(true)
	defer func() {
		if historyMgr != nil {
			if err := historyMgr.Save(); err != nil {
				slog.Warn("failed to save query history", "error", err)
			}
		}
		if err := line.Close(); err != nil {
			slog.Error("closing liner", "error", err)
		}
	}()

	// Load history into liner
	if historyMgr != nil {
		historyMgr.LoadIntoLiner(line)
		line.SetCompleter(historyMgr.GetCompleter())
	}

	client := api.NewQueriesClient(ctx)

	repl := &QueryREPL{
		client:         client,
		liner:          line,
		trackedQueries: make(map[string]*TrackedQuery),
		historyMgr:     historyMgr,
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	var queryMutex sync.Mutex
	var cancelPoll context.CancelFunc
	var isPolling bool
	var exitRequested bool

	// Helper to safely cancel any active poll context
	cleanupPoll := func() {
		queryMutex.Lock()
		if cancelPoll != nil {
			cancelPoll()
			cancelPoll = nil
		}
		queryMutex.Unlock()
	}
	defer cleanupPoll()

	// Handle signals in a goroutine
	go func() {
		for range sigChan {
			queryMutex.Lock()
			if isPolling && cancelPoll != nil {
				// Cancel the polling
				cancelPoll()
				exitRequested = false
			} else {
				if exitRequested {
					fmt.Println("\nExiting...")
					if historyMgr != nil {
						_ = historyMgr.Save()
					}
					os.Exit(0)
				}
				exitRequested = true
				fmt.Println("\nPress Ctrl+C again to exit, or continue entering commands.")
			}
			queryMutex.Unlock()
		}
	}()

	for {
		queryMutex.Lock()
		shouldExit := exitRequested
		queryMutex.Unlock()

		if shouldExit {
			fmt.Println()
			return nil
		}

		// Read input
		inputStr, err := readQueryInput(line, "query> ")
		if err == io.EOF {
			fmt.Println()
			return nil
		} else if err != nil {
			slog.Error("reading line", "error", err)
			continue
		}

		if inputStr == "" {
			continue
		}

		// Reset exit request on valid input
		queryMutex.Lock()
		exitRequested = false
		queryMutex.Unlock()

		// Handle special commands
		if strings.HasPrefix(inputStr, ".") {
			if repl.handleSpecialCommand(inputStr) {
				continue
			}
			// Exit requested
			return nil
		}

		// Handle regular commands
		lowerInput := strings.ToLower(inputStr)
		if lowerInput == "exit" || lowerInput == "quit" || lowerInput == "q" {
			return nil
		}

		if lowerInput == "help" {
			printQueryHelp()
			continue
		}

		// Add to history
		line.AppendHistory(inputStr)
		if historyMgr != nil {
			historyMgr.Add(inputStr)
			if err := historyMgr.Save(); err != nil {
				slog.Warn("failed to save history", "error", err)
			}
		}

		// Submit query
		req := &api.SubmitRequest{SQL: inputStr}
		resp, err := client.Submit(context.Background(), req)
		if err != nil {
			fmt.Printf("\033[31mError:\033[0m %v\n", err)
			continue
		}

		// Track the query
		repl.trackedQueries[resp.QueryID] = &TrackedQuery{
			QueryID:     resp.QueryID,
			SQL:         inputStr,
			Status:      resp.Status.State,
			SubmittedAt: time.Now(),
		}

		fmt.Printf("Submitted query: %s (%s)\n", resp.QueryID, resp.Status.State)
		fmt.Println("Press Ctrl+C to stop waiting (query continues in background)")

		// Create cancellable context for polling
		var pollCtx context.Context
		queryMutex.Lock()
		pollCtx, cancelPoll = context.WithCancel(context.Background())
		isPolling = true
		queryMutex.Unlock()

		// Poll for completion
		finalStatus, wasCancelled, elapsed := pollForCompletion(pollCtx, client, resp.QueryID)

		queryMutex.Lock()
		isPolling = false
		cancelPoll = nil // Clear to avoid double-cancel in cleanup
		queryMutex.Unlock()

		if wasCancelled {
			fmt.Printf("\nStopped waiting. Check status with: .status %s\n", resp.QueryID)
			fmt.Printf("Wait for completion with: .wait %s\n", resp.QueryID)
			continue
		}

		// Update tracked query
		if tracked, ok := repl.trackedQueries[resp.QueryID]; ok {
			tracked.Status = finalStatus.State
			now := time.Now()
			tracked.CompletedAt = &now
		}

		// Handle final status
		if finalStatus.IsSuccess() {
			if err := displayResultsWithTiming(client, resp.QueryID, elapsed); err != nil {
				fmt.Printf("\033[31mError displaying results:\033[0m %v\n", err)
			}
		} else if finalStatus.IsFailed() {
			fmt.Printf("\033[31m✗ FAILED\033[0m\n")
			if finalStatus.Error != nil {
				fmt.Printf("Error: %s\n", finalStatus.Error.Message)
			}
		} else if finalStatus.IsCancelled() {
			fmt.Printf("\033[33m⊘ CANCELLED\033[0m\n")
		} else {
			fmt.Printf("Query ended with status: %s\n", finalStatus.State)
		}
	}
}

// handleSpecialCommand processes dot commands and returns true to continue REPL, false to exit
func (r *QueryREPL) handleSpecialCommand(cmd string) bool {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return true
	}

	command := strings.ToLower(parts[0])
	args := parts[1:]

	switch command {
	case ".exit", ".quit", ".q":
		return false

	case ".help":
		printQueryHelp()

	case ".list":
		// List all tracked queries
		r.listTrackedQueries()

	case ".status":
		if len(args) == 0 {
			fmt.Println("Usage: .status <query_id>")
			return true
		}
		// Show status of specific query
		queryID := r.resolveQueryID(args[0])
		if queryID == "" {
			return true
		}
		info, err := r.client.GetQuery(context.Background(), queryID)
		if err != nil {
			fmt.Printf("\033[31mError:\033[0m %v\n", err)
			return true
		}
		displayQueryInfo(info)

	case ".results":
		if len(args) == 0 {
			fmt.Println("Usage: .results <query_id>")
			return true
		}
		queryID := r.resolveQueryID(args[0])
		if queryID == "" {
			return true
		}
		if err := displayResults(r.client, queryID); err != nil {
			fmt.Printf("\033[31mError:\033[0m %v\n", err)
		}

	case ".wait":
		if len(args) == 0 {
			fmt.Println("Usage: .wait <query_id>")
			return true
		}
		queryID := r.resolveQueryID(args[0])
		if queryID == "" {
			return true
		}
		r.waitForQuery(queryID)

	case ".cancel":
		if len(args) == 0 {
			fmt.Println("Usage: .cancel <query_id>")
			return true
		}
		queryID := r.resolveQueryID(args[0])
		if queryID == "" {
			return true
		}
		info, err := r.client.Cancel(context.Background(), queryID)
		if err != nil {
			fmt.Printf("\033[31mError:\033[0m %v\n", err)
			return true
		}
		fmt.Printf("Query %s cancelled (status: %s)\n", info.QueryID, info.Status.State)
		if tracked, ok := r.trackedQueries[queryID]; ok {
			tracked.Status = info.Status.State
		}

	case ".clear":
		if len(args) > 0 && strings.ToLower(args[0]) == "history" {
			r.liner.ClearHistory()
			if r.historyMgr != nil {
				r.historyMgr.Clear()
				if err := r.historyMgr.Save(); err != nil {
					fmt.Printf("\033[31mError:\033[0m Failed to clear history: %v\n", err)
				} else {
					fmt.Println("Query history cleared.")
				}
			}
		} else {
			// Clear tracked queries
			r.trackedQueries = make(map[string]*TrackedQuery)
			fmt.Println("Tracked queries cleared.")
		}

	default:
		fmt.Printf("Unknown command: %s. Type .help for available commands.\n", command)
	}

	return true
}

// resolveQueryID resolves a partial query ID to a full query ID
func (r *QueryREPL) resolveQueryID(partial string) string {
	// First, try exact match in tracked queries
	if _, ok := r.trackedQueries[partial]; ok {
		return partial
	}

	// Try partial match
	var matches []string
	for id := range r.trackedQueries {
		if strings.HasPrefix(id, partial) || strings.Contains(id, partial) {
			matches = append(matches, id)
		}
	}

	if len(matches) == 1 {
		return matches[0]
	} else if len(matches) > 1 {
		fmt.Printf("Multiple queries match '%s': %s. Please be more specific.\n", partial, strings.Join(matches, ", "))
		return ""
	}

	// No match in tracked queries, use the ID as-is (let the API handle it)
	return partial
}

// listTrackedQueries displays all tracked queries
func (r *QueryREPL) listTrackedQueries() {
	if len(r.trackedQueries) == 0 {
		fmt.Println("No tracked queries. Submit a query to start tracking.")
		return
	}

	// Prepare table data
	colNames := []string{"QUERY ID", "STATUS", "SUBMITTED", "SQL"}
	colWidths := []int{20, 12, 15, 40}
	var rows [][]string

	for _, q := range r.trackedQueries {
		ago := humanize.Time(q.SubmittedAt)
		sql := q.SQL
		if len(sql) > 37 {
			sql = sql[:37] + "..."
		}
		rows = append(rows, []string{q.QueryID, q.Status, ago, sql})
	}

	display.Table(colNames, nil, rows, colWidths)
}

// waitForQuery waits for a query to complete
func (r *QueryREPL) waitForQuery(queryID string) {
	fmt.Println("Press Ctrl+C to stop waiting (query continues in background)")

	ctx, cancel := context.WithCancel(context.Background())

	// Handle Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		cancel()
	}()
	defer signal.Stop(sigChan)

	finalStatus, wasCancelled, elapsed := pollForCompletion(ctx, r.client, queryID)
	cancel()

	if wasCancelled {
		fmt.Printf("\nStopped waiting. Check status with: .status %s\n", queryID)
		return
	}

	// Update tracked query if exists
	if tracked, ok := r.trackedQueries[queryID]; ok {
		tracked.Status = finalStatus.State
		now := time.Now()
		tracked.CompletedAt = &now
	}

	if finalStatus.IsSuccess() {
		if err := displayResultsWithTiming(r.client, queryID, elapsed); err != nil {
			fmt.Printf("\033[31mError displaying results:\033[0m %v\n", err)
		}
	} else if finalStatus.IsFailed() {
		fmt.Printf("\033[31m✗ FAILED\033[0m\n")
		if finalStatus.Error != nil {
			fmt.Printf("Error: %s\n", finalStatus.Error.Message)
		}
	} else if finalStatus.IsCancelled() {
		fmt.Printf("\033[33m⊘ CANCELLED\033[0m\n")
	}
}

// pollForCompletion polls for query completion with a spinner
// Returns the final status, whether polling was cancelled, and the elapsed time
func pollForCompletion(ctx context.Context, client *api.QueriesClient, queryID string) (*api.QueryStatus, bool, time.Duration) {
	ticker := time.NewTicker(api.PollInterval)
	defer ticker.Stop()

	spinnerIdx := 0
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			// Clear spinner line
			fmt.Print("\r\033[K")
			return nil, true, time.Since(startTime)

		case <-ticker.C:
			status, err := client.GetStatus(ctx, queryID)
			if err != nil {
				// If context was cancelled, treat as cancelled
				if ctx.Err() != nil {
					fmt.Print("\r\033[K")
					return nil, true, time.Since(startTime)
				}
				// Otherwise, continue polling (might be transient error)
				continue
			}

			elapsed := time.Since(startTime)

			if status.IsTerminal() {
				// Clear spinner and show final status
				fmt.Print("\r\033[K")
				if status.IsSuccess() {
					fmt.Printf("\033[32m✓ SUCCEEDED\033[0m (%.1fs)\n", elapsed.Seconds())
				}
				return status, false, elapsed
			}

			// Update spinner
			frame := spinnerFrames[spinnerIdx%len(spinnerFrames)]
			spinnerIdx++
			fmt.Printf("\r%s %s (%.1fs)...", frame, status.State, elapsed.Seconds())
		}
	}
}

// submitAndWait submits a query and optionally waits for completion
func submitAndWait(client *api.QueriesClient, sql string, wait bool, timeout time.Duration) error {
	req := &api.SubmitRequest{SQL: sql}
	resp, err := client.Submit(context.Background(), req)
	if err != nil {
		return err
	}

	fmt.Printf("Submitted query: %s (%s)\n", resp.QueryID, resp.Status.State)

	if !wait {
		fmt.Printf("Status URL: %s\n", resp.StatusURL)
		fmt.Printf("Results URL: %s\n", resp.ResultsURL)
		return nil
	}

	fmt.Println("Waiting for completion... (Ctrl+C to stop waiting)")

	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Handle Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-sigChan
		cancel()
	}()
	defer signal.Stop(sigChan)

	finalStatus, wasCancelled, elapsed := pollForCompletion(ctx, client, resp.QueryID)

	if wasCancelled {
		fmt.Printf("\nStopped waiting. Query ID: %s\n", resp.QueryID)
		return nil
	}

	if finalStatus.IsSuccess() {
		return displayResultsWithTiming(client, resp.QueryID, elapsed)
	} else if finalStatus.IsFailed() {
		if finalStatus.Error != nil {
			return fmt.Errorf("query failed: %s", finalStatus.Error.Message)
		}
		return fmt.Errorf("query failed")
	} else if finalStatus.IsCancelled() {
		fmt.Println("Query was cancelled.")
	}

	return nil
}

// displayResults fetches and displays query results
func displayResults(client *api.QueriesClient, queryID string) error {
	return displayResultsWithTiming(client, queryID, 0)
}

// displayResultsWithTiming fetches and displays query results with optional timing info
func displayResultsWithTiming(client *api.QueriesClient, queryID string, elapsed time.Duration) error {
	// First get query info to check status and get manifest
	info, err := client.GetQuery(context.Background(), queryID)
	if err != nil {
		return err
	}

	if !info.Status.IsSuccess() {
		return fmt.Errorf("query '%s' is still %s. Use .wait %s to wait for completion", queryID, info.Status.State, queryID)
	}

	if info.Manifest == nil {
		return fmt.Errorf("no result manifest available")
	}

	// Collect all rows up to MaxDisplayRows
	var allRows []map[string]interface{}
	totalRows := info.Manifest.TotalRowCount
	chunkIndex := 0

	for len(allRows) < MaxDisplayRows && chunkIndex < info.Manifest.TotalChunkCount {
		chunk, err := client.GetResults(context.Background(), queryID, chunkIndex)
		if err != nil {
			return fmt.Errorf("getting chunk %d: %w", chunkIndex, err)
		}

		for _, row := range chunk.DataArray {
			if len(allRows) >= MaxDisplayRows {
				break
			}
			allRows = append(allRows, row)
		}

		if chunk.NextChunkIndex == nil {
			break
		}
		chunkIndex = *chunk.NextChunkIndex
	}

	if len(allRows) == 0 {
		if elapsed > 0 {
			fmt.Printf("Time: %.8f seconds. 0 rows.\n", elapsed.Seconds())
		} else {
			fmt.Println("No results.")
		}
		return nil
	}

	// Extract column info from manifest
	colNames := make([]string, len(info.Manifest.Schema.Columns))
	colTypes := make([]string, len(info.Manifest.Schema.Columns))
	colWidths := make([]int, len(info.Manifest.Schema.Columns))

	for _, col := range info.Manifest.Schema.Columns {
		colNames[col.Position] = col.Name
		colTypes[col.Position] = col.TypeName
		colWidths[col.Position] = max(len(col.Name), len(col.TypeName))
	}

	// Convert rows to string arrays and calculate widths
	var rows [][]string
	for _, row := range allRows {
		rowValues := make([]string, len(colNames))
		for i, colName := range colNames {
			if val, ok := row[colName]; ok && val != nil {
				rowValues[i] = fmt.Sprintf("%v", val)
			}
			if len(rowValues[i]) > colWidths[i] {
				colWidths[i] = len(rowValues[i])
			}
		}
		rows = append(rows, rowValues)
	}

	// Display table
	display.Table(colNames, colTypes, rows, colWidths)

	// Show timing and row count (matching spice sql format)
	if elapsed > 0 {
		fmt.Printf("\nTime: %.8f seconds. %d rows.\n", elapsed.Seconds(), totalRows)
	} else if len(allRows) < totalRows {
		fmt.Printf("\nShowing %d/%d rows\n", len(allRows), totalRows)
	} else {
		fmt.Printf("\n%d row(s)\n", len(allRows))
	}

	return nil
}

// displayQueryList displays a list of queries
func displayQueryList(queries []api.QuerySummary) {
	if len(queries) == 0 {
		fmt.Println("No queries found.")
		return
	}

	colNames := []string{"QUERY ID", "STATE", "CREATED", "SQL PREVIEW"}
	colWidths := []int{20, 12, 25, 50}
	var rows [][]string

	for _, q := range queries {
		sql := q.SQLPreview
		if len(sql) > 47 {
			sql = sql[:47] + "..."
		}
		rows = append(rows, []string{q.QueryID, q.State, q.CreatedAt, sql})
	}

	display.Table(colNames, nil, rows, colWidths)
	fmt.Printf("\nTotal: %d queries\n", len(queries))
}

// displayQueryInfo displays detailed query information
func displayQueryInfo(info *api.QueryInfo) {
	fmt.Printf("Query ID:    %s\n", info.QueryID)
	fmt.Printf("Status:      %s\n", info.Status.State)
	fmt.Printf("Created:     %s\n", info.CreatedAt)
	if info.StartedAt != "" {
		fmt.Printf("Started:     %s\n", info.StartedAt)
	}
	if info.CompletedAt != "" {
		fmt.Printf("Completed:   %s\n", info.CompletedAt)
	}
	if info.ExpiresAt != "" {
		fmt.Printf("Expires:     %s\n", info.ExpiresAt)
	}
	if info.Status.Error != nil {
		fmt.Printf("Error:       %s\n", info.Status.Error.Message)
	}
	if info.Manifest != nil {
		fmt.Printf("Rows:        %d\n", info.Manifest.TotalRowCount)
		fmt.Printf("Chunks:      %d\n", info.Manifest.TotalChunkCount)
	}
}

// readQueryInput reads multi-line SQL input until semicolon or special command
func readQueryInput(line *liner.State, prompt string) (string, error) {
	var query strings.Builder
	firstLine := true
	currentPrompt := prompt

	for {
		inputLine, err := line.Prompt(currentPrompt)

		if err == liner.ErrPromptAborted {
			if query.Len() == 0 && firstLine {
				return "", io.EOF
			}
			return "", nil
		} else if err == io.EOF {
			if query.Len() == 0 {
				return "", io.EOF
			}
			return "", nil
		} else if err != nil {
			return "", fmt.Errorf("reading line: %w", err)
		}

		if query.Len() > 0 {
			query.WriteString("\n")
		}
		query.WriteString(inputLine)

		trimmedQuery := strings.TrimSpace(query.String())
		lowerQuery := strings.ToLower(trimmedQuery)

		// Check for special commands on first line
		if firstLine && (lowerQuery == "help" || lowerQuery == "exit" || lowerQuery == "quit" || lowerQuery == "q" || strings.HasPrefix(lowerQuery, ".")) {
			break
		}

		// Check if query ends with semicolon
		if strings.HasSuffix(trimmedQuery, ";") {
			break
		}

		firstLine = false
		currentPrompt = "     > "
	}

	return strings.TrimSpace(query.String()), nil
}

func printQueryHelp() {
	fmt.Println("Available commands:")
	fmt.Println()
	fmt.Println("  SQL statements    - Submit a query (end with semicolon)")
	fmt.Println()
	fmt.Println("Special commands:")
	fmt.Println("  .list             - List all tracked queries")
	fmt.Println("  .status <id>      - Show detailed status of a specific query")
	fmt.Println("  .results <id>     - Fetch and display results of a completed query")
	fmt.Println("  .wait <id>        - Resume waiting for a query to complete")
	fmt.Println("  .cancel <id>      - Cancel a running query")
	fmt.Println("  .clear            - Clear tracked queries from local list")
	fmt.Println("  .clear history    - Clear command history")
	fmt.Println("  .help             - Show this help message")
	fmt.Println("  .exit, .quit, .q  - Exit the REPL")
	fmt.Println()
	fmt.Println("Tips:")
	fmt.Println("  - Partial query IDs work if they uniquely identify a query")
	fmt.Println("  - Press Ctrl+C while waiting to stop (query continues in background)")
	fmt.Println("  - Press Ctrl+D or type .exit to quit")
	fmt.Println()
}

func init() {
	// Query command flags (for direct submission)
	queryCmd.Flags().Bool("no-wait", false, "Submit and return immediately without waiting for results")
	queryCmd.Flags().Duration("timeout", 0, "Maximum time to wait for query completion (0 = no timeout)")

	// Query list subcommand
	queryListCmd.Flags().String("status", "", "Filter by status (pending, running, succeeded, failed, cancelled)")
	queryListCmd.Flags().Int("limit", 100, "Maximum number of queries to return")

	// Add subcommands
	queryCmd.AddCommand(queryListCmd)
	queryCmd.AddCommand(queryStatusCmd)
	queryCmd.AddCommand(queryResultsCmd)
	queryCmd.AddCommand(queryCancelCmd)

	// Add to root
	RootCmd.AddCommand(queryCmd)
}
