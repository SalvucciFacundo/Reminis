package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/pkg/client"
)

type mcpMockPlanner struct {
	tasks []dag.Task
}

func (p *mcpMockPlanner) Plan(ctx context.Context, goal, manifest, rulesContent string) ([]dag.Task, error) {
	return p.tasks, nil
}

func setupTestMCP(t *testing.T) (*client.Client, string) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "mcp_test.db")

	c, err := client.New(
		client.WithDBPath(dbPath),
		client.WithWorkspaceDir(tempDir),
		client.WithPlanner(&mcpMockPlanner{
			tasks: []dag.Task{
				{
					ID:     "mcp_task_1",
					Action: "MCP Test Task",
				},
			},
		}),
		client.WithTaskHandler(func(ctx context.Context, task *dag.Task) (json.RawMessage, []dag.Task, error) {
			return json.RawMessage(`{"mcp_test": true}`), nil, nil
		}),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return c, tempDir
}

func TestMCPServerProtocolAndStdoutHygiene(t *testing.T) {
	c, _ := setupTestMCP(t)
	defer c.Close()

	var inBuf bytes.Buffer
	var outBuf bytes.Buffer
	var errBuf bytes.Buffer

	server := NewServer(c, &inBuf, &outBuf, &errBuf)

	// Build a sequence of JSON-RPC requests
	requests := []string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`,
		`{"jsonrpc":"2.0","id":2,"method":"ping"}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"reminis_facts_save","arguments":{"topic":"architecture","content":"Clean Hexagonal Architecture","scope":"project"}}}`,
		`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"reminis_facts_search","arguments":{"topic":"architecture"}}}`,
		`{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"reminis_prefix_show","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"reminis_run","arguments":{"goal":"Run test goal"}}}`,
	}

	for _, r := range requests {
		inBuf.WriteString(r + "\n")
	}

	ctx := context.Background()
	if err := server.Serve(ctx); err != nil {
		t.Fatalf("server.Serve error: %v", err)
	}

	// Verify stdout hygiene: EVERY line must be valid JSON and unmarshal into jsonrpcResponse
	lines := strings.Split(strings.TrimSpace(outBuf.String()), "\n")
	if len(lines) == 0 || (len(lines) == 1 && lines[0] == "") {
		t.Fatalf("expected stdout responses, got none")
	}

	// We sent 7 requests that require responses (1 notification sends no response)
	expectedResponses := 7
	if len(lines) != expectedResponses {
		t.Fatalf("expected %d response lines, got %d. Output:\n%s", expectedResponses, len(lines), outBuf.String())
	}

	for i, line := range lines {
		var resp jsonrpcResponse
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			t.Fatalf("stdout corruption on line %d: %v. Line: %q", i+1, err, line)
		}
		if resp.JSONRPC != "2.0" {
			t.Errorf("line %d: expected jsonrpc 2.0, got %s", i+1, resp.JSONRPC)
		}
		if resp.Error != nil {
			t.Errorf("line %d returned error: code=%d message=%s", i+1, resp.Error.Code, resp.Error.Message)
		}
	}

	// Inspect specific responses:
	// Line 0: initialize
	var initResp jsonrpcResponse
	_ = json.Unmarshal([]byte(lines[0]), &initResp)
	initMap := initResp.Result.(map[string]any)
	if initMap["protocolVersion"] != "2024-11-05" {
		t.Errorf("expected protocolVersion 2024-11-05, got %v", initMap["protocolVersion"])
	}
	serverInfo := initMap["serverInfo"].(map[string]any)
	if serverInfo["name"] != "reminis" {
		t.Errorf("expected server name reminis, got %v", serverInfo["name"])
	}

	// Line 2: tools/list (after ping)
	var toolsResp jsonrpcResponse
	_ = json.Unmarshal([]byte(lines[2]), &toolsResp)
	toolsMap := toolsResp.Result.(map[string]any)
	toolsList := toolsMap["tools"].([]any)
	if len(toolsList) < 6 {
		t.Errorf("expected at least 6 tools, got %d", len(toolsList))
	}

	// Line 4: facts_search
	var searchResp jsonrpcResponse
	_ = json.Unmarshal([]byte(lines[4]), &searchResp)
	searchMap := searchResp.Result.(map[string]any)
	contentList := searchMap["content"].([]any)
	if len(contentList) == 0 {
		t.Fatalf("expected content in facts_search response")
	}
	contentText := contentList[0].(map[string]any)["text"].(string)
	if !strings.Contains(contentText, "Clean Hexagonal Architecture") {
		t.Errorf("expected fact in search result, got: %s", contentText)
	}

	// Line 5: prefix_show
	var prefixResp jsonrpcResponse
	_ = json.Unmarshal([]byte(lines[5]), &prefixResp)
	prefixMap := prefixResp.Result.(map[string]any)
	prefixContent := prefixMap["content"].([]any)[0].(map[string]any)["text"].(string)
	if !strings.Contains(prefixContent, "hash") {
		t.Errorf("expected hash in prefix show, got: %s", prefixContent)
	}
}

func TestMCPApproveAndResume(t *testing.T) {
	c, _ := setupTestMCP(t)
	defer c.Close()

	// 1. Manually create a run that requires approval
	tasks := []dag.Task{
		{
			ID:               "appr_task_1",
			Action:           "Dangerous action",
			RequiresApproval: true,
		},
	}
	tempDir := t.TempDir()
	cWithAppr, err := client.New(
		client.WithDBPath(filepath.Join(tempDir, "appr.db")),
		client.WithPlanner(&mcpMockPlanner{tasks: tasks}),
		client.WithTaskHandler(func(ctx context.Context, task *dag.Task) (json.RawMessage, []dag.Task, error) {
			return json.RawMessage(`{"done": true}`), nil, nil
		}),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cWithAppr.Close()

	resp, err := cWithAppr.Run(context.Background(), client.RunRequest{
		Goal: "Test approval",
	})
	if err != nil {
		t.Fatalf("initial run failed: %v", err)
	}
	if resp.Status != string(dag.RunStatusWaitingApproval) {
		t.Fatalf("expected WAITING_APPROVAL, got %s", resp.Status)
	}

	// 2. Call reminis_approve via MCP server
	var inBuf, outBuf, errBuf bytes.Buffer
	server := NewServer(cWithAppr, &inBuf, &outBuf, &errBuf)

	approveCall := map[string]any{
		"jsonrpc": "2.0",
		"id":      10,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "reminis_approve",
			"arguments": map[string]any{
				"run_id":  resp.RunID,
				"task_id": "appr_task_1",
				"scope":   "action",
			},
		},
	}
	approveBytes, _ := json.Marshal(approveCall)
	inBuf.Write(approveBytes)
	inBuf.WriteByte('\n')

	if err := server.Serve(context.Background()); err != nil {
		t.Fatalf("Serve error: %v", err)
	}

	var jsonResp jsonrpcResponse
	if err := json.Unmarshal(outBuf.Bytes(), &jsonResp); err != nil {
		t.Fatalf("failed to unmarshal approve response: %v", err)
	}
	if jsonResp.Error != nil {
		t.Fatalf("approve tool error: %v", jsonResp.Error)
	}

	// Verify run in DB is now COMPLETED
	runRec, err := cWithAppr.Store().GetRun(context.Background(), resp.RunID)
	if err != nil {
		t.Fatalf("failed to get run from store: %v", err)
	}
	if runRec.Status != string(dag.RunStatusCompleted) {
		t.Errorf("expected run status COMPLETED after MCP approval, got %s", runRec.Status)
	}
}
