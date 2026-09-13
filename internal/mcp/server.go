package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/fds1288/reminis/pkg/client"
)

// MCP Protocol and Error Constants
const (
	MCPProtocolVersion = "2024-11-05"
	ServerName         = "reminis"
	ServerVersion      = "0.6.0"

	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// JSON-RPC 2.0 request envelope.
type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// JSON-RPC 2.0 response envelope.
type jsonrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonrpcError   `json:"error,omitempty"`
}

// JSON-RPC 2.0 error representation.
type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// ToolCallContent encapsulates a single content item in a tools/call response.
type ToolCallContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// ToolCallResult represents the structured result of an MCP tools/call invocation.
type ToolCallResult struct {
	Content []ToolCallContent `json:"content"`
	IsError bool              `json:"isError,omitempty"`
}

// Server provides a strict Model Context Protocol (MCP) server over stdio.
// Strict stdout hygiene is enforced: os.Stdout is reserved exclusively for framed
// JSON-RPC 2.0 messages; all diagnostics and errors are routed to errLog (os.Stderr).
type Server struct {
	client *client.Client
	in     io.Reader
	out    io.Writer
	errLog io.Writer
	writeMu sync.Mutex
}

// NewServer creates a new MCP Server with configured streams.
func NewServer(c *client.Client, in io.Reader, out io.Writer, errLog io.Writer) *Server {
	if in == nil {
		in = os.Stdin
	}
	if out == nil {
		out = os.Stdout
	}
	if errLog == nil {
		errLog = os.Stderr
	}

	return &Server{
		client: c,
		in:     in,
		out:    out,
		errLog: errLog,
	}
}

// Serve starts reading JSON-RPC 2.0 messages from the input stream and responding on stdout.
func (s *Server) Serve(ctx context.Context) error {
	reader := bufio.NewReader(s.in)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				if len(line) > 0 {
					s.handleMessage(ctx, line)
				}
				return nil
			}
			return fmt.Errorf("error reading mcp stdio: %w", err)
		}

		if len(line) == 0 {
			continue
		}

		s.handleMessage(ctx, line)
	}
}

func (s *Server) handleMessage(ctx context.Context, data []byte) {
	var req jsonrpcRequest
	if err := json.Unmarshal(data, &req); err != nil {
		s.logError("Failed to parse JSON-RPC request: %v", err)
		_ = s.sendError(json.RawMessage("null"), CodeParseError, "Parse error", nil)
		return
	}

	// Notifications (requests without an ID) do not send responses
	isNotification := len(req.ID) == 0 || string(req.ID) == "null"

	switch req.Method {
	case "initialize":
		var initParams struct {
			ProtocolVersion string         `json:"protocolVersion"`
			Capabilities    map[string]any `json:"capabilities"`
			ClientInfo      map[string]any `json:"clientInfo"`
		}
		_ = json.Unmarshal(req.Params, &initParams)

		protoVersion := MCPProtocolVersion
		if initParams.ProtocolVersion != "" {
			protoVersion = initParams.ProtocolVersion
		}

		result := map[string]any{
			"protocolVersion": protoVersion,
			"capabilities": map[string]any{
				"tools": map[string]any{},
			},
			"serverInfo": map[string]any{
				"name":    ServerName,
				"version": ServerVersion,
			},
		}
		if !isNotification {
			_ = s.sendResult(req.ID, result)
		}

	case "ping":
		if !isNotification {
			_ = s.sendResult(req.ID, map[string]any{})
		}

	case "notifications/initialized":
		// Standard MCP notification confirming host client initialization
		s.logDebug("Client confirmed initialization")

	case "tools/list":
		tools := GetToolDefinitions()
		result := map[string]any{
			"tools": tools,
		}
		if !isNotification {
			_ = s.sendResult(req.ID, result)
		}

	case "tools/call":
		var callParams struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &callParams); err != nil {
			if !isNotification {
				_ = s.sendError(req.ID, CodeInvalidParams, "Invalid params for tools/call", err.Error())
			}
			return
		}

		output, err := CallTool(ctx, s.client, callParams.Name, callParams.Arguments)
		if err != nil {
			s.logError("Tool call %q failed: %v", callParams.Name, err)
			if !isNotification {
				toolResult := ToolCallResult{
					Content: []ToolCallContent{
						{
							Type: "text",
							Text: fmt.Sprintf("Error executing tool %s: %v", callParams.Name, err),
						},
					},
					IsError: true,
				}
				_ = s.sendResult(req.ID, toolResult)
			}
			return
		}

		if !isNotification {
			toolResult := ToolCallResult{
				Content: []ToolCallContent{
					{
						Type: "text",
						Text: output,
					},
				},
				IsError: false,
			}
			_ = s.sendResult(req.ID, toolResult)
		}

	default:
		if !isNotification {
			_ = s.sendError(req.ID, CodeMethodNotFound, fmt.Sprintf("Method %q not found", req.Method), nil)
		}
	}
}

func (s *Server) sendResult(id json.RawMessage, result any) error {
	resp := jsonrpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	}
	return s.writeResponse(resp)
}

func (s *Server) sendError(id json.RawMessage, code int, message string, data any) error {
	resp := jsonrpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &jsonrpcError{
			Code:    code,
			Message: message,
			Data:    data,
		},
	}
	return s.writeResponse(resp)
}

func (s *Server) writeResponse(resp jsonrpcResponse) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = s.out.Write(data)
	return err
}

func (s *Server) logError(format string, args ...any) {
	if s.errLog != nil {
		_, _ = fmt.Fprintf(s.errLog, "[reminis-mcp ERROR] "+format+"\n", args...)
	}
}

func (s *Server) logDebug(format string, args ...any) {
	if s.errLog != nil {
		_, _ = fmt.Fprintf(s.errLog, "[reminis-mcp DEBUG] "+format+"\n", args...)
	}
}
