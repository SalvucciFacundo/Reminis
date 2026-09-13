package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fds1288/reminis/internal/prompt"
)

var (
	// ErrMaxRetriesExceeded is returned when all retry attempts fail.
	ErrMaxRetriesExceeded = errors.New("max retries exceeded for LLM completion")
	// ErrNonRetryableClient is returned when an unrecoverable 4xx error is received.
	ErrNonRetryableClient = errors.New("non-retryable client error from LLM provider")
)

// Tool represents an OpenAI tool declaration.
type Tool struct {
	Type     string              `json:"type"`
	Function FunctionDeclaration `json:"function"`
}

// FunctionDeclaration defines a callable function tool.
type FunctionDeclaration struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

// ChatCompletionRequest is the payload for /v1/chat/completions.
type ChatCompletionRequest struct {
	Model       string               `json:"model"`
	Messages    []prompt.ChatMessage `json:"messages"`
	Tools       []Tool               `json:"tools,omitempty"`
	ToolChoice  any                  `json:"tool_choice,omitempty"`
	Temperature *float64             `json:"temperature,omitempty"`
}

// ChatCompletionResponse is the response from /v1/chat/completions.
type ChatCompletionResponse struct {
	ID      string   `json:"id"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage,omitempty"`
}

// Choice represents a single completion choice.
type Choice struct {
	Index        int                `json:"index"`
	Message      prompt.ChatMessage `json:"message"`
	FinishReason string             `json:"finish_reason"`
}

// Usage captures token counts for the request.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Client is an OpenAI-compatible HTTP client equipped with Level 1 exponential backoff resilience.
type Client struct {
	BaseURL        string
	APIKey         string
	Model          string
	HTTPClient     *http.Client
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff       time.Duration
	Headers          map[string]string
	promptTokens     atomic.Int64
	completionTokens atomic.Int64
	totalTokens      atomic.Int64
}

// ClientOption configures a Client instance.
type ClientOption func(*Client)

// WithHTTPClient sets a custom http.Client.
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		c.HTTPClient = httpClient
	}
}

// WithHeaders sets additional HTTP headers for requests.
func WithHeaders(headers map[string]string) ClientOption {
	return func(c *Client) {
		if c.Headers == nil {
			c.Headers = make(map[string]string)
		}
		for k, v := range headers {
			c.Headers[k] = v
		}
	}
}

// WithRetries configures backoff and retry parameters.
func WithRetries(maxRetries int, initialBackoff, maxBackoff time.Duration) ClientOption {
	return func(c *Client) {
		if maxRetries >= 0 {
			c.MaxRetries = maxRetries
		}
		if initialBackoff > 0 {
			c.InitialBackoff = initialBackoff
		}
		if maxBackoff > 0 {
			c.MaxBackoff = maxBackoff
		}
	}
}

// NewClient initializes an OpenAI-compatible chat client.
func NewClient(baseURL, apiKey, model string, opts ...ClientOption) *Client {
	baseURL = strings.TrimRight(baseURL, "/")
	c := &Client{
		BaseURL:        baseURL,
		APIKey:         apiKey,
		Model:          model,
		HTTPClient:     &http.Client{Timeout: 60 * time.Second},
		MaxRetries:     3,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Headers:        make(map[string]string),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// TotalTokens returns the accumulated token count across all requests made by this client.
func (c *Client) TotalTokens() int {
	return int(c.totalTokens.Load())
}

// PromptTokens returns the accumulated prompt token count across all requests.
func (c *Client) PromptTokens() int {
	return int(c.promptTokens.Load())
}

// CompletionTokens returns the accumulated completion token count across all requests.
func (c *Client) CompletionTokens() int {
	return int(c.completionTokens.Load())
}

// isRetryableStatus returns true if the HTTP status code warrants Level 1 retry.
func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return true
	default:
		return false
	}
}

// isRetryableNetworkError checks whether a network-level error should be retried.
func isRetryableNetworkError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	// Connection reset, broken pipe, unexpected EOF
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "unexpected eof") {
		return true
	}
	return false
}

// CreateChatCompletion sends a request to /v1/chat/completions with Level 1 exponential backoff.
func (c *Client) CreateChatCompletion(ctx context.Context, req ChatCompletionRequest) (*ChatCompletionResponse, error) {
	if req.Model == "" {
		req.Model = c.Model
	}

	payloadBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.BaseURL + "/chat/completions"
	if !strings.HasSuffix(c.BaseURL, "/v1") && !strings.Contains(c.BaseURL, "/v1/") {
		url = c.BaseURL + "/v1/chat/completions"
	}

	var lastErr error
	for attempt := 0; attempt <= c.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payloadBytes))
		if err != nil {
			return nil, fmt.Errorf("failed to create http request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if c.APIKey != "" {
			httpReq.Header.Set("Authorization", "Bearer "+c.APIKey)
		}
		if strings.Contains(c.BaseURL, "opencode") {
			if httpReq.Header.Get("x-opencode-session") == "" {
				httpReq.Header.Set("x-opencode-session", "reminis-session")
			}
			if httpReq.Header.Get("x-opencode-client") == "" {
				httpReq.Header.Set("x-opencode-client", "reminis")
			}
		}
		for k, v := range c.Headers {
			httpReq.Header.Set(k, v)
		}

		resp, err := c.HTTPClient.Do(httpReq)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if isRetryableNetworkError(err) && attempt < c.MaxRetries {
				lastErr = err
				c.sleepWithJitter(ctx, attempt)
				continue
			}
			return nil, fmt.Errorf("http request failed: %w", err)
		}

		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			if attempt < c.MaxRetries {
				lastErr = readErr
				c.sleepWithJitter(ctx, attempt)
				continue
			}
			return nil, fmt.Errorf("failed to read response body: %w", readErr)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			var completion ChatCompletionResponse
			if err := json.Unmarshal(body, &completion); err != nil {
				return nil, fmt.Errorf("failed to unmarshal completion response: %w (body: %s)", err, string(body))
			}
			if completion.Usage.TotalTokens > 0 {
				c.promptTokens.Add(int64(completion.Usage.PromptTokens))
				c.completionTokens.Add(int64(completion.Usage.CompletionTokens))
				c.totalTokens.Add(int64(completion.Usage.TotalTokens))
			}
			return &completion, nil
		}

		// Check Level 1 retryable status codes (429, 500, 502, 503)
		if isRetryableStatus(resp.StatusCode) {
			lastErr = fmt.Errorf("http error %d: %s", resp.StatusCode, string(body))
			if attempt < c.MaxRetries {
				c.sleepWithJitter(ctx, attempt)
				continue
			}
			return nil, fmt.Errorf("%w: status %d (body: %s)", ErrMaxRetriesExceeded, resp.StatusCode, string(body))
		}

		// Non-retryable error (e.g. 400 Bad Request, 401 Unauthorized, 404 Not Found)
		return nil, fmt.Errorf("%w: status %d (body: %s)", ErrNonRetryableClient, resp.StatusCode, string(body))
	}

	return nil, fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, lastErr)
}

func (c *Client) sleepWithJitter(ctx context.Context, attempt int) {
	backoff := c.InitialBackoff * (1 << attempt)
	if backoff > c.MaxBackoff {
		backoff = c.MaxBackoff
	}
	// Add randomized jitter up to 25%
	jitter := time.Duration(rand.Float64() * float64(backoff) * 0.25)
	sleepDur := backoff + jitter

	select {
	case <-ctx.Done():
	case <-time.After(sleepDur):
	}
}
