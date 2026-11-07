package llmr

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type ResponsesAPIResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

type InferRequest struct {
	Model        string
	History      []Message
	Tools        []Tool
	ToolChoice   string
	Instructions string
}

type InferOutcome struct {
	Text      string
	ToolCalls []ToolCall
	TokensOut int
}

type InferEventKind int

const (
	InferEventChunk InferEventKind = iota
	InferEventDone
	InferEventError
)

type InferEvent struct {
	Kind    InferEventKind
	Text    string
	Outcome InferOutcome
	Err     error
}

type DeploymentClient interface {
	Infer(ctx context.Context, req InferRequest) <-chan InferEvent
}

func NewDeploymentClient(d Deployment) (DeploymentClient, error) {
	if d.APIBase == "" {
		return nil, fmt.Errorf("deployment %s has no api_base", d.ID)
	}
	return &openaiClient{
		apiBase: strings.TrimRight(d.APIBase, "/"),
		apiKey:  d.APIKey,
		model:   d.Model,
		http:    &http.Client{Timeout: 5 * time.Minute},
	}, nil
}

type openaiClient struct {
	apiBase string
	apiKey  string
	model   string
	http    *http.Client
}

type chatMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
}

type chatToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function chatToolFunction `json:"function"`
}

type chatToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type chatTool struct {
	Type     string       `json:"type"`
	Function chatFunction `json:"function"`
}

type chatFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

type chatCompletionChunk struct {
	Choices []struct {
		Delta struct {
			Content   string `json:"content"`
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
}

func (c *openaiClient) Infer(ctx context.Context, req InferRequest) <-chan InferEvent {
	events := make(chan InferEvent, 8)

	go func() {
		defer close(events)

		var messages []chatMessage
		if req.Instructions != "" {
			messages = append(messages, chatMessage{Role: "system", Content: req.Instructions})
		}
		for _, m := range req.History {
			cm := chatMessage{Role: m.Role, Content: m.Content, ToolCallID: m.ToolCallID}
			for _, tc := range m.ToolCalls {
				cm.ToolCalls = append(cm.ToolCalls, chatToolCall{
					ID:   tc.ID,
					Type: "function",
					Function: chatToolFunction{
						Name:      tc.Name,
						Arguments: tc.Arguments,
					},
				})
			}
			messages = append(messages, cm)
		}

		var tools []chatTool
		for _, t := range req.Tools {
			tools = append(tools, chatTool{
				Type: "function",
				Function: chatFunction{
					Name:        t.Name,
					Description: t.Description,
					Parameters:  t.Parameters,
				},
			})
		}

		body, err := json.Marshal(map[string]any{
			"model":    req.Model,
			"messages": messages,
			"tools":    tools,
			"stream":   true,
		})
		if err != nil {
			events <- InferEvent{Kind: InferEventError, Err: err}
			return
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiBase+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			events <- InferEvent{Kind: InferEventError, Err: err}
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if c.apiKey != "" {
			httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
		}

		resp, err := c.http.Do(httpReq)
		if err != nil {
			events <- InferEvent{Kind: InferEventError, Err: err}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 300 {
			events <- InferEvent{Kind: InferEventError, Err: fmt.Errorf("deployment returned status %s", resp.Status)}
			return
		}

		var textBuilder strings.Builder
		toolCalls := map[int]*ToolCall{}
		var toolOrder []int

		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload == "[DONE]" {
				break
			}

			var chunk chatCompletionChunk
			if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
				continue
			}
			if len(chunk.Choices) == 0 {
				continue
			}
			delta := chunk.Choices[0].Delta
			if delta.Content != "" {
				textBuilder.WriteString(delta.Content)
				events <- InferEvent{Kind: InferEventChunk, Text: delta.Content}
			}
			for _, tc := range delta.ToolCalls {
				existing, ok := toolCalls[tc.Index]
				if !ok {
					existing = &ToolCall{}
					toolCalls[tc.Index] = existing
					toolOrder = append(toolOrder, tc.Index)
				}
				if tc.ID != "" {
					existing.ID = tc.ID
				}
				if tc.Function.Name != "" {
					existing.Name += tc.Function.Name
				}
				existing.Arguments += tc.Function.Arguments
			}
		}
		if err := scanner.Err(); err != nil {
			events <- InferEvent{Kind: InferEventError, Err: err}
			return
		}

		var calls []ToolCall
		for _, idx := range toolOrder {
			calls = append(calls, *toolCalls[idx])
		}

		text := textBuilder.String()
		events <- InferEvent{
			Kind: InferEventDone,
			Outcome: InferOutcome{
				Text:      text,
				ToolCalls: calls,
				TokensOut: EstimateTokens(text),
			},
		}
	}()

	return events
}
