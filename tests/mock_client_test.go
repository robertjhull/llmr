package tests

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	llmr "llm-request-router/src"
)

func TestMain(m *testing.M) {
	llmr.CooldownDuration = 150 * time.Millisecond
	os.Exit(m.Run())
}

type mockResponse struct {
	text      string
	toolCalls []llmr.ToolCall
	err       error
	tokens    int
}

type mockClient struct {
	mu           sync.Mutex
	tokensPerSec float64
	speedFunc    func(call int) float64
	calls        int32
	responses    []mockResponse
	respond      func(call int, req llmr.InferRequest) mockResponse
	streamGap    time.Duration
}

func (m *mockClient) Infer(ctx context.Context, req llmr.InferRequest) <-chan llmr.InferEvent {
	events := make(chan llmr.InferEvent, 1)

	call := int(atomic.AddInt32(&m.calls, 1)) - 1

	go func() {
		defer close(events)

		m.mu.Lock()
		respond := m.respond
		responses := m.responses
		m.mu.Unlock()

		var resp mockResponse
		switch {
		case respond != nil:
			resp = respond(call, req)
		case len(responses) > 0:
			idx := call
			if idx >= len(responses) {
				idx = len(responses) - 1
			}
			resp = responses[idx]
		}

		tokens := resp.tokens
		if tokens == 0 {
			tokens = 40
		}

		tps := m.tokensPerSec
		if m.speedFunc != nil {
			tps = m.speedFunc(call)
		}

		var delay time.Duration
		if tps > 0 {
			delay = time.Duration(float64(tokens) / tps * float64(time.Second))
		}

		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				events <- llmr.InferEvent{Kind: llmr.InferEventError, Err: ctx.Err()}
				return
			}
		}

		if resp.err != nil {
			events <- llmr.InferEvent{Kind: llmr.InferEventError, Err: resp.err}
			return
		}

		if resp.text != "" || len(resp.toolCalls) > 0 {
			events <- llmr.InferEvent{Kind: llmr.InferEventChunk, Text: resp.text}
			if m.streamGap > 0 {
				select {
				case <-time.After(m.streamGap):
				case <-ctx.Done():
					events <- llmr.InferEvent{Kind: llmr.InferEventError, Err: ctx.Err()}
					return
				}
			}
		}

		events <- llmr.InferEvent{Kind: llmr.InferEventDone, Outcome: llmr.InferOutcome{
			Text:      resp.text,
			ToolCalls: resp.toolCalls,
			TokensOut: tokens,
		}}
	}()

	return events
}

func (m *mockClient) Calls() int {
	return int(atomic.LoadInt32(&m.calls))
}

func clientFactoryFor(clients map[string]*mockClient) func(llmr.Deployment) (llmr.DeploymentClient, error) {
	return func(d llmr.Deployment) (llmr.DeploymentClient, error) {
		if c, ok := clients[d.ID]; ok {
			return c, nil
		}
		return &mockClient{}, nil
	}
}

func newOrchestrator(cfg *llmr.Config, clients map[string]*mockClient) *llmr.Orchestrator {
	o := llmr.NewOrchestrator(cfg)
	o.ClientFactory = clientFactoryFor(clients)
	return o
}
