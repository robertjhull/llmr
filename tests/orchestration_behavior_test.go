package tests

import (
	"encoding/json"
	"testing"
	"time"

	llmr "llm-request-router/src"
)

func TestModelRequestsToolUseInResponseAndContinuesJobAfterReceivingResults(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
		if call == 0 {
			return mockResponse{toolCalls: []llmr.ToolCall{{ID: "call1", Name: "get_weather", Arguments: `{"city":"nyc"}`}}}
		}
		return mockResponse{text: "it is sunny"}
	}}}
	o := newOrchestrator(cfg, clients)

	job, err := o.SubmitResponse(llmr.ResponseRequest{
		Pool:  "pool1",
		Input: llmr.ResponseInput{Text: "what's the weather?"},
		Tools: []llmr.Tool{{Name: "get_weather", Description: "gets weather"}},
	})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusRequestedTool {
		t.Fatalf("expected job to pause for the tool call, got %s", job.Status)
	}
	if len(job.PendingToolCalls) != 1 || job.PendingToolCalls[0].Name != "get_weather" {
		t.Fatalf("expected pending tool call for get_weather, got %+v", job.PendingToolCalls)
	}
	firstDeployment := job.DeploymentID

	job2, err := o.SubmitResponse(llmr.ResponseRequest{
		Pool:               "pool1",
		PreviousResponseID: job.ID,
		Input:              llmr.ResponseInput{ToolCallID: "call1", ToolOutput: "sunny, 72F"},
	})
	if err != nil {
		t.Fatalf("SubmitResponse (continuation): %v", err)
	}
	if job2.Status != llmr.StatusDone {
		t.Fatalf("expected job to complete after tool result, got %s: %s", job2.Status, job2.FailureReason)
	}
	if job2.DeploymentID != firstDeployment {
		t.Fatalf("expected continuation pinned to same deployment %s, got %s", firstDeployment, job2.DeploymentID)
	}
	if job2.Result != "it is sunny" {
		t.Fatalf("unexpected result: %q", job2.Result)
	}

	foundToolMsg := false
	for _, m := range job2.History {
		if m.Role == "tool" && m.Content == "sunny, 72F" {
			foundToolMsg = true
		}
	}
	if !foundToolMsg {
		t.Fatal("expected the tool result message to appear in the conversation history")
	}
}

func TestBatchItemPausedOnAToolCallIsContinuedViaItsOwnPerItemID(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
		if call == 0 {
			return mockResponse{toolCalls: []llmr.ToolCall{{ID: "call1", Name: "tool", Arguments: "{}"}}}
		}
		return mockResponse{text: "done"}
	}}}
	o := newOrchestrator(cfg, clients)

	batch, err := o.SubmitBatch(llmr.BatchRequest{
		Pool:  "pool1",
		Tools: []llmr.Tool{{Name: "tool"}},
		Items: []llmr.BatchItemRequest{{Input: llmr.ResponseInput{Text: "hi"}}},
	})
	if err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}
	itemID := batch.Items[0].ID

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if job, _ := o.GetJob(itemID); job != nil && job.Status == llmr.StatusRequestedTool {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	job, err := o.GetJob(itemID)
	if err != nil || job.Status != llmr.StatusRequestedTool {
		t.Fatalf("expected item paused for tool call: err=%v job=%+v", err, job)
	}

	job2, err := o.SubmitResponse(llmr.ResponseRequest{
		Pool:               "pool1",
		PreviousResponseID: itemID,
		Input:              llmr.ResponseInput{ToolCallID: "call1", ToolOutput: "42"},
	})
	if err != nil {
		t.Fatalf("SubmitResponse (continuation via item id): %v", err)
	}
	if job2.Status != llmr.StatusDone {
		t.Fatalf("expected item to complete, got %s", job2.Status)
	}

	gotBatch, err := o.GetBatch(batch.ID)
	if err != nil {
		t.Fatalf("GetBatch: %v", err)
	}
	if gotBatch.Items[0].Job.Status != llmr.StatusDone {
		t.Fatalf("expected the batch's own snapshot to reflect the completed item, got %s", gotBatch.Items[0].Job.Status)
	}
}

func TestBatchItemContinuationAfterAToolCallIsPlacedFreshRatherThanPinned(t *testing.T) {
	cfg := twoModelConfig("pool1", 100, 100)
	release := make(chan struct{})
	clients := map[string]*mockClient{
		"a": {tokensPerSec: 50, respond: func(call int, req llmr.InferRequest) mockResponse {
			if call == 0 {
				return mockResponse{toolCalls: []llmr.ToolCall{{ID: "call1", Name: "tool"}}}
			}
			<-release
			return mockResponse{text: "other work done"}
		}},
		"b": {responses: []mockResponse{{text: "continuation done"}}},
	}
	o := newOrchestrator(cfg, clients)

	batch, err := o.SubmitBatch(llmr.BatchRequest{
		Pool:  "pool1",
		Tools: []llmr.Tool{{Name: "tool"}},
		Items: []llmr.BatchItemRequest{{Input: llmr.ResponseInput{Text: "hi"}}},
	})
	if err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}
	itemID := batch.Items[0].ID

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if job, _ := o.GetJob(itemID); job != nil && job.Status == llmr.StatusRequestedTool {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	otherDone := make(chan *llmr.Job, 1)
	go func() {
		job, _ := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", PinnedDeploymentID: "a", Input: llmr.ResponseInput{Text: "unrelated work"}})
		otherDone <- job
	}()
	time.Sleep(100 * time.Millisecond)

	job2, err := o.SubmitResponse(llmr.ResponseRequest{
		Pool:               "pool1",
		PreviousResponseID: itemID,
		Input:              llmr.ResponseInput{ToolCallID: "call1", ToolOutput: "42"},
	})
	if err != nil {
		t.Fatalf("SubmitResponse (continuation): %v", err)
	}
	if job2.DeploymentID != "b" {
		t.Fatalf("expected the batch item's continuation to be placed fresh onto idle b rather than waiting on busy a, got %s", job2.DeploymentID)
	}

	close(release)
	<-otherDone
}

func TestSkillReferenceExpandsIntoToolDefinitionsForTheJob(t *testing.T) {
	cfg := &llmr.Config{
		Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
			{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
		}}},
		Skills: map[string]llmr.Skill{
			"web-search": {
				Tools:        []llmr.Tool{{Name: "search_web", Description: "search"}},
				Instructions: "use search when needed",
			},
		},
	}

	var capturedTools []llmr.Tool
	var capturedInstructions string
	clients := map[string]*mockClient{"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
		capturedTools = req.Tools
		capturedInstructions = req.Instructions
		return mockResponse{text: "ok"}
	}}}
	o := newOrchestrator(cfg, clients)

	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, Skills: []string{"web-search"}})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusDone {
		t.Fatalf("expected job to succeed, got %s", job.Status)
	}

	found := false
	for _, tool := range capturedTools {
		if tool.Name == "search_web" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected the skill's tool definition to be merged into the job, got tools=%+v", capturedTools)
	}
	if capturedInstructions != "use search when needed" {
		t.Fatalf("expected skill instructions to be merged in, got %q", capturedInstructions)
	}
}

func TestFinalResponseSchemaMismatchTriggersCorrectiveContinuationOnSameDeployment(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
		if call == 0 {
			return mockResponse{text: `{"wrong_field": 1}`}
		}
		return mockResponse{text: `{"answer": 42}`}
	}}}
	o := newOrchestrator(cfg, clients)

	schema := json.RawMessage(`{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"]}`)
	maxRetries := 2
	job, err := o.SubmitResponse(llmr.ResponseRequest{
		Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"},
		FinalResponseFormat: schema, MaxRetries: &maxRetries,
	})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusDone {
		t.Fatalf("expected job to eventually succeed, got %s: %s", job.Status, job.FailureReason)
	}
	if job.Result != `{"answer": 42}` {
		t.Fatalf("unexpected result: %q", job.Result)
	}
	if job.Attempts != 1 {
		t.Fatalf("expected one corrective attempt counted against max_retries, got %d", job.Attempts)
	}
	if clients["a"].Calls() != 2 {
		t.Fatalf("expected exactly 2 calls, both on deployment a, got %d", clients["a"].Calls())
	}
}

func TestFinalResponseSchemaMismatchExhaustingMaxRetriesFailsTheJob(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{"a": {responses: []mockResponse{{text: `{"wrong_field": 1}`}}}}
	o := newOrchestrator(cfg, clients)

	schema := json.RawMessage(`{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"]}`)
	maxRetries := 2
	job, err := o.SubmitResponse(llmr.ResponseRequest{
		Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"},
		FinalResponseFormat: schema, MaxRetries: &maxRetries,
	})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusFailed {
		t.Fatalf("expected job to fail after exhausting retries on schema mismatch, got %s", job.Status)
	}
	if clients["a"].Calls() != maxRetries+1 {
		t.Fatalf("expected max_retries+1=%d calls, got %d", maxRetries+1, clients["a"].Calls())
	}
}

func TestUnknownSkillReferenceFailsTheRequest(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	o := newOrchestrator(cfg, map[string]*mockClient{"a": {}})

	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, Skills: []string{"does-not-exist"}})
	if err == nil {
		t.Fatalf("expected an error for an unknown skill reference, got job=%+v", job)
	}
}

func TestMalformedFinalResponseSchemaFailsTheRequest(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{"a": {}}
	o := newOrchestrator(cfg, clients)

	malformed := json.RawMessage(`{"type": "not-a-real-json-schema-type"`)
	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, FinalResponseFormat: malformed})
	if err == nil {
		t.Fatalf("expected malformed schema to be rejected before any inference call, got job=%+v", job)
	}
	if clients["a"].Calls() != 0 {
		t.Fatalf("expected no inference call to be made, got %d calls", clients["a"].Calls())
	}
}
