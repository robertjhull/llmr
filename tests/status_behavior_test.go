package tests

import (
	"errors"
	"sync"
	"testing"
	"time"

	llmr "llm-request-router/src"
)

func TestPoolsShowAccurateQueuedStatus(t *testing.T) {
	cfg := twoModelConfig("pool1", 100, 100)
	release := make(chan struct{})
	block := func(call int, req llmr.InferRequest) mockResponse {
		<-release
		return mockResponse{text: "ok"}
	}
	clients := map[string]*mockClient{"a": {respond: block}, "b": {respond: block}}
	o := newOrchestrator(cfg, clients)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
		}()
	}
	time.Sleep(100 * time.Millisecond)

	snap := o.PoolsSnapshot()
	if snap[0].Queued != 2 {
		t.Fatalf("expected 2 queued while both models are busy, got %d", snap[0].Queued)
	}
	for _, m := range snap[0].Models {
		if m.Status != string(llmr.StatusInferring) {
			t.Fatalf("expected model %s to be busy, got %s", m.ID, m.Status)
		}
	}

	close(release)
	wg.Wait()

	snap = o.PoolsSnapshot()
	if snap[0].Queued != 0 {
		t.Fatalf("expected queue to drain to 0 once all requests complete, got %d", snap[0].Queued)
	}
}

func TestPoolsShowAllStatuses(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
		{ID: "b", Model: "model-b", APIBase: "mock://b", Capacity: 1},
	}}}}

	clients := map[string]*mockClient{
		"a": {tokensPerSec: 50, responses: []mockResponse{{err: errors.New("transient failure")}}},
		"b": {tokensPerSec: 50, streamGap: 30 * time.Millisecond, respond: func(call int, req llmr.InferRequest) mockResponse {
			if call == 0 {
				return mockResponse{toolCalls: []llmr.ToolCall{{ID: "call1", Name: "some_tool", Arguments: "{}"}}}
			}
			return mockResponse{text: "final answer"}
		}},
	}
	o := newOrchestrator(cfg, clients)

	maxRetries := 1
	batch, err := o.SubmitBatch(llmr.BatchRequest{
		Pool:       "pool1",
		Tools:      []llmr.Tool{{Name: "some_tool", Description: "does a thing"}},
		MaxRetries: &maxRetries,
		Items:      []llmr.BatchItemRequest{{Input: llmr.ResponseInput{Text: "hi"}}},
	})
	if err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}
	jobID := batch.Items[0].ID

	seen := map[string]bool{}
	var seenMu sync.Mutex
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if job, err := o.GetJob(jobID); err == nil {
				seenMu.Lock()
				seen[string(job.Status)] = true
				seenMu.Unlock()
			}
		}
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if job, _ := o.GetJob(jobID); job != nil && job.Status == llmr.StatusRequestedTool {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Dispatched in the background (see SubmitResponse) -- poll for
	// completion instead of asserting on the return value, which is no
	// longer meaningful the instant this call returns.
	if _, err := o.SubmitResponse(llmr.ResponseRequest{
		Pool:               "pool1",
		PreviousResponseID: jobID,
		Input:              llmr.ResponseInput{ToolCallID: "call1", ToolOutput: "42"},
	}); err != nil {
		t.Fatalf("SubmitResponse (continuation): %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if job, _ := o.GetJob(jobID); job != nil && job.Status == llmr.StatusDone {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	job2, err := o.GetJob(jobID)
	if err != nil || job2.Status != llmr.StatusDone {
		t.Fatalf("expected job to finish done, got %s: %s", job2.Status, job2.FailureReason)
	}

	close(stop)

	for _, want := range []llmr.JobStatus{llmr.StatusInferring, llmr.StatusStreaming, llmr.StatusRequestedTool} {
		seenMu.Lock()
		ok := seen[string(want)]
		seenMu.Unlock()
		if !ok {
			t.Errorf("expected to observe status %q at some point during the job's lifecycle", want)
		}
	}

	snap := o.PoolsSnapshot()
	for _, m := range snap[0].Models {
		if m.Status != string(llmr.StatusIdle) {
			t.Fatalf("expected all models to be idle after completion, got %s=%s", m.ID, m.Status)
		}
	}
}

func TestFailingModelShowsCooldown(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{"a": {responses: []mockResponse{{err: errors.New("fail")}}}}
	o := newOrchestrator(cfg, clients)

	maxRetries := 0
	for i := 0; i < llmr.CooldownFailureThreshold; i++ {
		job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, MaxRetries: &maxRetries})
		if err != nil || job.Status != llmr.StatusFailed {
			t.Fatalf("expected failure %d to fail cleanly: err=%v job=%+v", i, err, job)
		}
	}

	snap := o.PoolsSnapshot()
	if snap[0].Models[0].Status != string(llmr.StatusCooldown) {
		t.Fatalf("expected model to show cooldown/unhealthy on the status page, got %s", snap[0].Models[0].Status)
	}
}

func TestPoolsDoNotLeakQueuedOrActiveStatusIntoEachOther(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{
		{Name: "poolA", Deployments: []llmr.Deployment{{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1}}},
		{Name: "poolB", Deployments: []llmr.Deployment{{ID: "b", Model: "model-b", APIBase: "mock://b", Capacity: 1}}},
	}}
	releaseB := make(chan struct{})
	clients := map[string]*mockClient{
		"a": {responses: []mockResponse{{text: "ok"}}},
		"b": {respond: func(call int, req llmr.InferRequest) mockResponse {
			<-releaseB
			return mockResponse{text: "ok"}
		}},
	}
	o := newOrchestrator(cfg, clients)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.SubmitResponse(llmr.ResponseRequest{Pool: "poolB", Input: llmr.ResponseInput{Text: "hi"}})
		}()
	}
	time.Sleep(50 * time.Millisecond)

	snap := o.PoolsSnapshot()
	var poolAStatus, poolBStatus llmr.PoolStatus
	for _, p := range snap {
		if p.Name == "poolA" {
			poolAStatus = p
		}
		if p.Name == "poolB" {
			poolBStatus = p
		}
	}

	if poolAStatus.Queued != 0 {
		t.Fatalf("expected pool A to show 0 queued (unaffected by pool B activity), got %d", poolAStatus.Queued)
	}
	if poolAStatus.Models[0].Status != string(llmr.StatusIdle) {
		t.Fatalf("expected pool A's model to remain idle, got %s", poolAStatus.Models[0].Status)
	}
	if poolBStatus.Queued != 2 {
		t.Fatalf("expected pool B to show 2 queued, got %d", poolBStatus.Queued)
	}
	if poolBStatus.Models[0].Status != string(llmr.StatusInferring) {
		t.Fatalf("expected pool B's model to be busy, got %s", poolBStatus.Models[0].Status)
	}

	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "poolA", Input: llmr.ResponseInput{Text: "hi"}})
	if err != nil || job.Status != llmr.StatusDone {
		t.Fatalf("expected pool A's request to succeed independently of pool B: err=%v job=%+v", err, job)
	}

	close(releaseB)
	wg.Wait()
}
