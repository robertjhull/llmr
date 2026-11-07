package tests

import (
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	llmr "llm-request-router/src"
)

func TestRouterLoadsPersistedMetricsOnStartupAndUsesThemImmediately(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pools.json")

	cfg, err := llmr.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	slow, err := cfg.AddDeployment("pool1", llmr.Deployment{Model: "model-slow", APIBase: "mock://slow", Capacity: 1})
	if err != nil {
		t.Fatalf("AddDeployment: %v", err)
	}
	fast, err := cfg.AddDeployment("pool1", llmr.Deployment{Model: "model-fast", APIBase: "mock://fast", Capacity: 1})
	if err != nil {
		t.Fatalf("AddDeployment: %v", err)
	}

	o1 := llmr.NewOrchestrator(cfg)
	clients := map[string]*mockClient{
		slow.ID: {tokensPerSec: 200},
		fast.ID: {tokensPerSec: 20000},
	}
	o1.ClientFactory = clientFactoryFor(clients)

	for _, id := range []string{slow.ID, fast.ID} {
		for i := 0; i < 3; i++ {
			job, err := o1.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, PinnedDeploymentID: id})
			if err != nil || job.Status != llmr.StatusDone {
				t.Fatalf("priming request to %s failed: err=%v job=%+v", id, err, job)
			}
		}
	}

	cfg2, err := llmr.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig (reload): %v", err)
	}

	o2 := llmr.NewOrchestrator(cfg2)
	o2.ClientFactory = clientFactoryFor(map[string]*mockClient{
		slow.ID: {tokensPerSec: 200},
		fast.ID: {tokensPerSec: 20000},
	})

	job, err := o2.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: strings.Repeat("hello ", 50)}})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.DeploymentID != fast.ID {
		t.Fatalf("expected a freshly started router to immediately prefer the persisted-fast deployment, got %s", job.DeploymentID)
	}
}

func TestCancellingAQueuedJobReleasesItWithoutEverClaimingASlot(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	release := make(chan struct{})
	clients := map[string]*mockClient{"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
		<-release
		return mockResponse{text: "ok"}
	}}}
	o := newOrchestrator(cfg, clients)

	firstDone := make(chan *llmr.Job, 1)
	go func() {
		job, _ := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
		firstDone <- job
	}()
	time.Sleep(50 * time.Millisecond)

	batch, err := o.SubmitBatch(llmr.BatchRequest{Pool: "pool1", Items: []llmr.BatchItemRequest{{Input: llmr.ResponseInput{Text: "hi"}}}})
	if err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	snapshot := o.PoolsSnapshot()
	if snapshot[0].Queued != 1 {
		t.Fatalf("expected the batch item to be queued, got queued=%d", snapshot[0].Queued)
	}

	if err := o.CancelJob(batch.Items[0].ID); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	job, err := o.GetJob(batch.Items[0].ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Status != llmr.StatusCancelled {
		t.Fatalf("expected cancelled status, got %s", job.Status)
	}

	snapshot = o.PoolsSnapshot()
	if snapshot[0].Queued != 0 {
		t.Fatalf("expected queue depth to decrement after cancel, got %d", snapshot[0].Queued)
	}

	close(release)
	<-firstDone
}

func TestCancellingAJobMidInferenceReleasesItsClaimedSlotForOthers(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{"a": {tokensPerSec: 0.1}}
	o := newOrchestrator(cfg, clients)

	batch, err := o.SubmitBatch(llmr.BatchRequest{Pool: "pool1", Items: []llmr.BatchItemRequest{{Input: llmr.ResponseInput{Text: "hi"}}}})
	if err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}
	firstID := batch.Items[0].ID

	time.Sleep(50 * time.Millisecond)
	snapshot := o.PoolsSnapshot()
	if snapshot[0].Models[0].Status != string(llmr.StatusInferring) {
		t.Fatalf("expected deployment to be actively processing the job, got status=%s", snapshot[0].Models[0].Status)
	}

	if err := o.CancelJob(firstID); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if o.PoolsSnapshot()[0].Models[0].Status == string(llmr.StatusIdle) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	snapshot = o.PoolsSnapshot()
	if snapshot[0].Models[0].Status != string(llmr.StatusIdle) {
		t.Fatalf("expected deployment slot to be freed after cancellation, got status=%s", snapshot[0].Models[0].Status)
	}

	job, err := o.GetJob(firstID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Status != llmr.StatusCancelled {
		t.Fatalf("expected job status cancelled, got %s", job.Status)
	}
}

func TestFreedSlotIsClaimedByExactlyOneWaitingJob(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	release := make(chan struct{})
	var concurrent int32
	clients := map[string]*mockClient{"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
		if atomic.AddInt32(&concurrent, 1) > 1 {
			t.Errorf("more than one job held deployment a's single slot concurrently")
		}
		defer atomic.AddInt32(&concurrent, -1)
		if call == 0 {
			<-release
		}
		return mockResponse{text: "ok"}
	}}}
	o := newOrchestrator(cfg, clients)

	firstDone := make(chan *llmr.Job, 1)
	go func() {
		job, _ := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
		firstDone <- job
	}()
	time.Sleep(50 * time.Millisecond)

	const n = 5
	results := make([]*llmr.Job, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			job, _ := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
			results[i] = job
		}(i)
	}
	time.Sleep(50 * time.Millisecond)

	close(release)
	<-firstDone
	wg.Wait()

	seen := map[string]bool{}
	for _, j := range results {
		if j == nil || j.Status != llmr.StatusDone {
			t.Fatalf("expected all queued jobs to eventually complete: %+v", j)
		}
		if seen[j.ID] {
			t.Fatalf("job %s appears more than once in results", j.ID)
		}
		seen[j.ID] = true
	}
}

func TestToolRoundLimitExceededWithoutFinalResponseFormatFailsTheJob(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
		return mockResponse{toolCalls: []llmr.ToolCall{{ID: "call1", Name: "some_tool", Arguments: "{}"}}}
	}}}
	o := newOrchestrator(cfg, clients)

	maxToolRounds := 1
	job, err := o.SubmitResponse(llmr.ResponseRequest{
		Pool:          "pool1",
		Input:         llmr.ResponseInput{Text: "hi"},
		Tools:         []llmr.Tool{{Name: "some_tool", Description: "does a thing"}},
		MaxToolRounds: &maxToolRounds,
	})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusRequestedTool {
		t.Fatalf("expected job to pause for the first tool call, got %s", job.Status)
	}

	job2, err := o.SubmitResponse(llmr.ResponseRequest{
		Pool:               "pool1",
		PreviousResponseID: job.ID,
		Input:              llmr.ResponseInput{ToolCallID: "call1", ToolOutput: "42"},
	})
	if err != nil {
		t.Fatalf("SubmitResponse (continuation): %v", err)
	}
	if job2.Status != llmr.StatusFailed {
		t.Fatalf("expected job to fail once tool round limit is exceeded, got %s", job2.Status)
	}
}
