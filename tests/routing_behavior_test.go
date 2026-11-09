package tests

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	llmr "llm-request-router/src"
)

func twoModelConfig(poolName string, tpsA, tpsB float64) *llmr.Config {
	return &llmr.Config{Pools: []llmr.Pool{{Name: poolName, Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1, TokensPerSecond: tpsA},
		{ID: "b", Model: "model-b", APIBase: "mock://b", Capacity: 1, TokensPerSecond: tpsB},
	}}}}
}

func submitConcurrent(o *llmr.Orchestrator, pool string, n int) []*llmr.Job {
	jobs := make([]*llmr.Job, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: pool, Input: llmr.ResponseInput{Text: "hello"}})
			jobs[i] = job
			errs[i] = err
		}(i)
	}
	wg.Wait()
	return jobs
}

func TestTenQueuedAgainstTwoModelsSameSpeedEqualDistribution(t *testing.T) {
	cfg := twoModelConfig("pool1", 100, 100)
	clients := map[string]*mockClient{
		"a": {tokensPerSec: 100},
		"b": {tokensPerSec: 100},
	}
	o := newOrchestrator(cfg, clients)

	jobs := submitConcurrent(o, "pool1", 10)

	counts := map[string]int{}
	for _, j := range jobs {
		if j == nil {
			t.Fatal("submission failed")
		}
		if j.Status != llmr.StatusDone {
			t.Fatalf("job %s ended with status %s: %s", j.ID, j.Status, j.FailureReason)
		}
		counts[j.DeploymentID]++
	}

	if counts["a"] != 5 || counts["b"] != 5 {
		t.Fatalf("expected 5/5 split, got a=%d b=%d", counts["a"], counts["b"])
	}
}

func TestTenQueuedAgainstTwoModelsDifferentSpeedUnequalDistribution(t *testing.T) {
	cfg := twoModelConfig("pool1", 400, 100)
	clients := map[string]*mockClient{
		"a": {tokensPerSec: 400},
		"b": {tokensPerSec: 100},
	}
	o := newOrchestrator(cfg, clients)

	jobs := submitConcurrent(o, "pool1", 10)

	counts := map[string]int{}
	for _, j := range jobs {
		if j == nil || j.Status != llmr.StatusDone {
			t.Fatalf("job did not complete successfully: %+v", j)
		}
		counts[j.DeploymentID]++
	}

	if counts["a"]+counts["b"] != 10 {
		t.Fatalf("expected 10 total completions, got a=%d b=%d", counts["a"], counts["b"])
	}
	if counts["a"] < 2*counts["b"] {
		t.Fatalf("expected faster model (a) to handle at least double, got a=%d b=%d", counts["a"], counts["b"])
	}
}

func TestTenQueuedAgainstTwoModelsChangingSpeedChangesDistribution(t *testing.T) {
	cfg := twoModelConfig("pool1", 200, 100)
	clients := map[string]*mockClient{
		"a": {speedFunc: func(call int) float64 {
			if call < 2 {
				return 200
			}
			return 20
		}},
		"b": {tokensPerSec: 100},
	}
	o := newOrchestrator(cfg, clients)

	jobs := submitConcurrent(o, "pool1", 10)

	counts := map[string]int{}
	for _, j := range jobs {
		if j == nil || j.Status != llmr.StatusDone {
			t.Fatalf("job did not complete successfully: %+v", j)
		}
		counts[j.DeploymentID]++
	}

	if counts["a"]+counts["b"] != 10 {
		t.Fatalf("expected 10 total completions, got a=%d b=%d", counts["a"], counts["b"])
	}
	if counts["b"] <= counts["a"] {
		t.Fatalf("expected b to overtake a once a degrades, got a=%d b=%d", counts["a"], counts["b"])
	}
}

func TestTenQueuedAgainstTwoModelsModelErrorsOutRoutesRemainingRequestsToHealthyModel(t *testing.T) {
	cfg := twoModelConfig("pool1", 100, 100)
	clients := map[string]*mockClient{
		"a": {responses: []mockResponse{{err: errors.New("deployment unreachable")}}},
		"b": {tokensPerSec: 100},
	}
	o := newOrchestrator(cfg, clients)

	jobs := submitConcurrent(o, "pool1", 10)

	for _, j := range jobs {
		if j == nil || j.Status != llmr.StatusDone {
			t.Fatalf("job did not complete successfully: %+v", j)
		}
		if j.DeploymentID != "b" {
			t.Fatalf("expected all completions on healthy model b, job %s completed on %s", j.ID, j.DeploymentID)
		}
	}
}

func TestTenQueuedAgainstTwoModelsBothModelsErrorOut(t *testing.T) {
	cfg := twoModelConfig("pool1", 100, 100)
	clients := map[string]*mockClient{
		"a": {responses: []mockResponse{{err: errors.New("deployment unreachable")}}},
		"b": {responses: []mockResponse{{err: errors.New("deployment unreachable")}}},
	}
	o := newOrchestrator(cfg, clients)

	jobs := submitConcurrent(o, "pool1", 10)

	for _, j := range jobs {
		if j == nil {
			t.Fatal("submission failed")
		}
		if j.Status != llmr.StatusFailed {
			t.Fatalf("expected job %s to fail, got status %s", j.ID, j.Status)
		}
	}
}

func TestOneRequestFailsOnModelRetriesOnOtherModels(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
		{ID: "b", Model: "model-b", APIBase: "mock://b", Capacity: 1},
		{ID: "c", Model: "model-c", APIBase: "mock://c", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{
		"a": {responses: []mockResponse{{err: errors.New("fail a")}}},
		"b": {responses: []mockResponse{{err: errors.New("fail b")}}},
		"c": {responses: []mockResponse{{err: errors.New("fail c")}}},
	}
	o := newOrchestrator(cfg, clients)

	maxRetries := 2
	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, MaxRetries: &maxRetries})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusFailed {
		t.Fatalf("expected job to fail after exhausting all 3 models, got %s", job.Status)
	}
	if clients["a"].Calls() != 1 || clients["b"].Calls() != 1 || clients["c"].Calls() != 1 {
		t.Fatalf("expected each model tried exactly once, got a=%d b=%d c=%d", clients["a"].Calls(), clients["b"].Calls(), clients["c"].Calls())
	}
}

func TestModelInCooldownBecomesEligibleAgainAfterItsCooldownWindowElapses(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{"a": {}}
	o := newOrchestrator(cfg, clients)

	maxRetries := 0
	for i := 0; i < 3; i++ {
		clients["a"].mu.Lock()
		clients["a"].responses = []mockResponse{{err: errors.New("fail")}}
		clients["a"].mu.Unlock()
		job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, MaxRetries: &maxRetries})
		if err != nil {
			t.Fatalf("SubmitResponse: %v", err)
		}
		if job.Status != llmr.StatusFailed {
			t.Fatalf("expected failure %d to fail cleanly, got %s", i, job.Status)
		}
	}

	snapshot := o.PoolsSnapshot()
	if snapshot[0].Models[0].Status != string(llmr.StatusCooldown) {
		t.Fatalf("expected model in cooldown after 3 consecutive failures, got %s", snapshot[0].Models[0].Status)
	}

	time.Sleep(llmr.CooldownDuration + 50*time.Millisecond)

	clients["a"].mu.Lock()
	clients["a"].responses = []mockResponse{{text: "ok"}}
	clients["a"].mu.Unlock()

	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusDone {
		t.Fatalf("expected model to be back in rotation after cooldown elapses, got %s: %s", job.Status, job.FailureReason)
	}
}

func TestCooldownRequiresAConsecutiveFailureThresholdNotASingleBlip(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{"a": {responses: []mockResponse{{err: errors.New("blip")}}}}
	o := newOrchestrator(cfg, clients)

	maxRetries := 0
	job1, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, MaxRetries: &maxRetries})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job1.Status != llmr.StatusFailed {
		t.Fatalf("expected the lone blip to fail this one request (no other deployment to fall back to), got %s", job1.Status)
	}

	snapshot := o.PoolsSnapshot()
	if snapshot[0].Models[0].Status == string(llmr.StatusCooldown) {
		t.Fatal("a single failure should not trigger cooldown")
	}

	clients["a"].mu.Lock()
	clients["a"].responses = []mockResponse{{text: "ok"}}
	clients["a"].mu.Unlock()

	job2, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi again"}})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job2.Status != llmr.StatusDone || job2.DeploymentID != "a" {
		t.Fatalf("expected model to still serve new requests after one blip, got status=%s deployment=%s", job2.Status, job2.DeploymentID)
	}
}

func TestFailureStreakResetsAfterAnInterveningSuccessfulCall(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{"a": {}}
	o := newOrchestrator(cfg, clients)
	maxRetries := 0

	runOnce := func(shouldFail bool) llmr.JobStatus {
		clients["a"].mu.Lock()
		if shouldFail {
			clients["a"].responses = []mockResponse{{err: errors.New("fail")}}
		} else {
			clients["a"].responses = []mockResponse{{text: "ok"}}
		}
		clients["a"].mu.Unlock()
		job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, MaxRetries: &maxRetries})
		if err != nil {
			t.Fatalf("SubmitResponse: %v", err)
		}
		return job.Status
	}

	for i := 0; i < llmr.CooldownFailureThreshold-1; i++ {
		if status := runOnce(true); status != llmr.StatusFailed {
			t.Fatalf("expected failure %d to fail, got %s", i, status)
		}
	}
	if status := runOnce(false); status != llmr.StatusDone {
		t.Fatalf("expected intervening success to succeed, got %s", status)
	}
	for i := 0; i < llmr.CooldownFailureThreshold-1; i++ {
		if status := runOnce(true); status != llmr.StatusFailed {
			t.Fatalf("expected post-reset failure %d to fail, got %s", i, status)
		}
	}

	snapshot := o.PoolsSnapshot()
	if snapshot[0].Models[0].Status == string(llmr.StatusCooldown) {
		t.Fatal("failure count should have reset after the intervening success, cooldown should not have triggered")
	}
}

func TestRetrySucceedsOnSecondModelAfterFirstModelFails(t *testing.T) {
	cfg := twoModelConfig("pool1", 100, 100)
	clients := map[string]*mockClient{
		"a": {responses: []mockResponse{{err: errors.New("fail a")}}},
		"b": {responses: []mockResponse{{text: "ok"}}},
	}
	o := newOrchestrator(cfg, clients)

	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusDone {
		t.Fatalf("expected job to succeed via retry, got %s: %s", job.Status, job.FailureReason)
	}
	if job.DeploymentID != "b" {
		t.Fatalf("expected job to report model b, got %s", job.DeploymentID)
	}
	if job.Attempts != 1 {
		t.Fatalf("expected attempt count of 1 (one failure before success), got %d", job.Attempts)
	}
}

func TestRetryDoesNotReuseAModelAlreadyFailedForThisJob(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
		{ID: "b", Model: "model-b", APIBase: "mock://b", Capacity: 1},
		{ID: "c", Model: "model-c", APIBase: "mock://c", Capacity: 1},
	}}}}
	var mu sync.Mutex
	var order []string
	track := func(id string) func(call int, req llmr.InferRequest) mockResponse {
		return func(call int, req llmr.InferRequest) mockResponse {
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			return mockResponse{err: errors.New("fail " + id)}
		}
	}
	clients := map[string]*mockClient{
		"a": {respond: track("a")},
		"b": {respond: track("b")},
		"c": {respond: track("c")},
	}
	o := newOrchestrator(cfg, clients)

	maxRetries := 2
	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, MaxRetries: &maxRetries})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusFailed {
		t.Fatalf("expected job to fail after exhausting all models, got %s", job.Status)
	}

	seen := map[string]int{}
	for _, id := range order {
		seen[id]++
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("expected each model tried exactly once, model %s was tried %d times (order=%v)", id, count, order)
		}
	}
	if len(order) != 3 {
		t.Fatalf("expected exactly 3 attempts across 3 distinct models, got %v", order)
	}
}

func TestRetryAttemptsAreCappedAtTheConfiguredMaxRetries(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
		{ID: "b", Model: "model-b", APIBase: "mock://b", Capacity: 1},
		{ID: "c", Model: "model-c", APIBase: "mock://c", Capacity: 1},
		{ID: "d", Model: "model-d", APIBase: "mock://d", Capacity: 1},
	}}}}
	clients := map[string]*mockClient{
		"a": {responses: []mockResponse{{err: errors.New("fail")}}},
		"b": {responses: []mockResponse{{err: errors.New("fail")}}},
		"c": {responses: []mockResponse{{err: errors.New("fail")}}},
		"d": {responses: []mockResponse{{err: errors.New("fail")}}},
	}
	o := newOrchestrator(cfg, clients)

	maxRetries := 1
	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, MaxRetries: &maxRetries})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusFailed {
		t.Fatalf("expected job to fail, got %s", job.Status)
	}

	total := clients["a"].Calls() + clients["b"].Calls() + clients["c"].Calls() + clients["d"].Calls()
	if total != maxRetries+1 {
		t.Fatalf("expected exactly max_retries+1=%d total attempts, got %d", maxRetries+1, total)
	}
}

func TestOversizedPromptOnlyRoutesToDeploymentsWithSufficientContextWindow(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "small", Model: "model-small", APIBase: "mock://small", Capacity: 1, MaxContextTokens: 10},
		{ID: "large", Model: "model-large", APIBase: "mock://large", Capacity: 1, MaxContextTokens: 1000},
	}}}}
	clients := map[string]*mockClient{
		"small": {responses: []mockResponse{{text: "ok"}}},
		"large": {responses: []mockResponse{{text: "ok"}}},
	}
	o := newOrchestrator(cfg, clients)

	bigPrompt := make([]byte, 400)
	for i := range bigPrompt {
		bigPrompt[i] = 'x'
	}
	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: string(bigPrompt)}})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusDone {
		t.Fatalf("expected oversized-for-small prompt to still succeed on large, got %s: %s", job.Status, job.FailureReason)
	}
	if job.DeploymentID != "large" {
		t.Fatalf("expected prompt exceeding small's context to route only to large, got %s", job.DeploymentID)
	}
	if clients["small"].Calls() != 0 {
		t.Fatalf("expected small deployment to never be tried, got %d calls", clients["small"].Calls())
	}

	hugePrompt := make([]byte, 100000)
	for i := range hugePrompt {
		hugePrompt[i] = 'x'
	}
	job2, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: string(hugePrompt)}})
	if err == nil && job2.Status != llmr.StatusFailed {
		t.Fatalf("expected a prompt exceeding every deployment's context to fail immediately, got status=%s err=%v", job2.Status, err)
	}
}

func TestDeploymentWithCapacityGreaterThanOneServesMultipleJobsConcurrently(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 2, TokensPerSecond: 50},
	}}}}
	gate := make(chan struct{})
	release := make(chan struct{})
	var entered sync.WaitGroup
	entered.Add(2)
	var once sync.Once

	clients := map[string]*mockClient{
		"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
			entered.Done()
			once.Do(func() { close(gate) })
			<-release
			return mockResponse{text: "ok"}
		}},
	}
	o := newOrchestrator(cfg, clients)

	var wg sync.WaitGroup
	results := make([]*llmr.Job, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
			if err != nil {
				t.Errorf("SubmitResponse: %v", err)
				return
			}
			results[i] = job
		}(i)
	}

	done := make(chan struct{})
	go func() {
		entered.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for both jobs to be accepted concurrently onto the capacity=2 deployment")
	}

	close(release)
	wg.Wait()

	for _, j := range results {
		if j == nil || j.Status != llmr.StatusDone {
			t.Fatalf("expected both jobs to complete successfully: %+v", j)
		}
	}
}

func TestPinnedDeploymentIsUsedEvenWhenAFasterDeploymentIsIdle(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "slow", Model: "model-slow", APIBase: "mock://slow", Capacity: 1, TokensPerSecond: 10},
		{ID: "fast", Model: "model-fast", APIBase: "mock://fast", Capacity: 1, TokensPerSecond: 1000},
	}}}}
	clients := map[string]*mockClient{
		"slow": {responses: []mockResponse{{text: "ok"}}},
		"fast": {responses: []mockResponse{{text: "ok"}}},
	}
	o := newOrchestrator(cfg, clients)

	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}, PinnedDeploymentID: "slow"})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.DeploymentID != "slow" {
		t.Fatalf("expected pinned deployment 'slow' to be used despite idle faster alternative, got %s", job.DeploymentID)
	}
	if clients["fast"].Calls() != 0 {
		t.Fatalf("expected the faster unpinned deployment to never be called, got %d calls", clients["fast"].Calls())
	}
}

func TestBusyDeploymentIsPreferredOnlyWhenItClearsTheMarginThreshold(t *testing.T) {
	t.Run("busy deployment preferred when it clears the margin", func(t *testing.T) {
		cfg := twoModelConfig("pool1", 1000, 10)
		release := make(chan struct{})
		clients := map[string]*mockClient{
			"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
				<-release
				return mockResponse{text: "first", tokens: 1}
			}},
			"b": {tokensPerSec: 10},
		}
		o := newOrchestrator(cfg, clients)

		longPrompt := strings.Repeat("hello world ", 50)

		firstDone := make(chan *llmr.Job, 1)
		go func() {
			job, _ := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: longPrompt}})
			firstDone <- job
		}()
		time.Sleep(50 * time.Millisecond)

		secondDone := make(chan *llmr.Job, 1)
		go func() {
			job, _ := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: longPrompt}})
			secondDone <- job
		}()
		time.Sleep(50 * time.Millisecond)
		close(release)

		first := <-firstDone
		second := <-secondDone
		if first.DeploymentID != "a" {
			t.Fatalf("expected first job on a, got %s", first.DeploymentID)
		}
		if second.DeploymentID != "a" {
			t.Fatalf("expected second job to wait for the much-faster busy deployment a, got %s", second.DeploymentID)
		}
	})

	t.Run("idle deployment preferred when busy falls short of the margin", func(t *testing.T) {
		cfg := twoModelConfig("pool1", 11, 10)
		release := make(chan struct{})
		clients := map[string]*mockClient{
			"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
				<-release
				return mockResponse{text: "first", tokens: 1}
			}},
			"b": {tokensPerSec: 10},
		}
		o := newOrchestrator(cfg, clients)

		longPrompt := strings.Repeat("hello world ", 50)

		firstDone := make(chan *llmr.Job, 1)
		go func() {
			job, _ := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: longPrompt}})
			firstDone <- job
		}()
		time.Sleep(50 * time.Millisecond)

		second, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: longPrompt}})
		if err != nil {
			t.Fatalf("SubmitResponse: %v", err)
		}
		close(release)
		<-firstDone

		if second.DeploymentID != "b" {
			t.Fatalf("expected second job to take the idle slot b when margin isn't cleared, got %s", second.DeploymentID)
		}
	})
}

func TestIdleDeploymentWithUnknownSpeedIsUsedRatherThanWaitingOnABusyUnknownDeployment(t *testing.T) {
	// Two brand-new deployments, neither with a learned tokens_per_second
	// (both 0, as when freshly added via `llmr add` and never yet used).
	// Deployment a is held busy; b sits idle. Since neither side of the
	// busy-vs-idle margin comparison has real data, b must still be used
	// rather than the comparison degenerating into a 0<=0 tie that always
	// favors waiting on the busy deployment.
	cfg := twoModelConfig("pool1", 0, 0)
	release := make(chan struct{})
	clients := map[string]*mockClient{
		"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
			<-release
			return mockResponse{text: "ok"}
		}},
		"b": {responses: []mockResponse{{text: "ok"}}},
	}
	o := newOrchestrator(cfg, clients)

	firstDone := make(chan *llmr.Job, 1)
	go func() {
		job, _ := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
		firstDone <- job
	}()
	time.Sleep(50 * time.Millisecond)

	secondDone := make(chan *llmr.Job, 1)
	go func() {
		job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
		if err != nil {
			t.Errorf("SubmitResponse: %v", err)
		}
		secondDone <- job
	}()

	select {
	case second := <-secondDone:
		if second.DeploymentID != "b" {
			t.Fatalf("expected the second job to use idle b rather than wait on busy a, got %s", second.DeploymentID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second job never completed: it appears stuck waiting on the busy deployment instead of using the idle one")
	}

	close(release)
	<-firstDone
}
