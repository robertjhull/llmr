package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	llmr "llm-request-router/src"
)

func doDelete(t *testing.T, url string) *http.Response {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		t.Fatalf("building DELETE request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", url, err)
	}
	return resp
}

func TestDeletingAnUnknownOrAlreadyFinishedJobReturnsAnError(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	o := newOrchestrator(cfg, map[string]*mockClient{"a": {responses: []mockResponse{{text: "ok"}}}})
	srv := llmr.NewServerFromOrchestrator(llmr.ServerConfig{}, o)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := doDelete(t, ts.URL+"/jobs/does-not-exist")
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("expected an error deleting an unknown job, got %d", resp.StatusCode)
	}

	job, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
	if err != nil || job.Status != llmr.StatusDone {
		t.Fatalf("priming request failed: err=%v job=%+v", err, job)
	}

	resp2 := doDelete(t, ts.URL+"/jobs/"+job.ID)
	defer resp2.Body.Close()
	if resp2.StatusCode == http.StatusOK {
		t.Fatalf("expected an error deleting an already-finished job, got %d", resp2.StatusCode)
	}
}

func TestFetchingAnUnknownJobIDReturnsAnError(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	o := newOrchestrator(cfg, map[string]*mockClient{"a": {}})
	srv := llmr.NewServerFromOrchestrator(llmr.ServerConfig{}, o)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/jobs/does-not-exist")
	if err != nil {
		t.Fatalf("GET /jobs/{id}: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("expected an error status for an unknown job id, got %d", resp.StatusCode)
	}
}

func TestCancellingABatchJobCancelsItImmediatelyInFull(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	release := make(chan struct{})
	clients := map[string]*mockClient{"a": {respond: func(call int, req llmr.InferRequest) mockResponse {
		<-release
		return mockResponse{text: "ok"}
	}}}
	o := newOrchestrator(cfg, clients)

	batch, err := o.SubmitBatch(llmr.BatchRequest{Pool: "pool1", Items: []llmr.BatchItemRequest{
		{Input: llmr.ResponseInput{Text: "1"}},
		{Input: llmr.ResponseInput{Text: "2"}},
		{Input: llmr.ResponseInput{Text: "3"}},
	}})
	if err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := o.CancelJob(batch.ID); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	gotBatch, err := o.GetBatch(batch.ID)
	if err != nil {
		t.Fatalf("GetBatch: %v", err)
	}
	if gotBatch.ComputedStatus() != llmr.StatusCancelled {
		t.Fatalf("expected batch status cancelled, got %s", gotBatch.ComputedStatus())
	}
	for _, item := range gotBatch.Items {
		if item.Job.Status != llmr.StatusCancelled {
			t.Fatalf("expected item %s cancelled, got %s", item.ID, item.Job.Status)
		}
	}

	snap := o.PoolsSnapshot()
	if snap[0].Queued != 0 {
		t.Fatalf("expected queue to be drained after batch cancel, got %d", snap[0].Queued)
	}

	close(release)
}

func TestRequestToAnUnknownPoolReturnsAnError(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	o := newOrchestrator(cfg, map[string]*mockClient{"a": {}})

	if _, err := o.SubmitResponse(llmr.ResponseRequest{Pool: "does-not-exist", Input: llmr.ResponseInput{Text: "hi"}}); err == nil {
		t.Fatal("expected an error submitting a single response to an unknown pool")
	}
	if _, err := o.SubmitBatch(llmr.BatchRequest{Pool: "does-not-exist", Items: []llmr.BatchItemRequest{{Input: llmr.ResponseInput{Text: "hi"}}}}); err == nil {
		t.Fatal("expected an error submitting a batch to an unknown pool")
	}
}

func TestAddedDeploymentPersistsAcrossConfigReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pools.json")
	cfg, err := llmr.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	o1 := llmr.NewOrchestrator(cfg)

	added, err := o1.AddDeployment("pool1", llmr.Deployment{Model: "model-a", APIBase: "http://localhost:9999/v1", Capacity: 1})
	if err != nil {
		t.Fatalf("AddDeployment: %v", err)
	}

	cfg2, err := llmr.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig (reload): %v", err)
	}
	o2 := llmr.NewOrchestrator(cfg2)
	o2.ClientFactory = clientFactoryFor(map[string]*mockClient{added.ID: {responses: []mockResponse{{text: "ok"}}}})

	job, err := o2.SubmitResponse(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
	if err != nil {
		t.Fatalf("SubmitResponse: %v", err)
	}
	if job.Status != llmr.StatusDone || job.DeploymentID != added.ID {
		t.Fatalf("expected the added deployment to be routable after reload without re-adding, got status=%s deployment=%s", job.Status, job.DeploymentID)
	}
}

func TestGetV1ResponsesIDAndGetJobsIDReturnDistinctShapesForTheSameJob(t *testing.T) {
	cfg := &llmr.Config{Pools: []llmr.Pool{{Name: "pool1", Deployments: []llmr.Deployment{
		{ID: "a", Model: "model-a", APIBase: "mock://a", Capacity: 1},
	}}}}
	o := newOrchestrator(cfg, map[string]*mockClient{"a": {responses: []mockResponse{{text: "hello"}}}})
	srv := llmr.NewServerFromOrchestrator(llmr.ServerConfig{}, o)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body, _ := json.Marshal(llmr.ResponseRequest{Pool: "pool1", Input: llmr.ResponseInput{Text: "hi"}})
	resp, err := http.Post(ts.URL+"/v1/responses", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/responses: %v", err)
	}
	defer resp.Body.Close()
	var submitResp map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&submitResp); err != nil {
		t.Fatalf("decoding submit response: %v", err)
	}
	id, _ := submitResp["id"].(string)
	if id == "" {
		t.Fatalf("expected an id in the /v1/responses body, got %v", submitResp)
	}

	respResp, err := http.Get(ts.URL + "/v1/responses/" + id)
	if err != nil {
		t.Fatalf("GET /v1/responses/{id}: %v", err)
	}
	defer respResp.Body.Close()
	var responsesShape map[string]any
	json.NewDecoder(respResp.Body).Decode(&responsesShape)
	if _, hasOutput := responsesShape["output"]; !hasOutput {
		t.Fatalf("expected the Responses-shaped body to carry an 'output' field, got %v", responsesShape)
	}
	if _, hasPool := responsesShape["pool"]; hasPool {
		t.Fatalf("did not expect the Responses-shaped body to carry llmr's 'pool' field, got %v", responsesShape)
	}

	jobResp, err := http.Get(ts.URL + "/jobs/" + id)
	if err != nil {
		t.Fatalf("GET /jobs/{id}: %v", err)
	}
	defer jobResp.Body.Close()
	var jobShape map[string]any
	json.NewDecoder(jobResp.Body).Decode(&jobShape)
	if _, hasPool := jobShape["pool"]; !hasPool {
		t.Fatalf("expected the llmr job-shaped body to carry a 'pool' field, got %v", jobShape)
	}
	if _, hasOutput := jobShape["output"]; hasOutput {
		t.Fatalf("did not expect the llmr job-shaped body to carry the Responses API's 'output' field, got %v", jobShape)
	}
}

func TestAddingADeploymentWithAMissingRequiredFieldReturnsAValidationError(t *testing.T) {
	cfg := &llmr.Config{}
	o := llmr.NewOrchestrator(cfg)

	if _, err := o.AddDeployment("pool1", llmr.Deployment{APIBase: "http://x"}); err == nil {
		t.Fatal("expected a validation error when model is missing")
	}
	if _, err := o.AddDeployment("pool1", llmr.Deployment{Model: "model-a"}); err == nil {
		t.Fatal("expected a validation error when api-base is missing")
	}
	if _, err := o.AddDeployment("", llmr.Deployment{Model: "model-a", APIBase: "http://x"}); err == nil {
		t.Fatal("expected a validation error when pool is missing")
	}
	if len(cfg.Pools) != 0 {
		t.Fatalf("expected no deployment to be persisted after validation failures, got pools=%+v", cfg.Pools)
	}
}

func TestHealthEndpointReportsLiveness(t *testing.T) {
	// Assert GET /health returns 200 while the process is up, independent
	// of /status's richer pool/deployment data.
	configPath := filepath.Join(t.TempDir(), "pools.json")
	srv, err := llmr.NewServer(llmr.ServerConfig{Host: "127.0.0.1", Port: 0, ConfigPath: configPath})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}
