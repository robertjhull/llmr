package llmr

import (
	"encoding/json"
	"net/http"
)

type API struct {
	orchestrator *Orchestrator
	status       *StatusService
}

func NewAPI(o *Orchestrator, s *StatusService) *API {
	return &API{orchestrator: o, status: s}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (a *API) HandleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) HandleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.status.Snapshot(r.Host))
}

func jobToResponsesAPI(job *Job) ResponsesAPIResponse {
	return ResponsesAPIResponse{
		ID:               job.ID,
		Status:           string(job.Status),
		Output:           job.Result,
		Error:            job.FailureReason,
		ModelID:          job.DeploymentID,
		PendingToolCalls: job.PendingToolCalls,
		Reasoning:        job.Reasoning,
	}
}

func (a *API) HandleSubmitResponse(w http.ResponseWriter, r *http.Request) {
	var req ResponseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	job, err := a.orchestrator.SubmitResponse(req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	if job.IsBatchItem {
		writeJSON(w, http.StatusAccepted, map[string]string{"id": job.ID, "status": "queued"})
		return
	}

	writeJSON(w, http.StatusOK, jobToResponsesAPI(job))
}

func (a *API) HandleGetResponse(w http.ResponseWriter, r *http.Request) {
	job, err := a.orchestrator.GetJob(r.PathValue("id"))
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, jobToResponsesAPI(job))
}

func (a *API) HandleSubmitBatch(w http.ResponseWriter, r *http.Request) {
	var req BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	batch, err := a.orchestrator.SubmitBatch(req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"id": batch.ID, "status": "queued"})
}

type ItemView struct {
	ID               string     `json:"id"`
	Status           string     `json:"status"`
	Result           string     `json:"result,omitempty"`
	Error            string     `json:"error,omitempty"`
	ModelID          string     `json:"model_id,omitempty"`
	PendingToolCalls []ToolCall `json:"pending_tool_calls,omitempty"`
	Reasoning        string     `json:"reasoning,omitempty"`
}

type JobView struct {
	ID               string     `json:"id"`
	Pool             string     `json:"pool"`
	Status           string     `json:"status"`
	Result           string     `json:"result,omitempty"`
	Error            string     `json:"error,omitempty"`
	ModelID          string     `json:"model_id,omitempty"`
	PendingToolCalls []ToolCall `json:"pending_tool_calls,omitempty"`
	Reasoning        string     `json:"reasoning,omitempty"`
	Items            []ItemView `json:"items,omitempty"`
}

func jobView(job *Job) JobView {
	return JobView{
		ID:               job.ID,
		Pool:             job.Pool,
		Status:           string(job.Status),
		Result:           job.Result,
		Error:            job.FailureReason,
		ModelID:          job.DeploymentID,
		PendingToolCalls: job.PendingToolCalls,
		Reasoning:        job.Reasoning,
	}
}

func batchView(batch *Batch) JobView {
	view := JobView{
		ID:     batch.ID,
		Pool:   batch.Pool,
		Status: string(batch.ComputedStatus()),
	}
	for _, item := range batch.Items {
		view.Items = append(view.Items, ItemView{
			ID:               item.ID,
			Status:           string(item.Job.Status),
			Result:           item.Job.Result,
			Error:            item.Job.FailureReason,
			ModelID:          item.Job.DeploymentID,
			PendingToolCalls: item.Job.PendingToolCalls,
			Reasoning:        item.Job.Reasoning,
		})
	}
	return view
}

func (a *API) HandleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if batch, err := a.orchestrator.GetBatch(id); err == nil {
		writeJSON(w, http.StatusOK, batchView(batch))
		return
	}

	job, err := a.orchestrator.GetJob(id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, jobView(job))
}

func (a *API) HandleCancelJob(w http.ResponseWriter, r *http.Request) {
	if err := a.orchestrator.CancelJob(r.PathValue("id")); err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

type addDeploymentRequest struct {
	Model            string `json:"model"`
	APIBase          string `json:"api_base"`
	APIKey           string `json:"api_key,omitempty"`
	APISpec          string `json:"api_spec,omitempty"`
	MaxContextTokens int    `json:"max_context_tokens,omitempty"`
	Capacity         int    `json:"capacity,omitempty"`
}

func (a *API) HandleAddDeployment(w http.ResponseWriter, r *http.Request) {
	var req addDeploymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	added, err := a.orchestrator.AddDeployment(r.PathValue("pool"), Deployment{
		Model:            req.Model,
		APIBase:          req.APIBase,
		APIKey:           req.APIKey,
		APISpec:          req.APISpec,
		MaxContextTokens: req.MaxContextTokens,
		Capacity:         req.Capacity,
	})
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, added)
}
