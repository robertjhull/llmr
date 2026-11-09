package llmr

import (
	"context"
	"encoding/json"
	"time"
)

type JobStatus string

const (
	StatusQueued        JobStatus = "queued"
	StatusInferring     JobStatus = "inferring"
	StatusRetrying      JobStatus = "retrying"
	StatusStreaming     JobStatus = "streaming"
	StatusRequestedTool JobStatus = "requested tool"
	StatusIdle          JobStatus = "idle"
	StatusDone          JobStatus = "done"
	StatusFailed        JobStatus = "failed"
	StatusCancelled     JobStatus = "cancelled"
	StatusCooldown      JobStatus = "cooldown"
)

type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
}

type ToolResultInput struct {
	ToolCallID string `json:"tool_call_id"`
	ToolOutput string `json:"tool_output"`
}

type ResponseInput struct {
	Text        string            `json:"text,omitempty"`
	ToolCallID  string            `json:"tool_call_id,omitempty"`
	ToolOutput  string            `json:"tool_output,omitempty"`
	ToolResults []ToolResultInput `json:"tool_results,omitempty"`
}

type ResponseRequest struct {
	Pool                string          `json:"pool"`
	Input               ResponseInput   `json:"input"`
	Tools               []Tool          `json:"tools,omitempty"`
	ToolChoice          string          `json:"tool_choice,omitempty"`
	PreviousResponseID  string          `json:"previous_response_id,omitempty"`
	PinnedDeploymentID  string          `json:"pinned_deployment_id,omitempty"`
	MaxRetries          *int            `json:"max_retries,omitempty"`
	MaxToolRounds       *int            `json:"max_tool_rounds,omitempty"`
	FinalResponseFormat json.RawMessage `json:"final_response_format,omitempty"`
	Skills              []string        `json:"skills,omitempty"`
}

type BatchItemRequest struct {
	Input ResponseInput `json:"input"`
}

type BatchRequest struct {
	Pool                string             `json:"pool"`
	Tools               []Tool             `json:"tools,omitempty"`
	ToolChoice          string             `json:"tool_choice,omitempty"`
	Skills              []string           `json:"skills,omitempty"`
	FinalResponseFormat json.RawMessage    `json:"final_response_format,omitempty"`
	MaxRetries          *int               `json:"max_retries,omitempty"`
	MaxToolRounds       *int               `json:"max_tool_rounds,omitempty"`
	Items               []BatchItemRequest `json:"items"`
}

type Job struct {
	ID                    string
	Pool                  string
	Status                JobStatus
	PinnedDeploymentID    string
	PreferredDeploymentID string
	DeploymentID          string
	History               []Message
	Tools                 []Tool
	ToolChoice            string
	SkillInstructions     string
	FinalResponseFormat   *CompiledSchema
	MaxRetries            int
	MaxToolRounds         int
	ToolRounds            int
	Attempts              int
	FailedDeploymentIDs   []string
	PendingToolCalls      []ToolCall
	Reasoning             string
	StreamPreview         string
	Result                string
	FailureReason         string
	IsBatchItem           bool
	SubmittedAt           time.Time
	StartedAt             time.Time

	cancel context.CancelFunc
}

type BatchItem struct {
	ID  string
	Job *Job
}

type Batch struct {
	ID          string
	Pool        string
	Items       []*BatchItem
	Status      JobStatus
	SubmittedAt time.Time
}

func (b *Batch) ComputedStatus() JobStatus {
	if b.Status == StatusCancelled {
		return StatusCancelled
	}
	for _, item := range b.Items {
		switch item.Job.Status {
		case StatusDone, StatusFailed, StatusCancelled:
		default:
			return StatusInferring
		}
	}
	return StatusDone
}
