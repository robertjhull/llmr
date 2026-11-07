package llmr

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CooldownFailureThreshold = 3
	BusyIdleMargin           = 0.15
	DefaultMaxRetries        = 2
	DefaultMaxToolRounds     = 5
	MaxStoredJobs            = 100
	MetricSmoothingAlpha     = 0.3
)

// CooldownDuration is a var (not const) so tests can shrink it; production
// code never changes it from the spec's hardcoded 30s default.
var CooldownDuration = 30 * time.Second

type ActiveSlot struct {
	JobID            string
	StartedAt        time.Time
	ExpectedDuration time.Duration
}

type DeploymentState struct {
	Config        Deployment
	AvgDuration   time.Duration
	FailureCount  int
	CooldownUntil time.Time
	ActiveSlots   []ActiveSlot
}

type QueuedRequest struct {
	Job             *Job
	EstimatedTokens int
	EnqueuedAt      time.Time
	deployment      *DeploymentState
	err             error
}

type PoolState struct {
	Name        string
	Deployments []*DeploymentState
	Queue       []*QueuedRequest
}

type Orchestrator struct {
	mu            sync.Mutex
	cond          *sync.Cond
	config        *Config
	pools         map[string]*PoolState
	jobs          map[string]*Job
	jobOrder      []string
	batches       map[string]*Batch
	totalRequests atomic.Int64

	ClientFactory func(Deployment) (DeploymentClient, error)
}

func NewOrchestrator(cfg *Config) *Orchestrator {
	o := &Orchestrator{
		config:        cfg,
		pools:         make(map[string]*PoolState),
		jobs:          make(map[string]*Job),
		batches:       make(map[string]*Batch),
		ClientFactory: NewDeploymentClient,
	}
	o.cond = sync.NewCond(&o.mu)
	for _, p := range cfg.Pools {
		o.pools[p.Name] = newPoolState(p)
	}

	go o.cooldownTicker()

	return o
}

func newPoolState(p Pool) *PoolState {
	ps := &PoolState{Name: p.Name}
	for _, d := range p.Deployments {
		ps.Deployments = append(ps.Deployments, &DeploymentState{Config: d})
	}
	return ps
}

func (o *Orchestrator) cooldownTicker() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		o.mu.Lock()
		o.cond.Broadcast()
		o.mu.Unlock()
	}
}

func (o *Orchestrator) TotalRequests() int64 {
	return o.totalRequests.Load()
}

func newJobID() string {
	return newDeploymentID()
}

func (o *Orchestrator) estimateJobTokens(job *Job) int {
	total := EstimateTokens(job.SkillInstructions)
	for _, m := range job.History {
		total += EstimateTokens(m.Content)
	}
	for _, t := range job.Tools {
		total += EstimateTokens(t.Name) + EstimateTokens(t.Description)
	}
	return total
}

func (o *Orchestrator) resolveJob(req ResponseRequest) (*Job, bool, error) {
	if req.PreviousResponseID != "" {
		job, ok := o.jobs[req.PreviousResponseID]
		if !ok {
			return nil, false, fmt.Errorf("unknown response id: %s", req.PreviousResponseID)
		}
		if req.Input.ToolCallID != "" {
			job.History = append(job.History, Message{Role: "tool", ToolCallID: req.Input.ToolCallID, Content: req.Input.ToolOutput})
		} else {
			job.History = append(job.History, Message{Role: "user", Content: req.Input.Text})
		}
		job.PendingToolCalls = nil
		job.FailedDeploymentIDs = nil
		job.Attempts = 0
		if !job.IsBatchItem {
			job.PinnedDeploymentID = job.DeploymentID
		} else {
			job.PinnedDeploymentID = ""
		}
		job.Status = StatusQueued
		return job, false, nil
	}

	job := &Job{
		ID:                 newJobID(),
		Pool:               req.Pool,
		PinnedDeploymentID: req.PinnedDeploymentID,
		Status:             StatusQueued,
		History:            []Message{{Role: "user", Content: req.Input.Text}},
		Tools:              req.Tools,
		ToolChoice:         req.ToolChoice,
		MaxRetries:         DefaultMaxRetries,
		MaxToolRounds:      DefaultMaxToolRounds,
		SubmittedAt:        time.Now(),
	}
	if req.MaxRetries != nil {
		job.MaxRetries = *req.MaxRetries
	}
	if req.MaxToolRounds != nil {
		job.MaxToolRounds = *req.MaxToolRounds
	}

	if len(req.Skills) > 0 {
		tools, instructions, err := ExpandSkills(o.config, req.Skills)
		if err != nil {
			return nil, true, err
		}
		job.Tools = append(job.Tools, tools...)
		job.SkillInstructions = instructions
	}

	if len(req.FinalResponseFormat) > 0 {
		compiled, err := CompileSchema(req.FinalResponseFormat)
		if err != nil {
			return nil, true, err
		}
		job.FinalResponseFormat = compiled
	}

	return job, true, nil
}

func (o *Orchestrator) SubmitResponse(req ResponseRequest) (*Job, error) {
	o.mu.Lock()
	pool, ok := o.pools[req.Pool]
	if !ok {
		o.mu.Unlock()
		return nil, fmt.Errorf("unknown pool: %s", req.Pool)
	}

	job, isNew, err := o.resolveJob(req)
	if err != nil {
		o.mu.Unlock()
		return nil, err
	}
	if isNew {
		o.registerJobLocked(job)
	}
	o.mu.Unlock()

	o.runJob(pool, job)
	return job, nil
}

func (o *Orchestrator) SubmitBatch(req BatchRequest) (*Batch, error) {
	o.mu.Lock()
	pool, ok := o.pools[req.Pool]
	if !ok {
		o.mu.Unlock()
		return nil, fmt.Errorf("unknown pool: %s", req.Pool)
	}

	var tools []Tool
	var skillInstructions string
	if len(req.Skills) > 0 {
		expanded, instructions, err := ExpandSkills(o.config, req.Skills)
		if err != nil {
			o.mu.Unlock()
			return nil, err
		}
		tools = expanded
		skillInstructions = instructions
	}
	tools = append(tools, req.Tools...)

	var schema *CompiledSchema
	if len(req.FinalResponseFormat) > 0 {
		compiled, err := CompileSchema(req.FinalResponseFormat)
		if err != nil {
			o.mu.Unlock()
			return nil, err
		}
		schema = compiled
	}

	maxRetries := DefaultMaxRetries
	if req.MaxRetries != nil {
		maxRetries = *req.MaxRetries
	}
	maxToolRounds := DefaultMaxToolRounds
	if req.MaxToolRounds != nil {
		maxToolRounds = *req.MaxToolRounds
	}

	batch := &Batch{ID: newJobID(), Pool: req.Pool, SubmittedAt: time.Now()}
	for _, item := range req.Items {
		job := &Job{
			ID:                  newJobID(),
			Pool:                req.Pool,
			Status:              StatusQueued,
			History:             []Message{{Role: "user", Content: item.Input.Text}},
			Tools:               tools,
			ToolChoice:          req.ToolChoice,
			SkillInstructions:   skillInstructions,
			FinalResponseFormat: schema,
			MaxRetries:          maxRetries,
			MaxToolRounds:       maxToolRounds,
			IsBatchItem:         true,
			SubmittedAt:         time.Now(),
		}
		o.registerJobLocked(job)
		batch.Items = append(batch.Items, &BatchItem{ID: job.ID, Job: job})
	}
	o.batches[batch.ID] = batch
	o.mu.Unlock()

	for _, item := range batch.Items {
		go o.runJob(pool, item.Job)
	}

	return batch, nil
}

func (o *Orchestrator) registerJobLocked(job *Job) {
	o.jobs[job.ID] = job
	o.jobOrder = append(o.jobOrder, job.ID)
	if len(o.jobOrder) > MaxStoredJobs {
		evictID := o.jobOrder[0]
		if evictJob := o.jobs[evictID]; evictJob != nil {
			switch evictJob.Status {
			case StatusDone, StatusFailed, StatusCancelled:
				delete(o.jobs, evictID)
				o.jobOrder = o.jobOrder[1:]
			}
		}
	}
}

func (o *Orchestrator) GetJob(id string) (*Job, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	job, ok := o.jobs[id]
	if !ok {
		return nil, fmt.Errorf("unknown job id: %s", id)
	}
	return job, nil
}

func (o *Orchestrator) GetBatch(id string) (*Batch, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	batch, ok := o.batches[id]
	if !ok {
		return nil, fmt.Errorf("unknown job id: %s", id)
	}
	return batch, nil
}

func (o *Orchestrator) cancelJobLocked(job *Job) error {
	switch job.Status {
	case StatusDone, StatusFailed, StatusCancelled:
		return fmt.Errorf("job %s has already finished", job.ID)
	}
	job.Status = StatusCancelled
	if job.cancel != nil {
		job.cancel()
	}
	if pool, ok := o.pools[job.Pool]; ok {
		filtered := pool.Queue[:0]
		for _, qr := range pool.Queue {
			if qr.Job.ID != job.ID {
				filtered = append(filtered, qr)
			}
		}
		pool.Queue = filtered
	}
	return nil
}

func (o *Orchestrator) CancelJob(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if job, ok := o.jobs[id]; ok {
		err := o.cancelJobLocked(job)
		o.cond.Broadcast()
		return err
	}
	if batch, ok := o.batches[id]; ok {
		if batch.Status == StatusCancelled || batch.ComputedStatus() != StatusInferring {
			return fmt.Errorf("job %s has already finished", id)
		}
		for _, item := range batch.Items {
			_ = o.cancelJobLocked(item.Job)
		}
		batch.Status = StatusCancelled
		o.cond.Broadcast()
		return nil
	}
	return fmt.Errorf("unknown job id: %s", id)
}

func (o *Orchestrator) AddDeployment(poolName string, d Deployment) (Deployment, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	added, err := o.config.AddDeployment(poolName, d)
	if err != nil {
		return Deployment{}, err
	}

	pool, ok := o.pools[poolName]
	if !ok {
		pool = &PoolState{Name: poolName}
		o.pools[poolName] = pool
	}
	pool.Deployments = append(pool.Deployments, &DeploymentState{Config: added})
	o.cond.Broadcast()

	return added, nil
}

func (o *Orchestrator) PoolsSnapshot() []PoolStatus {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := time.Now()
	pools := make([]PoolStatus, 0, len(o.pools))
	for _, p := range o.pools {
		models := make([]DeploymentStatus, 0, len(p.Deployments))
		for _, d := range p.Deployments {
			models = append(models, o.deploymentStatusLocked(d, now))
		}
		pools = append(pools, PoolStatus{Name: p.Name, Queued: len(p.Queue), Models: models})
	}
	return pools
}

func (o *Orchestrator) deploymentStatusLocked(d *DeploymentState, now time.Time) DeploymentStatus {
	var tps *float64
	if d.Config.TokensPerSecond > 0 {
		v := d.Config.TokensPerSecond
		tps = &v
	}

	status := DeploymentStatus{
		ID:              d.Config.ID,
		Model:           d.Config.Model,
		APIBase:         d.Config.APIBase,
		TokensPerSecond: tps,
	}

	if d.InCooldown(now) {
		status.Status = string(StatusCooldown)
		return status
	}

	if len(d.ActiveSlots) == 0 {
		status.Status = string(StatusIdle)
		return status
	}

	var longest time.Duration
	var jobStatus JobStatus = StatusInferring
	for _, slot := range d.ActiveSlots {
		elapsed := now.Sub(slot.StartedAt)
		if elapsed > longest {
			longest = elapsed
		}
		if job, ok := o.jobs[slot.JobID]; ok {
			jobStatus = job.Status
		}
	}
	minutes := int(longest.Minutes())
	status.ElapsedMinutes = &minutes
	status.Status = string(jobStatus)
	return status
}

func (o *Orchestrator) acquireSlot(pool *PoolState, job *Job, estimatedTokens int) (*DeploymentState, error) {
	qr := &QueuedRequest{Job: job, EstimatedTokens: estimatedTokens, EnqueuedAt: time.Now()}

	o.mu.Lock()
	pool.Queue = append(pool.Queue, qr)
	o.cond.Broadcast()

	for {
		o.scheduleLocked(pool)

		if qr.deployment != nil {
			d := qr.deployment
			o.mu.Unlock()
			return d, nil
		}
		if qr.err != nil {
			err := qr.err
			o.mu.Unlock()
			return nil, err
		}
		if job.Status == StatusCancelled {
			o.removeFromQueueLocked(pool, qr)
			o.mu.Unlock()
			return nil, fmt.Errorf("job cancelled")
		}

		o.cond.Wait()
	}
}

func (o *Orchestrator) removeFromQueueLocked(pool *PoolState, target *QueuedRequest) {
	filtered := pool.Queue[:0]
	for _, qr := range pool.Queue {
		if qr != target {
			filtered = append(filtered, qr)
		}
	}
	pool.Queue = filtered
}

func (o *Orchestrator) scheduleLocked(pool *PoolState) {
	now := time.Now()
	for len(pool.Queue) > 0 {
		qr := pool.Queue[0]

		if qr.Job.Status == StatusCancelled {
			pool.Queue = pool.Queue[1:]
			continue
		}

		d, err := SelectDeployment(pool, qr.EstimatedTokens, qr.Job.PinnedDeploymentID, qr.Job.FailedDeploymentIDs, now)
		if err != nil {
			if err == errPromptTooLarge || err == errUnknownDeployment || err == errAllDeploymentsExhausted {
				qr.err = err
				pool.Queue = pool.Queue[1:]
				continue
			}
			return
		}

		d.ActiveSlots = append(d.ActiveSlots, ActiveSlot{
			JobID:            qr.Job.ID,
			StartedAt:        now,
			ExpectedDuration: d.estimatedAvgDuration(qr.EstimatedTokens),
		})
		qr.deployment = d
		pool.Queue = pool.Queue[1:]
	}
}

func (o *Orchestrator) releaseSlotLocked(d *DeploymentState, jobID string) {
	filtered := d.ActiveSlots[:0]
	for _, s := range d.ActiveSlots {
		if s.JobID != jobID {
			filtered = append(filtered, s)
		}
	}
	d.ActiveSlots = filtered
}

func (o *Orchestrator) recordFailureLocked(d *DeploymentState) {
	d.FailureCount++
	if d.FailureCount >= CooldownFailureThreshold {
		d.CooldownUntil = time.Now().Add(CooldownDuration)
	}
}

func (o *Orchestrator) recordSuccessLocked(d *DeploymentState, elapsed time.Duration, tokensOut int) {
	d.FailureCount = 0

	if d.AvgDuration == 0 {
		d.AvgDuration = elapsed
	} else {
		d.AvgDuration = time.Duration(float64(d.AvgDuration)*(1-MetricSmoothingAlpha) + float64(elapsed)*MetricSmoothingAlpha)
	}

	if elapsed > 0 && tokensOut > 0 {
		observed := float64(tokensOut) / elapsed.Seconds()
		if d.Config.TokensPerSecond == 0 {
			d.Config.TokensPerSecond = observed
		} else {
			d.Config.TokensPerSecond = d.Config.TokensPerSecond*(1-MetricSmoothingAlpha) + observed*MetricSmoothingAlpha
		}
		o.persistDeploymentMetricLocked(d)
	}
}

func (o *Orchestrator) persistDeploymentMetricLocked(d *DeploymentState) {
	for pi := range o.config.Pools {
		for di := range o.config.Pools[pi].Deployments {
			if o.config.Pools[pi].Deployments[di].ID == d.Config.ID {
				o.config.Pools[pi].Deployments[di].TokensPerSecond = d.Config.TokensPerSecond
			}
		}
	}
	_ = o.config.Save()
}

func (o *Orchestrator) infer(ctx context.Context, client DeploymentClient, job *Job, d *DeploymentState) (InferOutcome, error) {
	events := client.Infer(ctx, InferRequest{
		Model:        d.Config.Model,
		History:      job.History,
		Tools:        job.Tools,
		ToolChoice:   job.ToolChoice,
		Instructions: job.SkillInstructions,
	})

	gotChunk := false
	for ev := range events {
		switch ev.Kind {
		case InferEventChunk:
			if !gotChunk {
				gotChunk = true
				o.mu.Lock()
				if job.Status != StatusCancelled {
					job.Status = StatusStreaming
				}
				o.mu.Unlock()
			}
		case InferEventDone:
			return ev.Outcome, nil
		case InferEventError:
			return InferOutcome{}, ev.Err
		}
	}
	return InferOutcome{}, fmt.Errorf("deployment closed connection without a result")
}

func (o *Orchestrator) runJob(pool *PoolState, job *Job) {
	for {
		o.mu.Lock()
		switch job.Status {
		case StatusCancelled, StatusDone, StatusFailed:
			o.mu.Unlock()
			return
		}
		o.mu.Unlock()

		estimated := o.estimateJobTokens(job)

		o.mu.Lock()
		if !pool.FitsAny(estimated) {
			job.Status = StatusFailed
			job.FailureReason = errPromptTooLarge.Error()
			o.mu.Unlock()
			return
		}
		o.mu.Unlock()

		d, err := o.acquireSlot(pool, job, estimated)
		if err != nil {
			o.mu.Lock()
			if job.Status != StatusCancelled {
				job.Status = StatusFailed
				job.FailureReason = err.Error()
			}
			o.mu.Unlock()
			return
		}

		client, clientErr := o.ClientFactory(d.Config)

		o.mu.Lock()
		job.Status = StatusInferring
		job.DeploymentID = d.Config.ID
		job.StartedAt = time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		job.cancel = cancel
		o.mu.Unlock()

		o.totalRequests.Add(1)

		var outcome InferOutcome
		var inferErr error
		if clientErr != nil {
			inferErr = clientErr
		} else {
			outcome, inferErr = o.infer(ctx, client, job, d)
		}
		cancel()

		now := time.Now()
		elapsed := now.Sub(job.StartedAt)

		o.mu.Lock()
		cancelled := job.Status == StatusCancelled
		o.releaseSlotLocked(d, job.ID)
		o.cond.Broadcast()

		if cancelled {
			o.mu.Unlock()
			return
		}

		if inferErr != nil {
			o.recordFailureLocked(d)
			job.Attempts++
			job.FailedDeploymentIDs = append(job.FailedDeploymentIDs, d.Config.ID)
			if job.Attempts > job.MaxRetries {
				job.Status = StatusFailed
				job.FailureReason = fmt.Sprintf("exhausted retries: %v", inferErr)
				o.mu.Unlock()
				return
			}
			job.Status = StatusRetrying
			o.mu.Unlock()
			continue
		}

		o.recordSuccessLocked(d, elapsed, outcome.TokensOut)
		o.mu.Unlock()

		if len(outcome.ToolCalls) > 0 {
			o.mu.Lock()
			job.History = append(job.History, Message{Role: "assistant", Content: outcome.Text, ToolCalls: outcome.ToolCalls})
			job.PendingToolCalls = outcome.ToolCalls
			job.ToolRounds++
			if job.ToolRounds > job.MaxToolRounds && job.FinalResponseFormat == nil {
				job.Status = StatusFailed
				job.FailureReason = "tool round limit exceeded"
				o.mu.Unlock()
				return
			}
			job.Status = StatusRequestedTool
			o.mu.Unlock()
			return
		}

		if job.FinalResponseFormat != nil {
			if validateErr := job.FinalResponseFormat.ValidateText(outcome.Text); validateErr != nil {
				o.mu.Lock()
				job.Attempts++
				if job.Attempts > job.MaxRetries {
					job.Status = StatusFailed
					job.FailureReason = fmt.Sprintf("schema validation failed after max retries: %v", validateErr)
					o.mu.Unlock()
					return
				}
				job.History = append(job.History,
					Message{Role: "assistant", Content: outcome.Text},
					Message{Role: "user", Content: fmt.Sprintf("Your response did not match the required schema: %v. Please correct it.", validateErr)},
				)
				job.PinnedDeploymentID = d.Config.ID
				job.Status = StatusRetrying
				o.mu.Unlock()
				continue
			}
		}

		o.mu.Lock()
		job.History = append(job.History, Message{Role: "assistant", Content: outcome.Text})
		job.Result = outcome.Text
		job.Status = StatusDone
		o.mu.Unlock()
		return
	}
}
