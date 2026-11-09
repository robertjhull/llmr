package llmr

import (
	"errors"
	"slices"
	"time"
)

var (
	errNoDeploymentAvailable   = errors.New("no deployment currently available")
	errPromptTooLarge          = errors.New("prompt exceeds context window of every deployment in the pool")
	errUnknownDeployment       = errors.New("unknown pinned deployment")
	errAllDeploymentsExhausted = errors.New("every deployment in the pool has already failed for this request")
)

func EstimateTokens(text string) int {
	if len(text) == 0 {
		return 0
	}
	if tokens := len(text) / 4; tokens > 1 {
		return tokens
	}
	return 1
}

func (d *DeploymentState) IsIdle() bool {
	capacity := d.Config.Capacity
	if capacity < 1 {
		capacity = 1
	}
	return len(d.ActiveSlots) < capacity
}

func (d *DeploymentState) InCooldown(now time.Time) bool {
	return now.Before(d.CooldownUntil)
}

func (d *DeploymentState) FitsContext(estimatedTokens int) bool {
	return d.Config.MaxContextTokens == 0 || estimatedTokens <= d.Config.MaxContextTokens
}

func (d *DeploymentState) estimatedAvgDuration(estimatedTokens int) time.Duration {
	if d.AvgDuration > 0 {
		return d.AvgDuration
	}
	if d.Config.TokensPerSecond <= 0 {
		return 0
	}
	seconds := float64(estimatedTokens) / d.Config.TokensPerSecond
	return time.Duration(seconds * float64(time.Second))
}

func (d *DeploymentState) ProjectedFinish(now time.Time, newJobTokens int) time.Duration {
	newEstimate := d.estimatedAvgDuration(newJobTokens)
	if d.IsIdle() {
		return newEstimate
	}
	var soonestRemaining time.Duration
	for i, slot := range d.ActiveSlots {
		elapsed := now.Sub(slot.StartedAt)
		remaining := slot.ExpectedDuration - elapsed
		if remaining < 0 {
			remaining = 0
		}
		if i == 0 || remaining < soonestRemaining {
			soonestRemaining = remaining
		}
	}
	return soonestRemaining + newEstimate
}

func (p *PoolState) FitsAny(estimatedTokens int) bool {
	for _, d := range p.Deployments {
		if d.FitsContext(estimatedTokens) {
			return true
		}
	}
	return false
}

func SelectDeployment(pool *PoolState, estimatedTokens int, pinnedDeploymentID string, preferredDeploymentID string, excluded []string, backlog map[string]time.Duration, now time.Time) (chosen *DeploymentState, deferredTo *DeploymentState, err error) {
	if pinnedDeploymentID != "" {
		for _, d := range pool.Deployments {
			if d.Config.ID != pinnedDeploymentID {
				continue
			}
			if d.InCooldown(now) || !d.IsIdle() || !d.FitsContext(estimatedTokens) {
				return nil, d, errNoDeploymentAvailable
			}
			return d, nil, nil
		}
		return nil, nil, errUnknownDeployment
	}

	if preferredDeploymentID != "" && !slices.Contains(excluded, preferredDeploymentID) {
		for _, d := range pool.Deployments {
			if d.Config.ID != preferredDeploymentID {
				continue
			}
			if !d.InCooldown(now) && d.IsIdle() && d.FitsContext(estimatedTokens) {
				return d, nil, nil
			}
			break
		}
	}

	var idleBest *DeploymentState
	var idleBestEstimate time.Duration
	var busyBest *DeploymentState
	var busyBestFinish time.Duration
	anyNonExcluded := false

	for _, d := range pool.Deployments {
		if slices.Contains(excluded, d.Config.ID) {
			continue
		}
		anyNonExcluded = true
		if d.InCooldown(now) || !d.FitsContext(estimatedTokens) {
			continue
		}
		if d.IsIdle() {
			estimate := d.estimatedAvgDuration(estimatedTokens)
			if idleBest == nil || estimate < idleBestEstimate {
				idleBest = d
				idleBestEstimate = estimate
			}
		} else {
			finish := d.ProjectedFinish(now, estimatedTokens) + backlog[d.Config.ID]
			if busyBest == nil || finish < busyBestFinish {
				busyBest = d
				busyBestFinish = finish
			}
		}
	}

	if idleBest == nil && busyBest == nil {
		if !anyNonExcluded {
			return nil, nil, errAllDeploymentsExhausted
		}
		return nil, nil, errNoDeploymentAvailable
	}
	if idleBest == nil {
		return nil, busyBest, errNoDeploymentAvailable
	}
	if busyBest != nil && idleBestEstimate > 0 {
		threshold := time.Duration(float64(idleBestEstimate) * (1 - BusyIdleMargin))
		if busyBestFinish <= threshold {
			return nil, busyBest, errNoDeploymentAvailable
		}
	}
	return idleBest, nil, nil
}
