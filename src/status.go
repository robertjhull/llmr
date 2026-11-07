package llmr

import (
	"fmt"
	"time"
)

type DeploymentStatus struct {
	ID              string   `json:"id"`
	Model           string   `json:"model"`
	APIBase         string   `json:"apiBase"`
	TokensPerSecond *float64 `json:"tokensPerSecond"`
	Status          string   `json:"status"`
	ElapsedMinutes  *int     `json:"elapsedMinutes"`
}

type PoolStatus struct {
	Name                string             `json:"name"`
	Queued              int                `json:"queued"`
	BatchElapsedMinutes *int               `json:"batchElapsedMinutes,omitempty"`
	Models              []DeploymentStatus `json:"models"`
}

type ServerStatus struct {
	Host          string `json:"host"`
	Uptime        string `json:"uptime"`
	TotalRequests int64  `json:"totalRequests"`
}

type StatusSnapshot struct {
	Server ServerStatus `json:"server"`
	Pools  []PoolStatus `json:"pools"`
}

type StatusService struct {
	orchestrator *Orchestrator
	startedAt    time.Time
}

func NewStatusService(o *Orchestrator) *StatusService {
	return &StatusService{orchestrator: o, startedAt: time.Now()}
}

func (s *StatusService) Snapshot(host string) StatusSnapshot {
	return StatusSnapshot{
		Server: ServerStatus{
			Host:          host,
			Uptime:        formatUptime(time.Since(s.startedAt)),
			TotalRequests: s.orchestrator.TotalRequests(),
		},
		Pools: s.orchestrator.PoolsSnapshot(),
	}
}

func formatUptime(d time.Duration) string {
	total := int(d.Seconds())
	days := total / 86400
	hours := (total % 86400) / 3600
	minutes := (total % 3600) / 60
	return fmt.Sprintf("%dd %02dh %02dm", days, hours, minutes)
}
