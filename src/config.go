package llmr

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

type Skill struct {
	Tools        []Tool `json:"tools"`
	Instructions string `json:"instructions,omitempty"`
}

type Deployment struct {
	ID               string  `json:"id"`
	Model            string  `json:"model"`
	APIBase          string  `json:"api_base"`
	APIKey           string  `json:"api_key,omitempty"`
	APISpec          string  `json:"api_spec,omitempty"`
	MaxContextTokens int     `json:"max_context_tokens,omitempty"`
	Capacity         int     `json:"capacity,omitempty"`
	TokensPerSecond  float64 `json:"tokens_per_second,omitempty"`
}

type Pool struct {
	Name        string       `json:"name"`
	Deployments []Deployment `json:"deployments"`
}

type Config struct {
	Pools  []Pool           `json:"pools"`
	Skills map[string]Skill `json:"skills,omitempty"`

	path string
}

var ErrNotImplemented = errors.New("not implemented")

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		cfg := &Config{path: path}
		if err := cfg.Save(); err != nil {
			return nil, err
		}
		return cfg, nil
	} else if err != nil {
		return nil, err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	cfg.path = path

	assigned := false
	for pi := range cfg.Pools {
		for di := range cfg.Pools[pi].Deployments {
			if cfg.Pools[pi].Deployments[di].ID == "" {
				cfg.Pools[pi].Deployments[di].ID = newDeploymentID()
				assigned = true
			}
		}
	}
	if assigned {
		if err := cfg.Save(); err != nil {
			return nil, err
		}
	}

	return &cfg, nil
}

func (c *Config) Save() error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(c.path, data, 0o644)
}

func (c *Config) AddDeployment(poolName string, d Deployment) (Deployment, error) {
	if poolName == "" {
		return Deployment{}, fmt.Errorf("pool is required")
	}
	if d.Model == "" {
		return Deployment{}, fmt.Errorf("model is required")
	}
	if d.APIBase == "" {
		return Deployment{}, fmt.Errorf("api-base is required")
	}
	if d.APISpec == "" {
		d.APISpec = "openai"
	}
	if d.Capacity == 0 {
		d.Capacity = 1
	}
	d.ID = newDeploymentID()
	d.TokensPerSecond = 0

	pool, ok := c.Pool(poolName)
	if !ok {
		c.Pools = append(c.Pools, Pool{Name: poolName})
		pool = &c.Pools[len(c.Pools)-1]
	}
	pool.Deployments = append(pool.Deployments, d)

	if err := c.Save(); err != nil {
		return Deployment{}, err
	}
	return d, nil
}

func (c *Config) Pool(name string) (*Pool, bool) {
	for i := range c.Pools {
		if c.Pools[i].Name == name {
			return &c.Pools[i], true
		}
	}
	return nil, false
}

func newDeploymentID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
