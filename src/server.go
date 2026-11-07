package llmr

import (
	"fmt"
	"net/http"
)

type ServerConfig struct {
	Host       string
	Port       int
	ConfigPath string
}

type Server struct {
	config ServerConfig
	api    *API
	mux    *http.ServeMux
}

func NewServer(cfg ServerConfig) (*Server, error) {
	config, err := LoadConfig(cfg.ConfigPath)
	if err != nil {
		return nil, err
	}
	return NewServerFromOrchestrator(cfg, NewOrchestrator(config)), nil
}

func NewServerFromOrchestrator(cfg ServerConfig, orchestrator *Orchestrator) *Server {
	status := NewStatusService(orchestrator)
	api := NewAPI(orchestrator, status)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", api.HandleHealth)
	mux.HandleFunc("GET /status", api.HandleStatus)
	mux.HandleFunc("POST /v1/responses", api.HandleSubmitResponse)
	mux.HandleFunc("GET /v1/responses/{id}", api.HandleGetResponse)
	mux.HandleFunc("POST /v1/batches", api.HandleSubmitBatch)
	mux.HandleFunc("GET /jobs/{id}", api.HandleGetJob)
	mux.HandleFunc("DELETE /jobs/{id}", api.HandleCancelJob)
	mux.HandleFunc("POST /pools/{pool}/models", api.HandleAddDeployment)
	mux.Handle("/", DashboardHandler())

	return &Server{config: cfg, api: api, mux: mux}
}

func (s *Server) Addr() string {
	return fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) Start() error {
	return http.ListenAndServe(s.Addr(), s.mux)
}
