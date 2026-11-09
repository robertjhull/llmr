package llmr

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

func Execute(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: llmr <start|status|add> [flags]")
	}
	switch args[0] {
	case "start":
		return runStart(args[1:])
	case "status":
		return runStatus(args[1:])
	case "add":
		return runAdd(args[1:])
	default:
		return fmt.Errorf("unknown command: %s", args[0])
	}
}

func runStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	host := fs.String("host", "127.0.0.1", "host to bind")
	port := fs.Int("port", 8080, "port to bind")
	configPath := fs.String("config", "pools.json", "path to pools config file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	srv, err := NewServer(ServerConfig{Host: *host, Port: *port, ConfigPath: *configPath})
	if err != nil {
		return err
	}

	fmt.Printf("llmr listening on %s\n", srv.Addr())
	return srv.Start()
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	host := fs.String("host", "127.0.0.1", "host of the running server")
	port := fs.Int("port", 8080, "port of the running server")
	if err := fs.Parse(args); err != nil {
		return err
	}

	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s:%d/status", *host, *port))
	if err != nil {
		return fmt.Errorf("llmr is not reachable at %s:%d: %w", *host, *port, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("llmr returned an error: %s: %s", resp.Status, body)
	}

	var snapshot StatusSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return fmt.Errorf("decoding status response: %w", err)
	}

	printStatus(snapshot)
	return nil
}

func printStatus(s StatusSnapshot) {
	fmt.Printf("%s  uptime %s  requests %d\n", s.Server.Host, s.Server.Uptime, s.Server.TotalRequests)
	for _, pool := range s.Pools {
		fmt.Printf("\n%s  (%d queued)\n", pool.Name, pool.Queued)
		for _, m := range pool.Models {
			tps := "--"
			if m.TokensPerSecond != nil {
				tps = fmt.Sprintf("%.1f", *m.TokensPerSecond)
			}
			fmt.Printf("  %-4s %-20s %-20s %6s tok/s  %s\n", m.ID, m.Model, m.APIBase, tps, m.Status)
		}
	}
}

func runAdd(args []string) error {
	fs := flag.NewFlagSet("add", flag.ContinueOnError)
	host := fs.String("host", "127.0.0.1", "host of the running server")
	port := fs.Int("port", 8080, "port of the running server")
	pool := fs.String("pool", "", "pool name (required)")
	model := fs.String("model", "", "model name (required)")
	apiBase := fs.String("api-base", "", "model api base (required)")
	apiKey := fs.String("api-key", "", "api key")
	apiSpec := fs.String("api-spec", "openai", "api spec")
	ctxSize := fs.Int("ctx-size", 0, "model context size")
	capacity := fs.Int("capacity", 1, "deployment capacity")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *pool == "" || *model == "" || *apiBase == "" {
		return fmt.Errorf("--pool, --model, and --api-base are required")
	}

	body, err := json.Marshal(map[string]any{
		"model":              *model,
		"api_base":           *apiBase,
		"api_key":            *apiKey,
		"api_spec":           *apiSpec,
		"max_context_tokens": *ctxSize,
		"capacity":           *capacity,
	})
	if err != nil {
		return err
	}

	reqURL := fmt.Sprintf("http://%s:%d/pools/%s/models", *host, *port, url.PathEscape(*pool))
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(reqURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("llmr is not reachable at %s:%d: %w", *host, *port, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("llmr returned an error: %s: %s", resp.Status, respBody)
	}

	fmt.Printf("added deployment: %s\n", respBody)
	return nil
}
