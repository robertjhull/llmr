# llm-request-router

`llmr` is a local Go server for routing, load-balancing, and batching requests across pools of LLM deployments (local or cloud-hosted). It tracks each deployment's throughput, retries around failures, supports tool-calling and structured output, and ships a small status dashboard.

Single binary, no external services required.

## Build

```sh
go build -o llmr .
```

## Running it

```sh
./llmr start --host 127.0.0.1 --port 8080 --config pools.json
```

- `--config` points at the pools file (default `pools.json`). It's created automatically on first run if it doesn't exist.
- Open `http://127.0.0.1:8080/` in a browser for the live status dashboard.
- `./llmr status --port 8080` pretty-prints the same data from the terminal.
- `curl http://127.0.0.1:8080/health` is a trivial liveness probe, separate from the richer `/status` data.

## Adding models to a pool

```sh
./llmr add --pool local --model llama-3.1-8b --api-base http://localhost:8001/v1 --port 8080
```

`--pool`, `--model`, and `--api-base` are required. Optional flags: `--api-key`, `--api-spec` (default `openai`), `--ctx-size` (max context tokens), `--capacity` (concurrent slots, default `1`).

This creates the pool if it doesn't exist yet and persists the deployment to `pools.json` with a stable, auto-assigned `id`. You can also hand-edit `pools.json` directly to bootstrap an initial set of deployments before the server has ever run — see `pools.example.json`. Once the server has added IDs to those entries and started routing traffic, let the app manage the file from then on; it also tracks each deployment's learned `tokens_per_second`.

## Making requests

### A single prompt

```sh
curl -s http://127.0.0.1:8080/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{
        "pool": "local",
        "input": {"text": "What is the capital of France?"}
      }'
```

This call blocks until the model finishes (or the job pauses for a tool call — see below) and returns:

```json
{"id": "a1b2c3d4", "status": "done", "output": "Paris."}
```

Fetch it again later with `GET /v1/responses/{id}`, or get llmr's own richer status/progress view of the same job with `GET /jobs/{id}` (different shape, same underlying job).

### Continuing a conversation

Pass `previous_response_id` with a new `input` to continue where a prior response left off — it's appended to that job's stored history and stays pinned to whichever deployment handled it originally:

```sh
curl -s http://127.0.0.1:8080/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{
        "pool": "local",
        "previous_response_id": "a1b2c3d4",
        "input": {"text": "And its population?"}
      }'
```

### Tool calling

Pass `tools` with your request. If the model asks to call one, the job pauses with `status: "requested tool"` and a list of pending tool calls instead of finishing:

```sh
curl -s http://127.0.0.1:8080/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{
        "pool": "local",
        "input": {"text": "What is the weather in NYC?"},
        "tools": [{"name": "get_weather", "description": "Look up current weather", "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}]
      }'
```

Run the tool yourself, then resubmit the result via `previous_response_id`, giving the tool call's id and your tool's output as the `input`:

```sh
curl -s http://127.0.0.1:8080/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{
        "pool": "local",
        "previous_response_id": "a1b2c3d4",
        "input": {"tool_call_id": "call_xyz", "tool_output": "sunny, 72F"}
      }'
```

The job resumes on the same deployment and either finishes or asks for another tool call, up to `max_tool_rounds` (default `5`).

### Batches

Submit many prompts at once under one shared configuration (`pool`, `tools`, `skills`, `final_response_format`, retry/round limits). Unlike a single request, this returns immediately with a job id — llmr distributes the items across the pool's deployments in the background, proportional to each one's measured throughput:

```sh
curl -s http://127.0.0.1:8080/v1/batches \
  -H 'Content-Type: application/json' \
  -d '{
        "pool": "local",
        "items": [
          {"input": {"text": "Summarize: ..."}},
          {"input": {"text": "Translate to French: ..."}},
          {"input": {"text": "Summarize: ..."}}
        ]
      }'
```

```json
{"id": "f00dcafe", "status": "queued"}
```

Poll progress with `GET /jobs/{id}` — it returns the batch's overall status plus each item's own status and result as it becomes available:

```json
{
  "id": "f00dcafe",
  "pool": "local",
  "status": "inferring",
  "items": [
    {"id": "item-1", "status": "done", "result": "..."},
    {"id": "item-2", "status": "inferring"},
    {"id": "item-3", "status": "requested tool"}
  ]
}
```

If a batch item's model requests a tool call, that item pauses the same way a single request does — resume it by posting to `/v1/responses` with `previous_response_id` set to **that item's own id** (not the batch's id). Unlike a single request's continuation, a resumed batch item isn't pinned back to its original deployment — it's placed fresh, wherever is fastest right now.

Cancel a batch (or a single job) with `DELETE /jobs/{id}`: every queued item is dropped and every in-flight item's deployment slot is released immediately.

### Structured output

Pass `final_response_format` as a JSON Schema. If the model's response doesn't validate, llmr automatically appends a corrective message and retries on the same deployment (counted against `max_retries`, default `2`) instead of failing immediately or rerouting elsewhere.

### Skills

Reusable bundles of tools + instructions can be defined once under a top-level `skills` map in `pools.json` and referenced by name:

```json
{"pool": "local", "input": {"text": "..."}, "skills": ["web-search"]}
```

See `pools.example.json` for the shape.

## Endpoint reference

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/responses` | Submit (or continue) a single prompt. Blocks until done or paused on a tool call. |
| `GET` | `/v1/responses/{id}` | OpenAI-Responses-shaped view of a job. |
| `POST` | `/v1/batches` | Submit a batch of prompts. Returns immediately. |
| `GET` | `/jobs/{id}` | llmr's status/progress view of any job or batch. |
| `DELETE` | `/jobs/{id}` | Cancel a job or batch. |
| `POST` | `/pools/{pool}/models` | Add a deployment to a pool (backs `llmr add`). |
| `GET` | `/status` | Full snapshot of pools/deployments/queues — backs the dashboard and `llmr status`. |
| `GET` | `/health` | Liveness probe. |

All endpoints are unauthenticated — `llmr` is meant to run locally for a single operator.
