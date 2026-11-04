from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel
from tokenizers import Tokenizer

DEFAULT_DURATION_SECONDS = 5.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_REQUEST_TIMEOUT = 120.0

_TOKENIZER_CACHE: dict[str, Tokenizer | None] = {}


class Deployment(BaseModel):
    id: str | None = None
    model: str | None = None
    api_base: str | None = None
    api_key: str | None = None
    capacity: int = 1
    max_context_tokens: int | None = None
    max_retries: int | None = None
    request_timeout: float | None = None
    tokenizer: str | None = None


@dataclass
class PoolsConfig:
    pools: dict[str, list[Deployment]]


@dataclass
class CallActivity:
    label: dict[str, Any] | None = None
    state: str = "inferring"  # inferring | streaming | complete
    started_at: float = field(default_factory=time.monotonic)
    first_token_at: float | None = None
    tokens_emitted: int = 0
    last_chunk_at: float | None = None


class DeploymentState:
    def __init__(self, deployment: Deployment):
        self.deployment = deployment
        self.healthy = False
        self.prefill_tokens_per_second: float | None = None
        self.decode_tokens_per_second: float | None = None
        self.avg_completion_tokens: float | None = None
        self.active_calls: dict[str, CallActivity] = {}
        self.has_tokenize_endpoint: bool | None = None

    def get_max_retries(self, override: int | None) -> int:
        if override is not None:
            return override
        if self.deployment.max_retries is not None:
            return self.deployment.max_retries
        return DEFAULT_MAX_RETRIES

    def get_timeout(self) -> float:
        return self.deployment.request_timeout or DEFAULT_REQUEST_TIMEOUT

    def estimate_duration(self, prompt_tokens: int) -> float:
        if self.prefill_tokens_per_second and self.decode_tokens_per_second and self.avg_completion_tokens:
            return (
                prompt_tokens / self.prefill_tokens_per_second
                + self.avg_completion_tokens / self.decode_tokens_per_second
            )
        return DEFAULT_DURATION_SECONDS

    def free_slots(self) -> int:
        return max(0, self.deployment.capacity - len(self.active_calls))

    def fits_context(self, prompt_tokens: int) -> bool:
        mct = self.deployment.max_context_tokens
        return not mct or prompt_tokens <= mct

    def soonest_remaining(self) -> float | None:
        knowns = [r for a in self.active_calls.values() if (r := self._remaining(a)) is not None]
        return min(knowns) if knowns else None

    def _remaining(self, activity: CallActivity) -> float | None:
        if activity.first_token_at is None:
            return None
        if not self.decode_tokens_per_second or not self.avg_completion_tokens:
            return None
        remaining_tokens = max(0.0, self.avg_completion_tokens - activity.tokens_emitted)
        return remaining_tokens / self.decode_tokens_per_second

    async def is_healthy(self) -> bool:
        if not self.deployment.api_base:
            return True

        parts = urlsplit(self.deployment.api_base)
        url = f"{parts.scheme}://{parts.netloc}/health"

        def _ping() -> bool:
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    return 200 <= resp.status < 300
            except (urllib.error.URLError, OSError, ValueError):
                return False

        self.healthy = await asyncio.to_thread(_ping)
        return self.healthy

    def count_tokens_via_endpoint(self, text: str) -> int | None:
        if self.has_tokenize_endpoint is False or not self.deployment.api_base:
            return None

        parts = urlsplit(self.deployment.api_base)
        url = f"{parts.scheme}://{parts.netloc}/tokenize"
        body = json.dumps({"content": text}).encode()
        headers = {"Content-Type": "application/json"}
        if self.deployment.api_key:
            headers["Authorization"] = f"Bearer {self.deployment.api_key}"
        req = urllib.request.Request(url, data=body, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                payload = json.loads(resp.read())
            tokens = payload["tokens"]
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                self.has_tokenize_endpoint = False
            return None
        except (urllib.error.URLError, OSError, ValueError, KeyError, TypeError):
            return None

        self.has_tokenize_endpoint = True
        return len(tokens)

    def count_tokens_via_local_tokenizer(self, text: str) -> int | None:
        name = self.deployment.tokenizer
        if not name:
            return None

        if name not in _TOKENIZER_CACHE:
            try:
                _TOKENIZER_CACHE[name] = (
                    Tokenizer.from_file(name) if Path(name).is_file() else Tokenizer.from_pretrained(name)
                )
            except Exception:
                _TOKENIZER_CACHE[name] = None

        tokenizer = _TOKENIZER_CACHE[name]
        return len(tokenizer.encode(text).ids) if tokenizer is not None else None


def load_pools(path: str | Path) -> PoolsConfig:
    raw = json.loads(Path(path).read_text())

    pools: dict[str, list[Deployment]] = {}
    id: int = 0
    for pool_name, deployments in raw.items():
        models = [Deployment(**d) for d in deployments]
        names = [m.api_base + m.model for m in models]
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate model url in pool {pool_name!r}")
        for model in models:
            id += 1
            model.id = f"m{id}"
        pools[pool_name] = models

    return PoolsConfig(pools)
