"""Native Triton / vLLM judge client implementing BaseJudgeClient protocol."""

import json
import re
import time
from typing import Any

import httpx
import structlog
from pydantic import BaseModel

from internal.config.settings import Settings, get_settings
from internal.evaluation.judges.base import BaseJudgeClient, JudgeResponse

logger = structlog.get_logger(__name__)


class TritonJudgeClient(BaseJudgeClient):
    """Judge client interacting with Triton / vLLM OpenAI-compatible inference endpoints."""

    def __init__(
        self,
        endpoint: str | None = None,
        model: str | None = None,
        settings: Settings | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        cfg = settings or get_settings()
        self.endpoint = (endpoint or cfg.JUDGE_ENDPOINT).rstrip("/")
        self.model = model or cfg.JUDGE_MODEL
        self.timeout_seconds = cfg.JUDGE_REQUEST_TIMEOUT_SECONDS
        self.max_connections = cfg.JUDGE_MAX_CONCURRENT_REQUESTS

        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = httpx.AsyncClient(
                base_url=self.endpoint,
                timeout=httpx.Timeout(self.timeout_seconds),
                limits=httpx.Limits(
                    max_connections=self.max_connections,
                    max_keepalive_connections=10,
                ),
            )
            self._owns_client = True

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[BaseModel] | None = None,
    ) -> JudgeResponse:
        """Execute a completion request expecting a structured JSON object response."""
        messages: list[dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
        }

        is_absolute = str(self._client.base_url).startswith("http")
        target_url = (
            "/v1/chat/completions" if is_absolute else f"{self.endpoint}/v1/chat/completions"
        )
        start_time = time.perf_counter()
        response = await self._client.post(
            target_url,
            json=payload,
        )
        latency_ms = int((time.perf_counter() - start_time) * 1000)

        response.raise_for_status()
        resp_json = response.json()

        choices = resp_json.get("choices", [])
        if not choices:
            raw_content = ""
        else:
            message = choices[0].get("message", {})
            raw_content = message.get("content", "")

        usage = resp_json.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)

        parsed_json = self._parse_json_payload(raw_content)

        return JudgeResponse(
            content=raw_content,
            parsed_json=parsed_json,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            latency_ms=latency_ms,
            raw_response=resp_json,
        )

    def _parse_json_payload(self, text: str) -> dict[str, Any] | None:
        """Defensively parse JSON from model output, handling markdown fences and escapes."""
        if not text or not text.strip():
            return None

        cleaned = text.strip()
        # Handle markdown code blocks
        if cleaned.startswith("```"):
            match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", cleaned)
            if match:
                cleaned = match.group(1).strip()

        try:
            val = json.loads(cleaned)
            if isinstance(val, dict):
                return val
            if isinstance(val, str):
                nested = json.loads(val)
                if isinstance(nested, dict):
                    return nested
        except Exception:
            # Fallback search for first outer JSON object
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start != -1 and end > start:
                try:
                    candidate = cleaned[start : end + 1]
                    val = json.loads(candidate)
                    if isinstance(val, dict):
                        return val
                except Exception:
                    pass

        return None

    async def aclose(self) -> None:
        """Close the underlying HTTP client session if owned."""
        if self._owns_client and not self._client.is_closed:
            await self._client.aclose()
