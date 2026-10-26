"""Unit tests for TritonJudgeClient and response parsing."""

from typing import Any

import httpx
import pytest

from internal.evaluation.judges.triton_client import TritonJudgeClient


class MockTransport(httpx.AsyncBaseTransport):
    """Mock HTTP transport returning configured JSON payloads."""

    def __init__(self, response_payload: dict[str, Any], status_code: int = 200) -> None:
        self.response_payload = response_payload
        self.status_code = status_code

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=self.status_code,
            json=self.response_payload,
            request=request,
        )


@pytest.mark.asyncio
async def test_triton_client_standard_json_response() -> None:
    payload = {
        "choices": [{"message": {"content": '{"score": 0.95, "reasoning": "Accurate response"}'}}],
        "usage": {
            "prompt_tokens": 120,
            "completion_tokens": 35,
        },
    }
    client = httpx.AsyncClient(transport=MockTransport(payload), base_url="http://test")
    judge = TritonJudgeClient(client=client)

    resp = await judge.generate(prompt="Evaluate this answer", system_prompt="You are a judge")
    assert resp.parsed_json == {"score": 0.95, "reasoning": "Accurate response"}
    assert resp.prompt_tokens == 120
    assert resp.completion_tokens == 35
    assert resp.latency_ms >= 0
    await judge.aclose()


@pytest.mark.asyncio
async def test_triton_client_markdown_fenced_json_response() -> None:
    raw_markdown = """```json
    {
      "verdicts": [
        {"chunk_idx": 0, "verdict": 1, "reasoning": "Relevant chunk"}
      ]
    }
    ```"""
    payload = {
        "choices": [{"message": {"content": raw_markdown}}],
        "usage": {"prompt_tokens": 50, "completion_tokens": 20},
    }
    client = httpx.AsyncClient(transport=MockTransport(payload), base_url="http://test")
    judge = TritonJudgeClient(client=client)

    resp = await judge.generate(prompt="Evaluate chunk relevance")
    assert resp.parsed_json is not None
    assert "verdicts" in resp.parsed_json
    assert resp.parsed_json["verdicts"][0]["chunk_idx"] == 0
    assert resp.parsed_json["verdicts"][0]["verdict"] == 1
    await judge.aclose()


@pytest.mark.asyncio
async def test_triton_client_double_encoded_json_response() -> None:
    nested = '{"score": 0.88, "reasoning": "Nested string"}'
    payload = {
        "choices": [{"message": {"content": f'"{nested}"'}}],
    }
    client = httpx.AsyncClient(transport=MockTransport(payload), base_url="http://test")
    judge = TritonJudgeClient(client=client)

    resp = await judge.generate(prompt="Evaluate")
    assert resp.parsed_json == {"score": 0.88, "reasoning": "Nested string"}
    await judge.aclose()


@pytest.mark.asyncio
async def test_triton_client_empty_choices() -> None:
    payload: dict[str, Any] = {"choices": []}
    client = httpx.AsyncClient(transport=MockTransport(payload), base_url="http://test")
    judge = TritonJudgeClient(client=client)

    resp = await judge.generate(prompt="Evaluate")
    assert resp.content == ""
    assert resp.parsed_json is None
    await judge.aclose()


@pytest.mark.asyncio
async def test_triton_client_http_error() -> None:
    client = httpx.AsyncClient(
        transport=MockTransport({"error": "server error"}, status_code=500),
        base_url="http://test",
    )
    judge = TritonJudgeClient(client=client)

    with pytest.raises(httpx.HTTPStatusError):
        await judge.generate(prompt="Evaluate")
    await judge.aclose()
