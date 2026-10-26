"""Abstract Base Class and data models for LLM-as-a-judge clients."""

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field


class JudgeResponse(BaseModel):
    """Response payload returned from judge client invocations."""

    content: str
    parsed_json: dict[str, Any] | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    latency_ms: int = 0
    raw_response: dict[str, Any] = Field(default_factory=dict)


class BaseJudgeClient(ABC):
    """Abstract interface for judge execution targeting inference engines."""

    @abstractmethod
    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[BaseModel] | None = None,
    ) -> JudgeResponse:
        """Execute a prompt turn against the judge engine with structured JSON format."""
        raise NotImplementedError

    @abstractmethod
    async def aclose(self) -> None:
        """Release underlying HTTP client connections or resources."""
        raise NotImplementedError
