"""Unit tests for native RAG triad metric evaluators and hash determinism."""

from typing import Any

import pytest

from internal.evaluation.judges.base import BaseJudgeClient, JudgeResponse
from internal.evaluation.metrics.answer_relevance import AnswerRelevanceEvaluator
from internal.evaluation.metrics.base import compute_reasoning_hash
from internal.evaluation.metrics.context_precision import ContextPrecisionEvaluator
from internal.evaluation.metrics.faithfulness import FaithfulnessEvaluator
from internal.evaluation.models import GoldenSampleItem


class DummyJudge(BaseJudgeClient):
    """Deterministic mock judge returning preconfigured JudgeResponse."""

    def __init__(self, parsed_json: dict[str, Any] | None = None, content: str = "") -> None:
        self.parsed_json = parsed_json
        self.content = content

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[Any] | None = None,
    ) -> JudgeResponse:
        return JudgeResponse(
            content=self.content,
            parsed_json=self.parsed_json,
            prompt_tokens=50,
            completion_tokens=25,
            latency_ms=10,
        )

    async def aclose(self) -> None:
        pass


def test_compute_reasoning_hash_length_and_determinism() -> None:
    text = "Detailed forensic reasoning explanation for faithfulness evaluation"
    h1 = compute_reasoning_hash(text)
    h2 = compute_reasoning_hash(text)

    assert len(h1) == 64
    assert h1 == h2
    assert all(c in "0123456789abcdef" for c in h1)

    # Empty reasoning returns valid 64-char hash
    empty_hash = compute_reasoning_hash(None)
    assert len(empty_hash) == 64


@pytest.mark.asyncio
async def test_context_precision_standard_calculation() -> None:
    # 3 chunks: chunk 0 relevant (1), chunk 1 irrelevant (0), chunk 2 relevant (1)
    # Ranks:
    # Rank 1: rel=1 -> prec@1 = 1/1 = 1.0
    # Rank 2: rel=0 -> prec@2 = 1/2 = 0.5 (not included in sum since v_2 = 0)
    # Rank 3: rel=1 -> prec@3 = 2/3 = 0.6667
    # Sum: 1.0 + 0.6667 = 1.6667
    # Precision@K = 1.6667 / 2 = 0.8333
    parsed = {
        "verdicts": [
            {"chunk_idx": 0, "verdict": 1, "reasoning": "c0"},
            {"chunk_idx": 1, "verdict": 0, "reasoning": "c1"},
            {"chunk_idx": 2, "verdict": 1, "reasoning": "c2"},
        ]
    }
    evaluator = ContextPrecisionEvaluator(DummyJudge(parsed_json=parsed))
    sample = GoldenSampleItem(
        sample_id="s1",
        sample_idx=0,
        query="What is jemalloc?",
        retrieved_contexts=["ctx0", "ctx1", "ctx2"],
        generated_answer="jemalloc is an allocator",
    )

    result = await evaluator.evaluate(sample)
    assert result.metric_name == "context_precision"
    assert pytest.approx(result.score, 0.001) == 0.8333
    assert len(result.reasoning_hash) == 64


@pytest.mark.asyncio
async def test_context_precision_zero_relevant_contract() -> None:
    parsed = {
        "verdicts": [
            {"chunk_idx": 0, "verdict": 0, "reasoning": "irrelevant"},
            {"chunk_idx": 1, "verdict": 0, "reasoning": "irrelevant"},
        ]
    }
    evaluator = ContextPrecisionEvaluator(DummyJudge(parsed_json=parsed))
    sample = GoldenSampleItem(
        sample_id="s2",
        sample_idx=1,
        query="Query",
        retrieved_contexts=["ctx0", "ctx1"],
        generated_answer="Answer",
    )

    result = await evaluator.evaluate(sample)
    assert result.score == 0.0


@pytest.mark.asyncio
async def test_context_precision_empty_contexts() -> None:
    evaluator = ContextPrecisionEvaluator(DummyJudge(parsed_json={}))
    sample = GoldenSampleItem(
        sample_id="s3",
        sample_idx=2,
        query="Query",
        retrieved_contexts=[],
        generated_answer="Answer",
    )

    result = await evaluator.evaluate(sample)
    assert result.score == 0.0


@pytest.mark.asyncio
async def test_faithfulness_standard_ratio() -> None:
    # 4 claims, 3 supported -> 3/4 = 0.75
    parsed = {
        "extracted_claims": [
            {"claim": "Claim 1", "supported": True, "citation_index": 0},
            {"claim": "Claim 2", "supported": True, "citation_index": 1},
            {"claim": "Claim 3", "supported": False, "citation_index": -1},
            {"claim": "Claim 4", "supported": True, "citation_index": 2},
        ],
        "reasoning": "3 of 4 claims verified",
    }
    evaluator = FaithfulnessEvaluator(DummyJudge(parsed_json=parsed))
    sample = GoldenSampleItem(
        sample_id="f1",
        sample_idx=0,
        query="Query",
        retrieved_contexts=["c0", "c1", "c2"],
        generated_answer="Answer with 4 claims",
    )

    result = await evaluator.evaluate(sample)
    assert result.metric_name == "faithfulness"
    assert result.score == 0.75
    assert len(result.reasoning_hash) == 64


@pytest.mark.asyncio
async def test_faithfulness_zero_extracted_claims_contract() -> None:
    # Zero Extracted Claims Contract: 1.0
    parsed: dict[str, Any] = {
        "extracted_claims": [],
        "reasoning": "No claims asserted",
    }
    evaluator = FaithfulnessEvaluator(DummyJudge(parsed_json=parsed))
    sample = GoldenSampleItem(
        sample_id="f2",
        sample_idx=1,
        query="Query",
        retrieved_contexts=["c0"],
        generated_answer="I don't know.",
    )

    result = await evaluator.evaluate(sample)
    assert result.score == 1.0


@pytest.mark.asyncio
async def test_answer_relevance_parsing_and_clamping() -> None:
    parsed = {"score": 0.92, "reasoning": "Highly relevant answer"}
    evaluator = AnswerRelevanceEvaluator(DummyJudge(parsed_json=parsed))
    sample = GoldenSampleItem(
        sample_id="a1",
        sample_idx=0,
        query="How to configure ClickHouse?",
        retrieved_contexts=["ClickHouse settings"],
        generated_answer="Set host and port in settings.",
    )

    result = await evaluator.evaluate(sample)
    assert result.metric_name == "answer_relevance"
    assert result.score == 0.92
    assert len(result.reasoning_hash) == 64

    # Test clamping above 1.0
    evaluator_over = AnswerRelevanceEvaluator(DummyJudge(parsed_json={"score": 1.5}))
    res_over = await evaluator_over.evaluate(sample)
    assert res_over.score == 1.0


@pytest.mark.asyncio
async def test_answer_relevance_validation_errors() -> None:
    sample = GoldenSampleItem(
        sample_id="a_err",
        sample_idx=0,
        query="Query",
        retrieved_contexts=["ctx"],
        generated_answer="Answer",
    )

    # Missing parsed_json
    evaluator_none = AnswerRelevanceEvaluator(DummyJudge(parsed_json=None))
    with pytest.raises(ValueError, match="Missing parsed JSON"):
        await evaluator_none.evaluate(sample)

    # Absent score / rating / verdict
    evaluator_empty = AnswerRelevanceEvaluator(DummyJudge(parsed_json={}))
    with pytest.raises(ValueError, match="No numeric score, verdict, or rating present"):
        await evaluator_empty.evaluate(sample)

    # Non-numeric score
    evaluator_str = AnswerRelevanceEvaluator(DummyJudge(parsed_json={"score": "not-a-number"}))
    with pytest.raises(ValueError, match="Invalid non-numeric relevance score"):
        await evaluator_str.evaluate(sample)

    # Non-finite score (NaN / Inf)
    evaluator_nan = AnswerRelevanceEvaluator(DummyJudge(parsed_json={"score": float("nan")}))
    with pytest.raises(ValueError, match="Non-finite relevance score"):
        await evaluator_nan.evaluate(sample)


@pytest.mark.asyncio
async def test_faithfulness_validation_errors() -> None:
    sample = GoldenSampleItem(
        sample_id="f_err",
        sample_idx=0,
        query="Query",
        retrieved_contexts=["ctx"],
        generated_answer="Answer",
    )

    # Missing parsed_json
    evaluator_none = FaithfulnessEvaluator(DummyJudge(parsed_json=None))
    with pytest.raises(ValueError, match="Missing parsed JSON"):
        await evaluator_none.evaluate(sample)

    # Missing extracted_claims key
    evaluator_no_key = FaithfulnessEvaluator(DummyJudge(parsed_json={"reasoning": "bad"}))
    with pytest.raises(ValueError, match="missing valid 'extracted_claims' list"):
        await evaluator_no_key.evaluate(sample)

    # extracted_claims not a list
    evaluator_bad_type = FaithfulnessEvaluator(
        DummyJudge(parsed_json={"extracted_claims": "invalid"})
    )
    with pytest.raises(ValueError, match="missing valid 'extracted_claims' list"):
        await evaluator_bad_type.evaluate(sample)
