"""Unit tests for trigger_regression_gate script."""

import argparse

import pytest

from scripts.trigger_regression_gate import run_regression_gate


@pytest.mark.asyncio
async def test_regression_gate_mock_judge_success() -> None:
    """Verify regression gate succeeds with mock judge against test/fixtures/golden_set.json."""
    args = argparse.Namespace(
        golden_set="test/fixtures/golden_set.json",
        endpoint="http://localhost:8001",
        mock_judge=True,
        concurrency=10,
        min_score=0.82,
        tenant_id="test-ci",
        run_id="test-run-gate",
    )
    exit_code = await run_regression_gate(args)
    assert exit_code == 0


@pytest.mark.asyncio
async def test_regression_gate_threshold_breach_fails() -> None:
    """Verify regression gate returns exit code 1 when required min_score is unreachable."""
    args = argparse.Namespace(
        golden_set="test/fixtures/golden_set.json",
        endpoint="http://localhost:8001",
        mock_judge=True,
        concurrency=5,
        min_score=0.99,  # Unreachable threshold since answer_relevance mock is 0.95
        tenant_id="test-ci-fail",
        run_id="test-run-fail",
    )
    exit_code = await run_regression_gate(args)
    assert exit_code == 1


@pytest.mark.asyncio
async def test_regression_gate_missing_file_fails() -> None:
    """Verify regression gate returns exit code 1 when golden-set file does not exist."""
    args = argparse.Namespace(
        golden_set="test/fixtures/non_existent.json",
        endpoint="http://localhost:8001",
        mock_judge=True,
        concurrency=5,
        min_score=0.82,
        tenant_id="test-ci",
        run_id="test-run",
    )
    exit_code = await run_regression_gate(args)
    assert exit_code == 1
