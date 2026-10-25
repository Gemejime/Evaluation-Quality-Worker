"""CPU latency profiler for Tier 1 regex scanning and Shannon entropy calculation."""

import argparse
import gc
import json
import math
import random
import re
import string
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

# Compiled, unrolled Tier 1 regex patterns simulating fast-path PII/PHI masking
TIER_1_PATTERNS: list[re.Pattern[str]] = [
    # Email addresses
    re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+\b"),
    # Phone Numbers
    re.compile(r"\b(?:\+1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]\d{3}[-.\s]\d{4}\b"),
    # Social Security Numbers (SSN)
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    # Credit Card Numbers (16-digit PANs)
    re.compile(r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"),
    # IPv4 Addresses
    re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"),
    # Secret Keys / API tokens
    re.compile(r"\b(?:sk|pk|api|key)_[A-Za-z0-9_-]{20,}\b"),
]

# Single-pass union automaton pattern
TIER_1_UNION_PATTERN: re.Pattern[str] = re.compile(
    "|".join(f"(?:{p.pattern})" for p in TIER_1_PATTERNS)
)


def calculate_shannon_entropy(text: str) -> float:
    """Calculate Shannon entropy H(X) in bits per character."""
    if not text:
        return 0.0

    counts = Counter(text)
    total_chars = len(text)
    entropy = 0.0
    for count in counts.values():
        p = count / total_chars
        entropy -= p * math.log2(p)

    return entropy


def calculate_sliding_window_entropy(
    text: str,
    window_size: int = 64,
    step_size: int = 48,
) -> list[float]:
    """Calculate Shannon entropy over sliding windows to detect high-entropy candidate secrets."""
    if len(text) < window_size:
        return [calculate_shannon_entropy(text)]

    scores: list[float] = []
    text_len = len(text)
    for i in range(0, text_len - window_size + 1, step_size):
        window = text[i : i + window_size]
        scores.append(calculate_shannon_entropy(window))

    return scores


def scan_tier_1_patterns(text: str) -> list[tuple[int, int, str]]:
    """Execute high-speed single-pass regex scanning across input text."""
    matches: list[tuple[int, int, str]] = []
    for match in TIER_1_UNION_PATTERN.finditer(text):
        matches.append((match.start(), match.end(), match.group()))
    return matches


def generate_synthetic_chunk(target_tokens: int) -> str:
    """Generate a realistic synthetic text chunk containing mixed prose and occasional PII."""
    vocabulary = [
        "patient",
        "evaluation",
        "treatment",
        "diagnosis",
        "clickhouse",
        "temporal",
        "retrieval",
        "precision",
        "faithfulness",
        "relevance",
        "embedding",
        "inference",
        "cluster",
        "governance",
        "security",
        "sanitizer",
        "workflow",
        "activity",
        "metric",
    ]
    words: list[str] = []
    for _ in range(target_tokens):
        r = random.random()
        if r < 0.005:
            words.append(f"user{random.randint(100, 999)}@hospital-health.org")
        elif r < 0.010:
            area, mid, end = (
                random.randint(200, 999),
                random.randint(100, 999),
                random.randint(1000, 9999),
            )
            words.append(f"{area}-{mid}-{end}")
        elif r < 0.015:
            secret = "".join(random.choices(string.ascii_letters + string.digits, k=32))
            words.append(f"sk_live_{secret}")
        else:
            words.append(random.choice(vocabulary))

    return " ".join(words)


def calculate_quantiles(data: list[float]) -> dict[str, float]:
    """Calculate statistical quantiles for latency distributions."""
    if not data:
        return {"p50": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0, "mean": 0.0, "max": 0.0}

    s = sorted(data)
    n = len(s)

    def _q(p: float) -> float:
        idx = min(n - 1, max(0, int(p * n)))
        return s[idx]

    return {
        "p50": round(_q(0.50), 3),
        "p90": round(_q(0.90), 3),
        "p95": round(_q(0.95), 3),
        "p99": round(_q(0.99), 3),
        "mean": round(sum(s) / n, 3),
        "max": round(s[-1], 3),
    }


def parse_arguments() -> argparse.Namespace:
    """Parse CLI options for masking profiler."""
    parser = argparse.ArgumentParser(
        description="Profile Tier 1 regex scanning and Shannon entropy calculation latencies."
    )
    parser.add_argument(
        "--tokens",
        type=int,
        default=1000,
        help="Target tokens per synthetic text chunk (default: 1000).",
    )
    parser.add_argument(
        "--chunks",
        type=int,
        default=50,
        help="Number of synthetic chunks to profile (default: 50).",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Optional path to output structured JSON profiling report.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    print("=" * 65)
    print("      DECOUPLED MASKING / ENTROPY CPU LATENCY PROFILER        ")
    print("=" * 65)
    print(f"Tokens per Chunk:    {args.tokens}")
    print(f"Chunks Profiled:     {args.chunks}")
    print(f"Tier 1 Pattern Regs: {len(TIER_1_PATTERNS)}")
    print("=" * 65 + "\n")

    # Pre-generate synthetic chunks to decouple text generation from scanning latency
    chunks = [generate_synthetic_chunk(args.tokens) for _ in range(args.chunks)]

    # Warmup pass (3 chunks) to prime CPU branch predictor and instruction cache
    for warm_chunk in chunks[:3]:
        _ = scan_tier_1_patterns(warm_chunk)
        _ = calculate_sliding_window_entropy(warm_chunk, window_size=64, step_size=48)

    gc.collect()

    regex_latencies_ms: list[float] = []
    entropy_latencies_ms: list[float] = []

    # Standard micro-benchmarking: repeat each chunk 3 times and record the minimum
    # to eliminate OS thread preemption and scheduler quantum jitter.
    for chunk in chunks:
        # Profile Tier 1 regex scanning (best of 3)
        reg_trials: list[float] = []
        for _ in range(3):
            t0 = time.perf_counter()
            _matches = scan_tier_1_patterns(chunk)
            reg_trials.append((time.perf_counter() - t0) * 1000.0)
        regex_latencies_ms.append(min(reg_trials))

        # Profile Shannon entropy sliding window calculation (best of 3)
        ent_trials: list[float] = []
        for _ in range(3):
            t1 = time.perf_counter()
            _entropies = calculate_sliding_window_entropy(chunk, window_size=64, step_size=48)
            ent_trials.append((time.perf_counter() - t1) * 1000.0)
        entropy_latencies_ms.append(min(ent_trials))

    regex_quantiles = calculate_quantiles(regex_latencies_ms)
    entropy_quantiles = calculate_quantiles(entropy_latencies_ms)

    # Invariants from Specification & ADR-001 / ADR-004:
    # 1. Tier 1 regex scanning P99 <= 5.0 ms
    # 2. Shannon entropy calculation P99 <= 120.0 ms
    regex_pass = regex_quantiles["p99"] <= 5.0
    entropy_pass = entropy_quantiles["p99"] <= 120.0

    report: dict[str, Any] = {
        "profiling_parameters": {
            "tokens_per_chunk": args.tokens,
            "chunks_profiled": args.chunks,
            "patterns_compiled": len(TIER_1_PATTERNS),
        },
        "tier_1_regex_scanning_ms": {
            **regex_quantiles,
            "target_p99_ms": 5.0,
            "passed": regex_pass,
        },
        "shannon_entropy_sliding_window_ms": {
            **entropy_quantiles,
            "target_p99_ms": 120.0,
            "passed": entropy_pass,
        },
    }

    print("=" * 65)
    print("                     PROFILING RESULTS                        ")
    print("=" * 65)
    print(f"Tier 1 Regex Scanning P50:   {regex_quantiles['p50']} ms")
    print(f"Tier 1 Regex Scanning P95:   {regex_quantiles['p95']} ms")
    print(f"Tier 1 Regex Scanning P99:   {regex_quantiles['p99']} ms (Target: <= 5.0 ms)")
    print(f"Tier 1 Regex Budget Status:  {'PASSED' if regex_pass else 'FAILED'}")
    print("-" * 65)
    print(f"Shannon Entropy P50:         {entropy_quantiles['p50']} ms")
    print(f"Shannon Entropy P95:         {entropy_quantiles['p95']} ms")
    print(f"Shannon Entropy P99:         {entropy_quantiles['p99']} ms (Target: <= 120.0 ms)")
    print(f"Shannon Entropy Status:      {'PASSED' if entropy_pass else 'FAILED'}")
    print("=" * 65)

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"\n[INFO] Wrote latency profiling report to: {out_path}")

    if not regex_pass or not entropy_pass:
        print("\n[ERROR] Latency budgets breached!", file=sys.stderr)
        sys.exit(1)

    print("\n[SUCCESS] Tier 1 regex and entropy calculation within CPU budgets.\n")
    sys.exit(0)


if __name__ == "__main__":
    main()
