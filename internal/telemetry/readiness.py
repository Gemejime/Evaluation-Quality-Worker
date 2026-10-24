"""Readiness health state manager for rag-eval-governance-worker."""

from typing import Literal

ComponentStatus = Literal[
    "connected", "polling", "ready", "degraded", "unreachable", "initializing"
]


class ReadinessManager:
    """Manages readiness state for downstream dependencies to support health probes."""

    def __init__(self) -> None:
        self._states: dict[str, str] = {
            "clickhouse": "connected",
            "temporal": "polling",
            "judge_endpoint": "ready",
        }

    def set_status(self, component: str, status: str) -> None:
        """Set health status for a specific dependency component."""
        self._states[component] = status

    def get_status(self, component: str) -> str:
        """Get current health status for a specific dependency component."""
        return self._states.get(component, "unknown")

    def is_ready(self) -> bool:
        """Return True if all components are in operational ready states."""
        operational_targets = {
            "clickhouse": "connected",
            "temporal": "polling",
            "judge_endpoint": "ready",
        }
        return all(self._states.get(k) == v for k, v in operational_targets.items())

    def get_readiness_payload(self) -> dict[str, str]:
        """Return full probe payload conforming to OpenAPI readiness contract."""
        ready = self.is_ready()
        return {
            "status": "ready" if ready else "degraded",
            "clickhouse": self._states.get("clickhouse", "unreachable"),
            "temporal": self._states.get("temporal", "unreachable"),
            "judge_endpoint": self._states.get("judge_endpoint", "unreachable"),
        }

    def reset(self) -> None:
        """Reset all components to default ready states."""
        self._states = {
            "clickhouse": "connected",
            "temporal": "polling",
            "judge_endpoint": "ready",
        }


# Global singleton readiness manager
readiness_manager = ReadinessManager()


def get_readiness_manager() -> ReadinessManager:
    """Return the global ReadinessManager instance."""
    return readiness_manager
