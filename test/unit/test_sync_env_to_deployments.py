"""test/unit/test_sync_env_to_deployments.py

Unit test suite for scripts/sync_env_to_deployments.py.
"""

from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import yaml

from scripts.sync_env_to_deployments import (
    check_drift,
    main,
    parse_env_file,
    sync_to_deployments,
    update_argocd_analysis_template,
)


@pytest.fixture
def temp_repo(tmp_path: Path) -> dict[str, Any]:
    """Fixture providing a mock repo directory structure."""
    env_file = tmp_path / ".env.prod"
    env_file.write_text(
        "SERVICE_NAME=rag-eval-worker\n"
        "# Comment line\n"
        "\n"
        "PORT=8080\n"
        'JUDGE_ENDPOINT="http://rag-serving:8001"\n'
        "QUALITY_GATE_MIN_FAITHFULNESS='0.85'\n"
        "CLICKHOUSE_PASSWORD=SuperSecretPass\n"
        "AUTH_TOKEN=jwt_secret_token\n",
        encoding="utf-8",
    )

    helm_dir = tmp_path / "deployments" / "helm"
    helm_dir.mkdir(parents=True)
    helm_values = helm_dir / "values.yaml"
    initial_values = {
        "replicaCount": 2,
        "image": {"repository": "rag-worker", "tag": "0.1.0"},
        "env": {
            "SERVICE_NAME": "rag-eval-worker",
            "PORT": "8080",
            "JUDGE_ENDPOINT": "http://rag-serving:8001",
            "QUALITY_GATE_MIN_FAITHFULNESS": "0.85",
            "secretName": "custom-secret",
        },
    }
    helm_values.write_text(yaml.dump(initial_values), encoding="utf-8")

    argocd_dir = tmp_path / "deployments" / "argocd"
    argocd_dir.mkdir(parents=True)
    analysis_file = argocd_dir / "analysis-template.yaml"
    initial_analysis = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "AnalysisTemplate",
        "spec": {
            "args": [
                {"name": "service-name", "value": "rag-eval-worker"},
                {"name": "min-quality-threshold", "value": "0.82"},
            ]
        },
    }
    analysis_file.write_text(yaml.dump(initial_analysis), encoding="utf-8")

    return {
        "root": tmp_path,
        "env_file": env_file,
        "helm_values": helm_values,
        "analysis_file": analysis_file,
    }


def test_parse_env_file_basic(temp_repo: dict[str, Any]) -> None:
    """Verify parse_env_file parses key-value pairs and strips quotes."""
    parsed = parse_env_file(temp_repo["env_file"])

    assert parsed["SERVICE_NAME"] == "rag-eval-worker"
    assert parsed["PORT"] == "8080"
    assert parsed["JUDGE_ENDPOINT"] == "http://rag-serving:8001"
    assert parsed["QUALITY_GATE_MIN_FAITHFULNESS"] == "0.85"


def test_parse_env_file_filters_secrets(temp_repo: dict[str, Any]) -> None:
    """Verify secrets containing PASSWORD, TOKEN, KEY, SECRET are excluded."""
    parsed = parse_env_file(temp_repo["env_file"])

    assert "CLICKHOUSE_PASSWORD" not in parsed
    assert "AUTH_TOKEN" not in parsed


def test_parse_env_file_nonexistent(tmp_path: Path) -> None:
    """Verify empty dict is returned for non-existent file."""
    assert parse_env_file(tmp_path / "does_not_exist.env") == {}


def test_check_drift_no_drift() -> None:
    """Verify check_drift returns empty list when values are in sync."""
    env_vars = {"PORT": "8080", "QUALITY": "0.85"}
    helm_env = {"PORT": "8080", "QUALITY": "0.85", "secretName": "sec"}
    assert check_drift(env_vars, helm_env) == []


def test_check_drift_missing_keys() -> None:
    """Verify check_drift detects missing keys in Helm environment."""
    env_vars = {"PORT": "8080", "NEW_VAR": "val"}
    helm_env = {"PORT": "8080"}

    drift = check_drift(env_vars, helm_env)
    assert len(drift) == 1
    assert "Missing key in values.yaml: 'NEW_VAR'" in drift[0]


def test_check_drift_mismatched_values() -> None:
    """Verify check_drift detects value differences."""
    env_vars = {"PORT": "8080", "JUDGE_MODEL": "model-v2"}
    helm_env = {"PORT": "8080", "JUDGE_MODEL": "model-v1"}

    drift = check_drift(env_vars, helm_env)
    assert len(drift) == 1
    assert "Mismatch for 'JUDGE_MODEL'" in drift[0]


def test_check_drift_stale_keys() -> None:
    """Verify check_drift detects stale keys in Helm values while excluding secretName."""
    env_vars = {"PORT": "8080"}
    helm_env = {"PORT": "8080", "STALE_KEY": "old_val", "secretName": "app-secret"}

    drift = check_drift(env_vars, helm_env)
    assert len(drift) == 1
    assert "Stale key in values.yaml: 'STALE_KEY'" in drift[0]


def test_update_argocd_analysis_template(
    temp_repo: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify update_argocd_analysis_template updates threshold in YAML."""
    monkeypatch.setattr(
        "scripts.sync_env_to_deployments.ARGOCD_ANALYSIS_FILE",
        temp_repo["analysis_file"],
    )

    # First update should change 0.82 to 0.85 and return True
    changed = update_argocd_analysis_template("0.85")
    assert changed is True

    with open(temp_repo["analysis_file"], encoding="utf-8") as f:
        data = yaml.safe_load(f)
    args = {a["name"]: a["value"] for a in data["spec"]["args"]}
    assert args["min-quality-threshold"] == "0.85"

    # Second call with same threshold should return False
    changed_again = update_argocd_analysis_template("0.85")
    assert changed_again is False


def test_sync_to_deployments(temp_repo: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify sync_to_deployments updates values.yaml and preserves secretName."""
    monkeypatch.setattr(
        "scripts.sync_env_to_deployments.HELM_VALUES_FILE",
        temp_repo["helm_values"],
    )
    monkeypatch.setattr(
        "scripts.sync_env_to_deployments.ARGOCD_ANALYSIS_FILE",
        temp_repo["analysis_file"],
    )

    new_env = {
        "SERVICE_NAME": "new-worker",
        "PORT": "9090",
        "QUALITY_GATE_MIN_FAITHFULNESS": "0.90",
    }
    sync_to_deployments(new_env)

    with open(temp_repo["helm_values"], encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # Check that new env values were written
    assert data["env"]["SERVICE_NAME"] == "new-worker"
    assert data["env"]["PORT"] == "9090"
    assert data["env"]["QUALITY_GATE_MIN_FAITHFULNESS"] == "0.90"
    # Check that secretName was preserved
    assert data["env"]["secretName"] == "custom-secret"
    # Check that non-env blocks remain intact
    assert data["replicaCount"] == 2
    assert data["image"]["repository"] == "rag-worker"


def test_cli_check_only_success(temp_repo: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify CLI --check-only exits 0 when in sync."""
    monkeypatch.setattr(
        "scripts.sync_env_to_deployments.DEFAULT_ENV_FILE",
        temp_repo["env_file"],
    )
    monkeypatch.setattr(
        "scripts.sync_env_to_deployments.HELM_VALUES_FILE",
        temp_repo["helm_values"],
    )

    with mock.patch("sys.argv", ["sync_env", "--check-only"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0


def test_cli_check_only_drift_failure(
    temp_repo: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify CLI --check-only exits 1 when drift is detected."""
    # Append a new variable to .env.prod that values.yaml does not have
    with open(temp_repo["env_file"], "a", encoding="utf-8") as f:
        f.write("UNSYNCED_VAR=true\n")

    monkeypatch.setattr(
        "scripts.sync_env_to_deployments.DEFAULT_ENV_FILE",
        temp_repo["env_file"],
    )
    monkeypatch.setattr(
        "scripts.sync_env_to_deployments.HELM_VALUES_FILE",
        temp_repo["helm_values"],
    )

    with mock.patch("sys.argv", ["sync_env", "--check-only"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
