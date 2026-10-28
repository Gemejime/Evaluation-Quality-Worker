"""scripts/sync_env_to_deployments.py

Synchronizes and validates environment configuration between .env.prod and
Kubernetes/Helm/ArgoCD deployment manifests.

Usage:
    python scripts/sync_env_to_deployments.py --sync
    python scripts/sync_env_to_deployments.py --check-only
    python scripts/sync_env_to_deployments.py --env-file .env.prod --check-only
"""

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml

# Ensure UTF-8 output on Windows consoles
reconfigure_stdout = getattr(sys.stdout, "reconfigure", None)
if callable(reconfigure_stdout):
    try:
        reconfigure_stdout(encoding="utf-8")
    except Exception:
        pass

reconfigure_stderr = getattr(sys.stderr, "reconfigure", None)
if callable(reconfigure_stderr):
    try:
        reconfigure_stderr(encoding="utf-8")
    except Exception:
        pass

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_FILE = REPO_ROOT / ".env.prod"
HELM_VALUES_FILE = REPO_ROOT / "deployments" / "helm" / "values.yaml"
ARGOCD_ANALYSIS_FILE = REPO_ROOT / "deployments" / "argocd" / "analysis-template.yaml"

# Keys containing secrets that must never be committed into values.yaml
EXCLUDED_SECRET_KEYWORDS = ("PASSWORD", "SECRET", "TOKEN", "KEY")


def parse_env_file(file_path: Path) -> dict[str, str]:
    """Parse .env formatted file into key-value pairs."""
    env_vars: dict[str, str] = {}
    if not file_path.exists():
        return env_vars

    for line in file_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip().strip("'\"")

        # Exclude secrets from being committed into Helm values
        if any(keyword in key.upper() for keyword in EXCLUDED_SECRET_KEYWORDS):
            continue

        env_vars[key] = val
    return env_vars


def check_drift(env_vars: dict[str, str], helm_env: dict[str, Any]) -> list[str]:
    """Identify discrepancies between environment variables and Helm values."""
    drift: list[str] = []
    for key, expected_val in env_vars.items():
        if key not in helm_env:
            drift.append(f"Missing key in values.yaml: '{key}' (expected '{expected_val}')")
        elif str(helm_env[key]) != expected_val:
            drift.append(
                f"Mismatch for '{key}': values.yaml='{helm_env[key]}' vs env_file='{expected_val}'"
            )
    return drift


def update_argocd_analysis_template(threshold: str) -> bool:
    """Update min-quality-threshold in argocd/analysis-template.yaml if present."""
    if not ARGOCD_ANALYSIS_FILE.exists():
        return False

    with open(ARGOCD_ANALYSIS_FILE, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    updated = False
    args = data.get("spec", {}).get("args", [])
    for arg in args:
        if arg.get("name") == "min-quality-threshold":
            if str(arg.get("value")) != threshold:
                arg["value"] = threshold
                updated = True

    if updated:
        with open(ARGOCD_ANALYSIS_FILE, "w", encoding="utf-8") as f:
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)
    return updated


def sync_to_deployments(env_vars: dict[str, str]) -> None:
    """Propagate env_vars into deployments/helm/values.yaml and related manifests."""
    with open(HELM_VALUES_FILE, encoding="utf-8") as f:
        values_data = yaml.safe_load(f) or {}

    current_env = values_data.get("env", {})
    secret_name = current_env.get("secretName", "")

    # Replace env dictionary while preserving secretName
    new_env = dict(env_vars)
    if secret_name:
        new_env["secretName"] = secret_name
    elif "secretName" in current_env:
        new_env["secretName"] = ""

    values_data["env"] = new_env

    with open(HELM_VALUES_FILE, "w", encoding="utf-8") as f:
        yaml.dump(values_data, f, sort_keys=False, default_flow_style=False)

    print(f"[OK] Successfully synced {len(env_vars)} variables to {HELM_VALUES_FILE}")

    # Check and update ArgoCD analysis template if threshold exists
    if "QUALITY_GATE_MIN_FAITHFULNESS" in env_vars:
        if update_argocd_analysis_template(env_vars["QUALITY_GATE_MIN_FAITHFULNESS"]):
            print(f"[OK] Updated quality threshold in {ARGOCD_ANALYSIS_FILE}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync or check .env.prod against Helm/ArgoCD deployment manifests"
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="Path to environment file (default: .env.prod)",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only check for drift; exit code 1 if out of sync, 0 if clean",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Sync environment variables directly into deployments/helm/values.yaml",
    )
    args = parser.parse_args()

    env_file = args.env_file
    if not env_file.exists():
        print(f"[WARN] Environment file '{env_file}' does not exist. Skipping.")
        sys.exit(0)

    env_vars = parse_env_file(env_file)
    if not env_vars:
        print(f"[WARN] No valid environment variables found in '{env_file}'.")
        sys.exit(0)

    with open(HELM_VALUES_FILE, encoding="utf-8") as f:
        values_data = yaml.safe_load(f) or {}
    helm_env = values_data.get("env", {})

    drift = check_drift(env_vars, helm_env)

    if args.check_only:
        if drift:
            print(
                f"[DRIFT] Drift detected between '{env_file.name}' "
                "and deployments/helm/values.yaml:"
            )
            for issue in drift:
                print(f"   * {issue}")
            print(
                f"\nRun 'python scripts/sync_env_to_deployments.py "
                f"--env-file {env_file.name} --sync' to resolve."
            )
            sys.exit(1)
        else:
            print(f"[OK] deployments/helm/values.yaml is in sync with '{env_file.name}'.")
            sys.exit(0)

    # Perform synchronization
    sync_to_deployments(env_vars)


if __name__ == "__main__":
    main()
