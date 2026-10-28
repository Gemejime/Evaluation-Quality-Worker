# Secret Management Guide & Operational Architecture
## `rag-eval-governance-worker` Deployment Infrastructure

[![Security Status](https://img.shields.io/badge/Security-Zero--Trust%20GitOps-blue.svg)](Architecture.md)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Secrets%20v1-green.svg)](https://kubernetes.io/docs/concepts/configuration/secret/)
[![Helm](https://img.shields.io/badge/Helm-values.yaml%20Excluded-orange.svg)](deployments/helm/values.yaml)

---

## 1. Executive Summary & Security Principles

The **RAG Evaluation & Governance Worker (`rag-eval-governance-worker`)** executes batch evaluation workloads, communicates with downstream LLM judge services, and ingests metrics directly into ClickHouse.

To uphold **Zero-Trust GitOps compliance**, the service enforces strict decoupling between non-sensitive configuration parameters and cryptographic secrets:

1. **Config vs. Secret Decoupling**:
   - **Configuration (`ConfigMap`)**: Non-sensitive settings (network hostnames, ports, timeouts, batch sizes, retry limits) are stored in `rag-eval-governance-worker-config` and tracked in Git via `.env.prod` and `deployments/helm/values.yaml`.
   - **Secrets (`Kubernetes Secret`)**: Sensitive credentials (database passwords, tokens, private keys) are stored in dedicated Kubernetes `Secret` objects (`rag-eval-worker-secret` and `clickhouse-eval-worker-secret`).
2. **GitOps Invariant (Zero Plaintext Secrets in SCM)**:
   - Plaintext secrets **must never** be committed to Git repositories or Helm `values.yaml` files.
   - The synchronization script [`scripts/sync_env_to_deployments.py`](../scripts/sync_env_to_deployments.py) automatically strips variables matching `("PASSWORD", "SECRET", "TOKEN", "KEY")` before propagating `.env.prod` values to `values.yaml`.
3. **Defense in Depth**:
   - Pods execute as unprivileged, non-root users (`UID 1000`, `runAsNonRoot: true`).
   - All runtime logs are structured JSON with automated credential redaction and payload hashing (Secured Logging Invariant).

---

## 2. Secret Inventory Matrix

| Secret Variable | Target K8s Secret | Target Namespace | Key in Secret | Consuming Component | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `CLICKHOUSE_PASSWORD` | `rag-eval-worker-secret` | `rag-evaluation` | `CLICKHOUSE_PASSWORD` | `rag-eval-governance-worker` | Authentication password for user `eval_worker` inserting metrics into ClickHouse database `rag_governance`. |
| `password` | `clickhouse-eval-worker-secret` | `storage` | `password` | Altinity ClickHouse Operator (`rag-clickhouse`) | Provisioned password credential for user `eval_worker` defined in `deployments/clickhouse/chi-production.yaml`. |
| `TEMPORAL_AUTH_TOKEN` *(Optional / Secured)* | `rag-eval-worker-secret` | `rag-evaluation` | `TEMPORAL_AUTH_TOKEN` | Temporal Activity Worker | Bearer / API token when connecting to managed Temporal Cloud or secured clusters. |
| `TEMPORAL_TLS_KEY` / `CERT` *(Optional)* | `rag-eval-worker-tls` | `rag-evaluation` | `tls.key`, `tls.crt`, `ca.crt` | Temporal gRPC Client | mTLS private key and client certificate for mutual TLS authentication. |
| `JUDGE_API_KEY` *(Optional / Secured)* | `rag-eval-worker-secret` | `rag-evaluation` | `JUDGE_API_KEY` | `TritonJudgeClient` | Egress authorization header if upstream `rag-inference-serving` gateway requires authentication. |

---

## 3. Secret Flow Architecture

```
                  ┌────────────────────────────────────────────────────────┐
                  │                 External Secret Store                  │
                  │   (GitHub Actions Secrets / AWS Secrets Manager /      │
                  │                 HashiCorp Vault)                       │
                  └───────────────────────────┬────────────────────────────┘
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      │ CI/CD Injection or External Secrets Operator  │
                      └───────┬───────────────────────────────┬───────┘
                              │                               │
        Namespace: `storage`  ▼         Namespace: `rag-evaluation`  ▼
       ┌───────────────────────────────┐     ┌─────────────────────────────────┐
       │ Kubernetes Secret:            │     │ Kubernetes Secret:              │
       │ `clickhouse-eval-worker-secret│     │ `rag-eval-worker-secret`        │
       │   - password: <CH_PASSWORD>   │     │   - CLICKHOUSE_PASSWORD: <PASS> │
       └──────────────┬────────────────┘     └────────────────┬────────────────┘
                      │                                       │
                      ▼                                       ▼
       ┌───────────────────────────────┐     ┌─────────────────────────────────┐
       │ Altinity ClickHouse Operator  │     │ `rag-eval-governance-worker`    │
       │ (chi-production.yaml)         │     │ (deployment.yaml / rollout.yaml)│
       │   users.eval_worker.password  │     │   envFrom:                      │
       │     valueFrom.secretKeyRef    │     │     - secretRef:                │
       └───────────────────────────────┘     │         name: secretName        │
                                             └─────────────────────────────────┘
```

---

## 4. Provisioning Methods

### Method 1: CI/CD Pipeline Injection (GitHub Actions - Active Default)

As implemented in [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml), GitHub repository secrets are injected into target Kubernetes namespaces immediately prior to Helm release deployment:

```yaml
- name: "Inject Worker Secret"
  run: |
    kubectl create secret generic rag-eval-worker-secret \
      --from-literal=CLICKHOUSE_PASSWORD="${{ secrets.PROD_CLICKHOUSE_PASSWORD }}" \
      --namespace rag-evaluation \
      --dry-run=client -o yaml | kubectl apply -f -

- name: "Inject ClickHouse Operator Secret"
  run: |
    kubectl create secret generic clickhouse-eval-worker-secret \
      --from-literal=password="${{ secrets.PROD_CLICKHOUSE_PASSWORD }}" \
      --namespace storage \
      --dry-run=client -o yaml | kubectl apply -f -
```

**Required GitHub Repository Secrets**:
- `KUBECONFIG`: Base64 or plaintext kubeconfig with deployment privileges.
- `PROD_CLICKHOUSE_PASSWORD`: Production database password for the `eval_worker` account.

---

### Method 2: External Secrets Operator (ESO) + Cloud Secret Manager (Enterprise Recommended)

For pure GitOps workflows (e.g., ArgoCD pulling exclusively from Git), utilize the **External Secrets Operator (ESO)** to synchronize secrets directly from AWS Secrets Manager, HashiCorp Vault, or Google Cloud Secret Manager.

#### 1. ClusterSecretStore Definition (AWS Secrets Manager Example)
```yaml
apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  name: aws-secrets-manager
spec:
  provider:
    aws:
      service: SecretsManager
      region: eu-west-1
      auth:
        jwt:
          serviceAccountRef:
            name: eso-irsa-service-account
            namespace: external-secrets
```

#### 2. ExternalSecret for `rag-eval-governance-worker`
File: `deployments/helm/templates/externalsecret.yaml`
```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: rag-eval-worker-external-secret
  namespace: rag-evaluation
spec:
  refreshInterval: 1h
  secretStoreRef:
    kind: ClusterSecretStore
    name: aws-secrets-manager
  target:
    name: rag-eval-worker-secret
    creationPolicy: Owner
  data:
    - secretKey: CLICKHOUSE_PASSWORD
      remoteRef:
        key: production/rag-platform/clickhouse
        property: eval_worker_password
```

#### 3. ExternalSecret for ClickHouse Storage
```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: clickhouse-eval-worker-external-secret
  namespace: storage
spec:
  refreshInterval: 1h
  secretStoreRef:
    kind: ClusterSecretStore
    name: aws-secrets-manager
  target:
    name: clickhouse-eval-worker-secret
    creationPolicy: Owner
  data:
    - secretKey: password
      remoteRef:
        key: production/rag-platform/clickhouse
        property: eval_worker_password
```

---

### Method 3: Mozilla SOPS / Encrypted Secrets (GitOps In-Repo Alternative)

When secrets must reside in the Git repository alongside Helm charts, encrypt them using **Mozilla SOPS** and **age** or **AWS KMS**:

1. **Encrypt Local Secret**:
   ```bash
   sops --encrypt \
     --kms "arn:aws:kms:eu-west-1:123456789012:key/your-key-id" \
     deployments/secrets/production-secrets.dec.yaml > deployments/secrets/production-secrets.enc.yaml
   ```

2. **ArgoCD Decryption Integration**:
   Configure ArgoCD with the SOPS plugin (`ksops` or `argocd-vault-plugin`) so that `production-secrets.enc.yaml` is decrypted exclusively inside the cluster during deployment reconciliation.

---

## 5. Helm & Pod Injection Architecture

### Helm Configuration (`values.yaml`)
In [`deployments/helm/values.yaml`](helm/values.yaml):
```yaml
env:
  ENVIRONMENT: production
  CLICKHOUSE_USER: eval_worker
  CLICKHOUSE_DATABASE: rag_governance
  # Reference to the secret object containing CLICKHOUSE_PASSWORD
  secretName: 'rag-eval-worker-secret'
```

### Workload Ingestion (`deployment.yaml` and `rollout.yaml`)
Both [`deployments/helm/templates/deployment.yaml`](helm/templates/deployment.yaml) and [`deployments/helm/templates/rollout.yaml`](helm/templates/rollout.yaml) bind both ConfigMaps and Secrets into container environment variables:

```yaml
envFrom:
  - configMapRef:
      name: {{ include "rag-eval-governance-worker.fullname" . }}-config
  {{- if .Values.env.secretName }}
  - secretRef:
      name: {{ .Values.env.secretName }}
  {{- end }}
```

### Runtime Settings Ingestion
Pydantic's `BaseSettings` in [`internal/config/settings.py`](../internal/config/settings.py) dynamically reads `CLICKHOUSE_PASSWORD` from the injected container environment:
```python
CLICKHOUSE_HOST: str = "clickhouse.storage.svc.cluster.local"
CLICKHOUSE_PORT: int = 8123
CLICKHOUSE_USER: str = "eval_worker"
CLICKHOUSE_PASSWORD: str = ""  # Populated via secretRef
CLICKHOUSE_DATABASE: str = "rag_governance"
```

---

## 6. Local Development vs. Production Workflows

| Lifecycle Phase | Configuration File | Secrets Location | Git Policy |
| :--- | :--- | :--- | :--- |
| **Local Development** | `.env` | `.env` | Ignored by `.gitignore` |
| **Development Reference** | `.env.example` | Blank (`CLICKHOUSE_PASSWORD=`) | Committed to Git |
| **Production Manifest Sync** | `.env.prod` | Stripped during sync | Synchronized via `sync_env_to_deployments.py` |
| **Kubernetes Runtime** | `rag-eval-governance-worker-config` | `rag-eval-worker-secret` | Provisioned out-of-band |

### Automated Drift & Leak Prevention
Run the drift detection tool to confirm that no credentials have leaked into Helm values:
```bash
# Validate sync state without modifying manifests
python scripts/sync_env_to_deployments.py --check-only

# Synchronize non-secret environment parameters safely
python scripts/sync_env_to_deployments.py --sync
```

---

## 7. Zero-Downtime Secret Rotation Runbook

Follow this runbook to rotate the ClickHouse `eval_worker` credentials without terminating active Temporal evaluation workflows:

### Step 1: Update ClickHouse User with Dual Passwords
ClickHouse allows multiple SHA-256 hashes per user. In the ClickHouse SQL console:
```sql
ALTER USER eval_worker IDENTIFIED WITH sha256_password BY 'NEW_PASSWORD_HERE' ADD IDENTIFIED WITH sha256_password BY 'OLD_PASSWORD_HERE';
```

### Step 2: Update the External Secret Store
Update the secret in your primary secret store (AWS Secrets Manager, Vault, or GitHub Secrets `PROD_CLICKHOUSE_PASSWORD`).

### Step 3: Apply the Secret to Kubernetes
```bash
# Update Worker Secret
kubectl create secret generic rag-eval-worker-secret \
  --from-literal=CLICKHOUSE_PASSWORD="NEW_PASSWORD_HERE" \
  --namespace rag-evaluation \
  --dry-run=client -o yaml | kubectl apply -f -

# Update ClickHouse Operator Secret
kubectl create secret generic clickhouse-eval-worker-secret \
  --from-literal=password="NEW_PASSWORD_HERE" \
  --namespace storage \
  --dry-run=client -o yaml | kubectl apply -f -
```

### Step 4: Perform Progressive Rolling Restart
Because the worker uses **Temporal Activity Heartbeating** (`ACTIVITY_HEARTBEAT_INTERVAL_SECONDS=10.0`), restarted pods resume evaluation tasks from their last checkpointed `sample_idx` without duplicating judge calls:

```bash
# If using standard deployment:
kubectl rollout restart deployment/rag-eval-governance-worker -n rag-evaluation

# If using Argo Rollouts:
kubectl argo rollouts restart rollout/rag-eval-governance-worker -n rag-evaluation

# Monitor rollout progression:
kubectl rollout status deployment/rag-eval-governance-worker -n rag-evaluation --timeout=180s
```

### Step 5: Revoke the Old Password in ClickHouse
Once all pods are running on the new password and metrics flush successfully:
```sql
ALTER USER eval_worker IDENTIFIED WITH sha256_password BY 'NEW_PASSWORD_HERE';
```

---

## 8. Verification & Diagnostic Commands

### Confirm Secret Presence (Without Leaking Values)
```bash
# Verify worker secret exists in rag-evaluation namespace
kubectl get secret rag-eval-worker-secret -n rag-evaluation -o jsonpath='{.data.CLICKHOUSE_PASSWORD}' | [ -n "$(cat)" ] && echo "Secret key CLICKHOUSE_PASSWORD exists"

# Verify ClickHouse storage secret exists in storage namespace
kubectl get secret clickhouse-eval-worker-secret -n storage -o jsonpath='{.data.password}' | [ -n "$(cat)" ] && echo "Secret key password exists"
```

### Verify Container Injection
```bash
# Confirm variable is bound inside running container without revealing value in bash history
kubectl exec -it deploy/rag-eval-governance-worker -n rag-evaluation -- printenv CLICKHOUSE_USER
kubectl exec -it deploy/rag-eval-governance-worker -n rag-evaluation -- python -c "import os; print('CLICKHOUSE_PASSWORD bound:', bool(os.environ.get('CLICKHOUSE_PASSWORD')))"
```

### Inspect Pod Logs for Credential Redaction Compliance
```bash
kubectl logs -l app.kubernetes.io/name=rag-eval-governance-worker -n rag-evaluation --tail=100 | grep -i "password"
# Expectation: Zero plaintext passwords logged (structured JSON logs with redact_sensitive_data)
```

---

## 9. Troubleshooting & Failure Modes

| Failure Mode | Symptoms | Root Cause | Remediation |
| :--- | :--- | :--- | :--- |
| `CreateContainerConfigError` | Pod status shows `CreateContainerConfigError` | `secretName: 'rag-eval-worker-secret'` is missing in the target namespace. | Create the secret using Method 1 or trigger ESO synchronization. |
| `CrashLoopBackOff` on Startup | Health check fails or logs show `ClickHouse connection refused / authentication failed` | `CLICKHOUSE_PASSWORD` in `rag-eval-worker-secret` does not match ClickHouse database credentials. | Execute the Rotation Runbook (Section 7) or inspect `clickhouse-eval-worker-secret` in `storage`. |
| Git Commit Rejected by Pre-Commit Hook | Git commit fails on `.env.prod` or `values.yaml` diff | Secret values were inadvertently committed to `values.yaml`. | Run `python scripts/sync_env_to_deployments.py --sync` to scrub secret keywords from `values.yaml`. |
| ArgoCD OutOfSync / Drift | ArgoCD flags deployment as OutOfSync | Manual edits made to in-cluster secret while Git specifies different manifest state. | Ensure secrets are managed via ESO or out-of-band CI/CD pipeline, and exclude secret objects from ArgoCD drift calculation. |
